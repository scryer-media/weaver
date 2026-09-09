use super::*;

use crate::pipeline::completion::finalize::check::{
    CleanPar2VerificationMode, Par2SetSettlementReason, QuickPar2Evidence,
    bounded_repair_evidence_covers_assessment, error_chain_has_file_descriptor_exhaustion,
    run_file_descriptor_bounded_par2_repair,
};

async fn grid_verified_direct_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    payload_filename: &str,
    payload: &[u8],
    declared_size: u32,
) -> NzbFileId {
    let spec = standalone_job_spec(
        "Grid Verified Direct Job",
        &[(payload_filename.to_string(), declared_size)],
    );
    insert_active_job(pipeline, job_id, spec).await;

    let mut par2_set = placement_par2_file_set(&[(payload_filename.to_string(), payload.to_vec())]);
    par2_set.slice_size = 32;
    let par2_file_id = par2_set.recovery_file_ids[0];
    let slice_checksums: Vec<par2_rs::SliceChecksum> = payload
        .chunks(32)
        .map(|slice| {
            let mut state = par2_rs::SliceChecksumState::new();
            state.update(slice);
            let (crc32, md5) = state.finalize(Some(32));
            par2_rs::SliceChecksum { crc32, md5 }
        })
        .collect();
    par2_set
        .slice_checksums
        .insert(par2_file_id, slice_checksums);
    install_test_par2_runtime(pipeline, job_id, par2_set, &[]);

    write_and_complete_file(pipeline, job_id, 0, payload_filename, payload).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    for (index, slice) in payload.chunks(32).enumerate() {
        let offset = (index as u64) * 32;
        let crc = par2_rs::checksum::crc32(slice);
        pipeline.note_block_crc_segments(
            file_id,
            offset,
            slice.len() as u64,
            crc,
            true,
            false,
            &[weaver_yenc::Segment {
                file_offset: offset,
                len: slice.len() as u64,
                crc32: crc,
            }],
        );
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    file_id
}

/// A two-payload job whose first file is damaged and whose second is intact,
/// wired the same way as the single-payload repair test above.
///
/// The damaged payload carries a `Zip` archive topology so the job's clean-PAR2
/// integrity gate reads `StrongDecode`. That is what routes it through the
/// verify-then-repair branch — the one whose post-repair pass this exercises —
/// rather than the repairer-analysis branch a bare payload takes.
///
/// Returns the working directory plus the two payloads' original bytes.
async fn two_payload_repair_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
) -> (PathBuf, Vec<u8>, Vec<u8>) {
    let damaged_filename = "damaged.zip";
    let intact_filename = "intact.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let damaged_original: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let intact_original: Vec<u8> = (0..128u32)
        .map(|value| (value % 241) as u8 ^ 0x5A)
        .collect();
    let mut damaged_on_disk = damaged_original.clone();
    for byte in &mut damaged_on_disk[64..128] {
        *byte = 0;
    }

    let par2_bytes = build_test_par2_index_for_files(
        &[
            (damaged_filename, &damaged_original),
            (intact_filename, &intact_original),
        ],
        64,
    );
    let recovery_bytes = vec![0xAA; 64];
    let payload_segments = |prefix: &str| {
        vec![
            segment_spec! {
                number: 0,
                bytes: 64,
                message_id: format!("{prefix}-0@example.com"),
            },
            segment_spec! {
                number: 1,
                bytes: 64,
                message_id: format!("{prefix}-1@example.com"),
            },
        ]
    };
    let spec = JobSpec {
        name: job_name.to_string(),
        password: None,
        total_bytes: (damaged_original.len()
            + intact_original.len()
            + par2_bytes.len()
            + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: damaged_filename.to_string(),
                role: FileRole::from_filename(damaged_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: payload_segments("selective-damaged"),
            },
            FileSpec {
                filename: intact_filename.to_string(),
                role: FileRole::from_filename(intact_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: payload_segments("selective-intact"),
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "selective-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: "selective-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(damaged_filename), &damaged_on_disk)
        .await
        .unwrap();
    tokio::fs::write(working_dir.join(intact_filename), &intact_original)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        for file_index in 0..2u32 {
            let file_id = NzbFileId { job_id, file_index };
            let file = state.assembly.file_mut(file_id).unwrap();
            file.commit_segment(0, 64).unwrap();
            file.commit_segment(1, 64).unwrap();
        }
        state.assembly.set_archive_topology(
            damaged_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Zip,
                volume_map: HashMap::from([(damaged_filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }
    write_and_complete_file(pipeline, job_id, 2, index_filename, &par2_bytes).await;
    write_and_complete_file(pipeline, job_id, 3, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        pipeline,
        job_id,
        build_repairable_par2_set_for_files(
            &[
                (damaged_filename, &damaged_original),
                (intact_filename, &intact_original),
            ],
            64,
            1,
        ),
        &[
            (2, index_filename, 0, false),
            (3, recovery_filename, 1, true),
        ],
    );

    (working_dir, damaged_original, intact_original)
}

/// A real NZB's `<segment bytes=…>` is the *yEnc-encoded* article size, about
/// 3% larger than the decoded payload PAR2 describes. Every live-PAR2 fixture
/// declares inflated sizes so the declared total never equals the decoded
/// length — the shape production always has.
fn yenc_declared_bytes(decoded_len: u32) -> u32 {
    decoded_len + decoded_len.div_ceil(32) + 2
}

fn split_payload_job_split(payload_len: u32) -> u32 {
    payload_len * 3 / 8
}

fn split_payload_par2_job_spec(
    name: &str,
    payload_filename: &str,
    payload_len: u32,
    index_filename: &str,
    index_len: u32,
) -> JobSpec {
    let first_segment = split_payload_job_split(payload_len);
    let declared_first = yenc_declared_bytes(first_segment);
    let declared_second = yenc_declared_bytes(payload_len - first_segment);
    let declared_index = yenc_declared_bytes(index_len);
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: (declared_first + declared_second + declared_index) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: declared_first,
                        message_id: "live-par2-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: declared_second,
                        message_id: "live-par2-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: declared_index,
                    message_id: "live-par2-index@example.com".to_string(),
                }],
            },
        ],
    }
}

async fn submit_split_payload(
    pipeline: &mut Pipeline,
    job_id: JobId,
    payload_filename: &str,
    written_payload: &[u8],
) {
    let payload_file = NzbFileId {
        job_id,
        file_index: 0,
    };
    // Segment 0 stops mid-block, so block 0 is staged across the boundary and
    // block 1 is claimed whole.
    let split = split_payload_job_split(written_payload.len() as u32) as usize;
    submit_decoded_segment(
        pipeline,
        payload_file,
        0,
        0,
        &written_payload[..split],
        payload_filename,
        None,
    )
    .await;
    submit_decoded_segment(
        pipeline,
        payload_file,
        1,
        split as u64,
        &written_payload[split..],
        payload_filename,
        None,
    )
    .await;
}

async fn submit_par2_index(
    pipeline: &mut Pipeline,
    job_id: JobId,
    index_filename: &str,
    par2_bytes: &[u8],
) {
    submit_decoded_segment(
        pipeline,
        NzbFileId {
            job_id,
            file_index: 1,
        },
        0,
        0,
        par2_bytes,
        index_filename,
        None,
    )
    .await;
}

async fn drain_job_to_completion(pipeline: &mut Pipeline, job_id: JobId) {
    {
        // A damaged job can already have failed and been retired by the time
        // the last segment commits; nothing left to drain.
        let Some(state) = pipeline.jobs.get_mut(&job_id) else {
            return;
        };
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.check_job_completion(job_id).await;
    // The damaged path hands its authoritative read to a blocking worker and
    // returns; the verdict arrives as a message the orchestrator's select loop
    // would service. Draining the detached tickets here is what stands in for
    // that loop, and without it the job parks in `Verifying` forever.
    settle_direct_post_repair_work(pipeline).await;
}

/// Build a single-payload job whose article bitmap is one segment short, with a
/// PAR2 set describing the payload. Returns the working dir and the payload's
/// file id.
async fn incomplete_protected_payload_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    name: &str,
    payload_filename: &str,
    payload: &[u8],
) -> (PathBuf, NzbFileId) {
    let spec = JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: payload.len() as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: payload_filename.to_string(),
            role: FileRole::from_filename(payload_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: format!("{name}-0@example.com"),
                },
                segment_spec! {
                    number: 1,
                    bytes: 64,
                    message_id: format!("{name}-1@example.com"),
                },
            ],
        }],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }
    (working_dir, file_id)
}

fn complete_verification_for(
    par2_set: &Par2FileSet,
    filename: &str,
) -> par2_rs::VerificationResult {
    par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_set.recovery_file_ids[0],
            filename: filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true, true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    }
}

async fn install_par2_rename_candidate(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posted_filename: &str,
    payload: &[u8],
    described: &[(&str, &[u8])],
) -> PathBuf {
    let (working_dir, _) = incomplete_protected_payload_job(
        pipeline,
        job_id,
        "PAR2 Rename Candidate",
        posted_filename,
        payload,
    )
    .await;
    tokio::fs::write(working_dir.join(posted_filename), payload)
        .await
        .unwrap();
    install_test_par2_runtime(
        pipeline,
        job_id,
        build_repairable_par2_set_for_files(described, 1024, 1),
        &[],
    );
    working_dir
}

/// A complete, PAR2-protected payload whose bytes are not where their names say.
///
/// `described` gives the recovery set (and the NZB) its file names and the
/// content each name is supposed to hold; `on_disk[i]` is what is actually
/// written at `described[i]`'s name, so a caller expresses a swap by handing the
/// two entries each other's bytes and damage by handing one entry holed bytes.
/// Every article has arrived either way — this is a posting fault, not a
/// download one.
///
/// No archive topology is installed, so the completion gate's integrity gate
/// reads `None` and the job takes the repairer-analysis arm. That is the arm the
/// field job took, and the one that has to tell "nothing to repair, only to
/// place" from "damaged".
async fn misplaced_payload_par2_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    described: &[(&str, Vec<u8>)],
    on_disk: &[Vec<u8>],
    recovery_blocks: usize,
) -> PathBuf {
    assert_eq!(described.len(), on_disk.len());
    let index_filename = "silver-horizon.par2";
    let recovery_filename = "silver-horizon.vol00+01.par2";
    let described_refs: Vec<(&str, &[u8])> = described
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let par2_bytes = build_test_par2_index_for_files(&described_refs, 64);
    let recovery_bytes = vec![0xAA; 64];

    let mut files: Vec<FileSpec> = described
        .iter()
        .enumerate()
        .map(|(index, (filename, bytes))| FileSpec {
            filename: (*filename).to_string(),
            role: FileRole::from_filename(filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: (0..bytes.len() as u32 / 64)
                .map(|segment| {
                    segment_spec! {
                        number: segment,
                        bytes: 64,
                        message_id: format!("misplaced-{index}-{segment}@example.com"),
                    }
                })
                .collect(),
        })
        .collect();
    let payload_count = files.len() as u32;
    files.push(FileSpec {
        filename: index_filename.to_string(),
        role: FileRole::from_filename(index_filename),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: par2_bytes.len() as u32,
            message_id: "misplaced-index@example.com".to_string(),
        }],
    });
    files.push(FileSpec {
        filename: recovery_filename.to_string(),
        role: FileRole::from_filename(recovery_filename),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: recovery_bytes.len() as u32,
            message_id: "misplaced-recovery@example.com".to_string(),
        }],
    });

    let total_bytes = (described
        .iter()
        .map(|(_, bytes)| bytes.len())
        .sum::<usize>()
        + par2_bytes.len()
        + recovery_bytes.len()) as u64;
    let spec = JobSpec {
        name: job_name.to_string(),
        password: None,
        total_bytes,
        category: None,
        metadata: vec![],
        files,
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    for ((filename, _), bytes) in described.iter().zip(on_disk.iter()) {
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        for file_index in 0..payload_count {
            let file_id = NzbFileId { job_id, file_index };
            let file = state.assembly.file_mut(file_id).unwrap();
            let segment_count = described[file_index as usize].1.len() as u32 / 64;
            for segment in 0..segment_count {
                file.commit_segment(segment, 64).unwrap();
            }
        }
    }
    write_and_complete_file(pipeline, job_id, payload_count, index_filename, &par2_bytes).await;
    write_and_complete_file(
        pipeline,
        job_id,
        payload_count + 1,
        recovery_filename,
        &recovery_bytes,
    )
    .await;
    install_test_par2_runtime(
        pipeline,
        job_id,
        build_repairable_par2_set_for_files(&described_refs, 64, recovery_blocks),
        &[
            (payload_count, index_filename, 0, false),
            (payload_count + 1, recovery_filename, 1, true),
        ],
    );

    working_dir
}

fn misplacement_payload(seed: u32) -> Vec<u8> {
    (0..128u32)
        .map(|value| ((value * 7 + seed * 31) % 251) as u8)
        .collect()
}

/// Stage the identity-rebound misplaced-payload shape as a conventional
/// (non-direct) job and hand back the two pieces a direct quick-verify call
/// needs: the working directory and the served recovery set.
///
/// `described[i]` is the name the recovery set gives file `i` and the content it
/// says that name should hold; `on_disk[i]` is what is actually written at that
/// name — `None` leaves the file absent (never completed). A caller expresses a
/// swap by handing two present entries each other's bytes, damage by handing one
/// holed bytes, and a missing partner by handing `None`. Every present file is
/// completed and its identity rebound to its canonical name with a PAR2 source,
/// which is the post-rebind state the completion gate meets in the field. No
/// article is fed to the dual-CRC grid and no whole-file digest is recorded, so
/// this is the metadata-early shape that streams no MD5 at all — callers that
/// want content evidence add it explicitly.
async fn stage_misplaced_payload_shape(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    described: &[(&str, Vec<u8>)],
    on_disk: &[Option<Vec<u8>>],
) -> (PathBuf, Arc<Par2FileSet>) {
    assert_eq!(described.len(), on_disk.len());
    let files: Vec<(String, u32)> = described
        .iter()
        .zip(on_disk.iter())
        .map(|((name, canonical), disk)| {
            let bytes = disk.as_ref().map_or(canonical.len(), Vec::len);
            ((*name).to_string(), bytes as u32)
        })
        .collect();
    let spec = standalone_job_spec(job_name, &files);
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    for (index, ((name, _), disk)) in described.iter().zip(on_disk.iter()).enumerate() {
        let Some(bytes) = disk else {
            continue;
        };
        write_and_complete_file(pipeline, job_id, index as u32, name, bytes).await;
        pipeline
            .set_file_identity(
                job_id,
                crate::jobs::record::ActiveFileIdentity {
                    file_index: index as u32,
                    source_filename: (*name).to_string(),
                    current_filename: (*name).to_string(),
                    canonical_filename: Some((*name).to_string()),
                    classification: None,
                    classification_source: crate::jobs::record::FileIdentitySource::Par2,
                },
            )
            .unwrap();
    }

    let described_pairs: Vec<(String, Vec<u8>)> = described
        .iter()
        .map(|(name, canonical)| ((*name).to_string(), canonical.clone()))
        .collect();
    install_test_par2_runtime(
        pipeline,
        job_id,
        placement_par2_file_set(&described_pairs),
        &[],
    );
    let par2_set = Arc::clone(pipeline.par2_set(job_id).expect("served recovery set"));
    (working_dir, par2_set)
}

/// Record a trusted whole-file MD5 for a completed file, the current-generation
/// evidence a metadata-early download deliberately never streams.
fn set_measured_md5(pipeline: &mut Pipeline, job_id: JobId, file_index: u32, content: &[u8]) {
    pipeline
        .ensure_par2_runtime(job_id)
        .completed_checksums
        .insert(
            NzbFileId { job_id, file_index },
            crate::pipeline::CompletedFileChecksum {
                md5: Some(par2_rs::checksum::md5(content)),
                crc32: par2_rs::checksum::crc32(content),
                all_parts_crc_verified: false,
            },
        );
}

/// Describe a file the set lists but does not protect.
///
/// A PAR2 set's non-recovery files carry a name and the two digests every
/// description carries, and nothing else: no slice checksums, no recovery data.
/// `verify_all` reads only the protected files, while the deobfuscator reads
/// every description — which is what lets a file arrive under a posted name and
/// be given the one the set says it should have.
fn describe_non_recovery_file(
    par2_set: &mut Par2FileSet,
    filename: &str,
    bytes: &[u8],
) -> par2_rs::FileId {
    let length = bytes.len() as u64;
    let hash_full = par2_rs::checksum::md5(bytes);
    let hash_16k = par2_rs::checksum::md5(&bytes[..bytes.len().min(16 * 1024)]);
    let mut id_input = Vec::new();
    id_input.extend_from_slice(&hash_16k);
    id_input.extend_from_slice(&length.to_le_bytes());
    id_input.extend_from_slice(filename.as_bytes());
    let file_id = par2_rs::FileId::from_bytes(par2_rs::checksum::md5(&id_input));
    par2_set.files.insert(
        file_id,
        par2_rs::FileDescription {
            file_id,
            hash_full,
            hash_16k,
            length,
            par2_name: filename.to_string(),
            filename: filename.to_string(),
        },
    );
    par2_set.non_recovery_file_ids.push(file_id);
    file_id
}

const FURNITURE_SLICE_SIZE: u64 = 64;

/// A recovery set describing a payload plus one piece of metadata "furniture"
/// (an `.nfo`, an `.sfv`), with the PAR2 index — and, when the set carries
/// recovery slices, one recovery volume — already downloaded.
struct FurnitureJob<'a> {
    name: &'a str,
    payload_filename: &'a str,
    /// The bytes the recovery set describes.
    payload: &'a [u8],
    /// What is on disk under that name, if anything.
    payload_on_disk: Option<&'a [u8]>,
    furniture_filename: &'a str,
    furniture: &'a [u8],
    furniture_on_disk: Option<&'a [u8]>,
    /// Whether the furniture's articles all arrived.
    furniture_articles_complete: bool,
    /// Recovery slices the set carries; also the block count the NZB
    /// advertises for the one recovery volume, which is what the fail-fast
    /// arithmetic reads.
    recovery_blocks: u32,
}

async fn install_furniture_par2_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job: FurnitureJob<'_>,
) -> PathBuf {
    let index_filename = "silver.horizon.par2";
    let recovery_filename = format!("silver.horizon.vol00+{:02}.par2", job.recovery_blocks);
    let described: [(&str, &[u8]); 2] = [
        (job.payload_filename, job.payload),
        (job.furniture_filename, job.furniture),
    ];
    let par2_bytes = build_test_par2_index_for_files(&described, FURNITURE_SLICE_SIZE);
    let recovery_bytes = vec![0xAA; 64];
    let payload_segment_bytes = (job.payload.len() / 2) as u32;

    let mut files = vec![
        FileSpec {
            filename: job.payload_filename.to_string(),
            role: FileRole::from_filename(job.payload_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                segment_spec! {
                    number: 0,
                    bytes: payload_segment_bytes,
                    message_id: format!("{}-payload-0@example.com", job.name),
                },
                segment_spec! {
                    number: 1,
                    bytes: payload_segment_bytes,
                    message_id: format!("{}-payload-1@example.com", job.name),
                },
            ],
        },
        FileSpec {
            filename: job.furniture_filename.to_string(),
            role: FileRole::from_filename(job.furniture_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: job.furniture.len() as u32,
                message_id: format!("{}-furniture-0@example.com", job.name),
            }],
        },
        FileSpec {
            filename: index_filename.to_string(),
            role: FileRole::from_filename(index_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: par2_bytes.len() as u32,
                message_id: format!("{}-index@example.com", job.name),
            }],
        },
    ];
    if job.recovery_blocks > 0 {
        files.push(FileSpec {
            filename: recovery_filename.clone(),
            role: FileRole::from_filename(&recovery_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: recovery_bytes.len() as u32,
                message_id: format!("{}-recovery@example.com", job.name),
            }],
        });
    }

    let spec = JobSpec {
        name: job.name.to_string(),
        password: None,
        total_bytes: (job.payload.len() + job.furniture.len() + par2_bytes.len() + 128) as u64,
        category: None,
        metadata: vec![],
        files,
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    if let Some(bytes) = job.payload_on_disk {
        tokio::fs::write(working_dir.join(job.payload_filename), bytes)
            .await
            .unwrap();
    }
    if let Some(bytes) = job.furniture_on_disk {
        tokio::fs::write(working_dir.join(job.furniture_filename), bytes)
            .await
            .unwrap();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        let payload_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        for segment_number in 0..2 {
            state
                .assembly
                .file_mut(payload_id)
                .unwrap()
                .commit_segment(segment_number, payload_segment_bytes)
                .unwrap();
        }
        if job.furniture_articles_complete {
            state
                .assembly
                .file_mut(NzbFileId {
                    job_id,
                    file_index: 1,
                })
                .unwrap()
                .commit_segment(0, job.furniture.len() as u32)
                .unwrap();
        }
    }

    write_and_complete_file(pipeline, job_id, 2, index_filename, &par2_bytes).await;
    let mut runtime_files: Vec<(u32, &str, u32, bool)> = vec![(2, index_filename, 0, false)];
    if job.recovery_blocks > 0 {
        write_and_complete_file(pipeline, job_id, 3, &recovery_filename, &recovery_bytes).await;
        runtime_files.push((3, recovery_filename.as_str(), job.recovery_blocks, true));
    }
    install_test_par2_runtime(
        pipeline,
        job_id,
        build_repairable_par2_set_for_files(
            &described,
            FURNITURE_SLICE_SIZE,
            job.recovery_blocks as usize,
        ),
        &runtime_files,
    );

    working_dir
}

/// Drive the completion gate until the job settles, the way a live pipeline
/// would through its own re-arms.
async fn settle_job_completion(pipeline: &mut Pipeline, job_id: JobId) {
    for _ in 0..12 {
        if matches!(
            job_status_for_assert(pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        pipeline.check_job_completion(job_id).await;
        pump_pipeline_runtime_queues(pipeline).await;
        settle_inflight_moves(pipeline).await;
    }
}

fn intact_furniture_payload() -> Vec<u8> {
    (0..128u32).map(|value| (value % 251) as u8).collect()
}

fn second_half_zeroed(bytes: &[u8]) -> Vec<u8> {
    let mut damaged = bytes.to_vec();
    damaged[64..].fill(0);
    damaged
}

const PARTIAL_VOLUME_SLICE_SIZE: u64 = 64;

/// Payload slices, and therefore the width of the recovery set's solve.
const PARTIAL_VOLUME_PAYLOAD_SLICES: usize = 8;

struct PartialVolumeJob<'a> {
    name: &'a str,
    /// Leading payload slices zeroed on disk — the damage the repair must cover.
    damaged_slices: usize,
    /// Recovery packets of the short volume whose payload bytes are a hole on
    /// disk. Their headers survive, so only the packet's own MD5 can tell.
    holed_packets: &'a [usize],
}

struct PartialVolumeFixture {
    payload: Vec<u8>,
    short_volume_filename: String,
    short_volume_bytes: Vec<u8>,
    working_dir: PathBuf,
}

/// A job whose damage needs more recovery blocks than the one *complete* volume
/// carries, with the balance sitting in a second volume that lost an article.
///
/// The short volume is on disk with its surviving packets intact and the lost
/// article's bytes zeroed, its assembly entry is one segment short forever, and
/// the recovery set is installed carrying only the complete volume's blocks —
/// so every block the short volume still holds has to be recovered from the
/// bytes themselves or it is not counted at all.
async fn install_partial_volume_par2_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job: PartialVolumeJob<'_>,
) -> PartialVolumeFixture {
    let slice_size = PARTIAL_VOLUME_SLICE_SIZE;
    let slice_bytes = slice_size as usize;
    let payload_filename = "silver.horizon.mkv";
    let index_filename = "silver.horizon.par2";
    let whole_volume_filename = "silver.horizon.vol00+02.par2";
    let short_volume_filename = "silver.horizon.vol02+02.par2";

    let payload: Vec<u8> = (0..(PARTIAL_VOLUME_PAYLOAD_SLICES * slice_bytes) as u32)
        .map(|value| (value % 251) as u8)
        .collect();
    let mut damaged = payload.clone();
    for slice in 0..job.damaged_slices {
        damaged[slice * slice_bytes..(slice + 1) * slice_bytes].fill(0);
    }

    // Four blocks split two-and-two. The set installed below keeps only the
    // first two; the rest must come off the short volume's disk bytes.
    let full_set = build_repairable_par2_set(payload_filename, &payload, slice_size, 4);
    let recovery_set_id = *full_set.recovery_set_id.as_bytes();
    let slice_data = |exponent: u32| -> Vec<u8> {
        full_set.recovery_slices[&exponent]
            .data
            .as_bytes()
            .expect("test recovery slices are built in memory")
            .to_vec()
    };
    let whole_volume_bytes = build_test_par2_recovery_volume(
        recovery_set_id,
        &[(0, &slice_data(0)), (1, &slice_data(1))],
    );
    let mut short_volume_bytes = build_test_par2_recovery_volume(
        recovery_set_id,
        &[(2, &slice_data(2)), (3, &slice_data(3))],
    );
    for packet_index in job.holed_packets {
        punch_recovery_packet_payload(&mut short_volume_bytes, *packet_index, slice_bytes);
    }

    let par2_bytes = build_test_par2_index(payload_filename, &payload, slice_size);
    let payload_segment_bytes = (payload.len() / 2) as u32;
    let packet_len = par2_rs::packet::header::HEADER_SIZE + 4 + slice_bytes;
    // The lost article is the second packet's payload, so the surviving article
    // covers the first packet and the second packet's header.
    let short_head_bytes = (packet_len + par2_rs::packet::header::HEADER_SIZE + 4) as u32;
    let short_tail_bytes = slice_bytes as u32;

    let spec = JobSpec {
        name: job.name.to_string(),
        password: None,
        total_bytes: (payload.len()
            + par2_bytes.len()
            + whole_volume_bytes.len()
            + short_volume_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: payload_segment_bytes,
                        message_id: format!("{}-payload-0@example.com", job.name),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: payload_segment_bytes,
                        message_id: format!("{}-payload-1@example.com", job.name),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: format!("{}-index@example.com", job.name),
                }],
            },
            FileSpec {
                filename: whole_volume_filename.to_string(),
                role: FileRole::from_filename(whole_volume_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: whole_volume_bytes.len() as u32,
                    message_id: format!("{}-vol00@example.com", job.name),
                }],
            },
            FileSpec {
                filename: short_volume_filename.to_string(),
                role: FileRole::from_filename(short_volume_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: short_head_bytes,
                        message_id: format!("{}-vol02-0@example.com", job.name),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: short_tail_bytes,
                        message_id: format!("{}-vol02-1@example.com", job.name),
                    },
                ],
            },
        ],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        let payload_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        for segment_number in 0..2 {
            state
                .assembly
                .file_mut(payload_id)
                .unwrap()
                .commit_segment(segment_number, payload_segment_bytes)
                .unwrap();
        }
    }

    write_and_complete_file(pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(
        pipeline,
        job_id,
        2,
        whole_volume_filename,
        &whole_volume_bytes,
    )
    .await;

    // The short volume: bytes on disk, one article that will never arrive.
    tokio::fs::write(working_dir.join(short_volume_filename), &short_volume_bytes)
        .await
        .unwrap();
    let short_volume_id = NzbFileId {
        job_id,
        file_index: 3,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(short_volume_id).unwrap();
        file.record_placement(0, 0, short_head_bytes);
        file.commit_segment(0, short_head_bytes).unwrap();
    }
    pipeline
        .unavailable_promoted_recovery_segments
        .insert(SegmentId {
            file_id: short_volume_id,
            segment_number: 1,
        });

    let mut installed_set = full_set.clone();
    installed_set.recovery_slices.remove(&2);
    installed_set.recovery_slices.remove(&3);
    install_test_par2_runtime(
        pipeline,
        job_id,
        installed_set,
        &[
            (1, index_filename, 0, false),
            (2, whole_volume_filename, 2, true),
            (3, short_volume_filename, 0, true),
        ],
    );

    PartialVolumeFixture {
        payload,
        short_volume_filename: short_volume_filename.to_string(),
        short_volume_bytes,
        working_dir,
    }
}

/// Recovery packets of a short volume posted one article per packet.
///
/// The two-article fixture above cannot express a volume that takes *more* bytes
/// and is still short: its only outstanding article is the one that would
/// complete it. This one loses its middle article and keeps its last, so the
/// volume grows on disk twice while never completing.
struct GrowingVolumeFixture {
    working_dir: PathBuf,
    volume_filename: String,
    /// The volume's packets in posting order, one per article.
    packets: Vec<Vec<u8>>,
}

impl GrowingVolumeFixture {
    fn packet_len(&self) -> usize {
        self.packets[0].len()
    }

    /// The volume as it looks on disk once `arrived` articles have landed: the
    /// packets that came, and holes where the rest will go.
    fn on_disk(&self, arrived: &[usize]) -> Vec<u8> {
        let mut bytes = vec![0u8; self.packets.len() * self.packet_len()];
        for index in arrived {
            let start = index * self.packet_len();
            bytes[start..start + self.packet_len()].copy_from_slice(&self.packets[*index]);
        }
        bytes
    }
}

async fn install_growing_partial_volume_par2_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
) -> GrowingVolumeFixture {
    let slice_size = PARTIAL_VOLUME_SLICE_SIZE;
    let slice_bytes = slice_size as usize;
    let payload_filename = "ivory.meadow.mkv";
    let index_filename = "ivory.meadow.par2";
    let whole_volume_filename = "ivory.meadow.vol00+01.par2";
    let short_volume_filename = "ivory.meadow.vol01+03.par2";

    let payload: Vec<u8> = (0..(PARTIAL_VOLUME_PAYLOAD_SLICES * slice_bytes) as u32)
        .map(|value| (value % 241) as u8)
        .collect();
    let full_set = build_repairable_par2_set(payload_filename, &payload, slice_size, 4);
    let recovery_set_id = *full_set.recovery_set_id.as_bytes();
    let slice_data = |exponent: u32| -> Vec<u8> {
        full_set.recovery_slices[&exponent]
            .data
            .as_bytes()
            .expect("test recovery slices are built in memory")
            .to_vec()
    };
    let whole_volume_bytes =
        build_test_par2_recovery_volume(recovery_set_id, &[(0, &slice_data(0))]);
    // One packet per article, so the volume can take an article without being
    // finished by it.
    let packets: Vec<Vec<u8>> = (1..4u32)
        .map(|exponent| {
            build_test_par2_recovery_volume(recovery_set_id, &[(exponent, &slice_data(exponent))])
        })
        .collect();
    let packet_len = packets[0].len();

    let par2_bytes = build_test_par2_index(payload_filename, &payload, slice_size);
    let payload_segment_bytes = (payload.len() / 2) as u32;

    let spec = JobSpec {
        name: "Ivory Meadow Growing Volume".to_string(),
        password: None,
        total_bytes: (payload.len()
            + par2_bytes.len()
            + whole_volume_bytes.len()
            + packets.len() * packet_len) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: payload_segment_bytes,
                        message_id: "ivory-meadow-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: payload_segment_bytes,
                        message_id: "ivory-meadow-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "ivory-meadow-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: whole_volume_filename.to_string(),
                role: FileRole::from_filename(whole_volume_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: whole_volume_bytes.len() as u32,
                    message_id: "ivory-meadow-vol00@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: short_volume_filename.to_string(),
                role: FileRole::from_filename(short_volume_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: (0..3u32)
                    .map(|ordinal| {
                        segment_spec! {
                            number: ordinal,
                            bytes: packet_len as u32,
                            message_id: format!("ivory-meadow-vol01-{ordinal}@example.com"),
                        }
                    })
                    .collect(),
            },
        ],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        let payload_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        for segment_number in 0..2 {
            state
                .assembly
                .file_mut(payload_id)
                .unwrap()
                .commit_segment(segment_number, payload_segment_bytes)
                .unwrap();
        }
    }

    write_and_complete_file(pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(
        pipeline,
        job_id,
        2,
        whole_volume_filename,
        &whole_volume_bytes,
    )
    .await;

    let fixture = GrowingVolumeFixture {
        working_dir,
        volume_filename: short_volume_filename.to_string(),
        packets,
    };

    // Only the volume's first article has landed; its second has run out of
    // servers, and its third has yet to arrive.
    tokio::fs::write(
        fixture.working_dir.join(&fixture.volume_filename),
        fixture.on_disk(&[0]),
    )
    .await
    .unwrap();
    let short_volume_id = NzbFileId {
        job_id,
        file_index: 3,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(short_volume_id).unwrap();
        file.record_placement(0, 0, packet_len as u32);
        file.commit_segment(0, packet_len as u32).unwrap();
    }
    pipeline
        .unavailable_promoted_recovery_segments
        .insert(SegmentId {
            file_id: short_volume_id,
            segment_number: 1,
        });

    let mut installed_set = full_set.clone();
    for exponent in 1..4u32 {
        installed_set.recovery_slices.remove(&exponent);
    }
    install_test_par2_runtime(
        pipeline,
        job_id,
        installed_set,
        &[
            (1, index_filename, 0, false),
            (2, whole_volume_filename, 1, true),
            (3, short_volume_filename, 0, true),
        ],
    );

    fixture
}

const TWO_SET_SLICE_SIZE: u64 = 64;

const LARGER_PAYLOAD: &str = "silver.horizon.mkv";

const LARGER_INDEX: &str = "silver.horizon.par2";

const LARGER_VOLUME: &str = "silver.horizon.vol00+08.par2";

const SMALLER_PAYLOAD: &str = "amber.trail.mkv";

const SMALLER_INDEX: &str = "amber.trail.par2";

const SMALLER_VOLUME: &str = "amber.trail.vol00+04.par2";

/// One posting carrying two independent recovery sets.
///
/// The sets describe different files and share no bytes, and one protects four
/// times the payload of the other — so which of them is served has to be a
/// decision rather than an accident of arrival order. File indices are fixed
/// so a test can name them: 0/1/2 are the larger set's payload, index and
/// volume, 3/4/5 the smaller set's.
struct TwoSetPosting {
    larger_payload: Vec<u8>,
    larger_index: Vec<u8>,
    larger_volume: Vec<u8>,
    smaller_payload: Vec<u8>,
    smaller_index: Vec<u8>,
    smaller_volume: Vec<u8>,
}

impl TwoSetPosting {
    fn build() -> Self {
        let larger_payload: Vec<u8> = (0..256u32).map(|value| (value % 251) as u8).collect();
        let smaller_payload: Vec<u8> = (0..128u32).map(|value| (value % 241) as u8).collect();
        let larger_index = build_test_par2_index_for_files(
            &[(LARGER_PAYLOAD, &larger_payload)],
            TWO_SET_SLICE_SIZE,
        );
        let smaller_index = build_test_par2_index_for_files(
            &[(SMALLER_PAYLOAD, &smaller_payload)],
            TWO_SET_SLICE_SIZE,
        );
        let recovery_slice = |fill: u8| vec![fill; TWO_SET_SLICE_SIZE as usize];
        let larger_slices: Vec<Vec<u8>> =
            (0..8u8).map(|index| recovery_slice(0xB0 + index)).collect();
        let smaller_slices: Vec<Vec<u8>> =
            (0..4u8).map(|index| recovery_slice(0xA0 + index)).collect();
        let larger_volume = build_test_par2_recovery_volume(
            *Self::recovery_set_id(&larger_index).as_bytes(),
            &larger_slices
                .iter()
                .enumerate()
                .map(|(exponent, slice)| (exponent as u32, slice.as_slice()))
                .collect::<Vec<_>>(),
        );
        let smaller_volume = build_test_par2_recovery_volume(
            *Self::recovery_set_id(&smaller_index).as_bytes(),
            &smaller_slices
                .iter()
                .enumerate()
                .map(|(exponent, slice)| (exponent as u32, slice.as_slice()))
                .collect::<Vec<_>>(),
        );
        Self {
            larger_payload,
            larger_index,
            larger_volume,
            smaller_payload,
            smaller_index,
            smaller_volume,
        }
    }

    fn recovery_set_id(par2_bytes: &[u8]) -> par2_rs::RecoverySetId {
        par2_rs::Par2FileSet::from_files(&[par2_bytes])
            .expect("the fixture index must parse")
            .recovery_set_id
    }

    fn spec(&self) -> JobSpec {
        let payload_segment = (self.larger_payload.len() / 2) as u32;
        let smaller_segment = (self.smaller_payload.len() / 2) as u32;
        JobSpec {
            name: "Two Recovery Sets".to_string(),
            password: None,
            total_bytes: (self.larger_payload.len() + self.smaller_payload.len()) as u64,
            category: None,
            metadata: vec![],
            files: vec![
                FileSpec {
                    filename: LARGER_PAYLOAD.to_string(),
                    role: FileRole::from_filename(LARGER_PAYLOAD),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![
                        segment_spec! {
                            number: 0,
                            bytes: payload_segment,
                            message_id: "two-sets-larger-0@example.com".to_string(),
                        },
                        segment_spec! {
                            number: 1,
                            bytes: payload_segment,
                            message_id: "two-sets-larger-1@example.com".to_string(),
                        },
                    ],
                },
                FileSpec {
                    filename: LARGER_INDEX.to_string(),
                    role: FileRole::from_filename(LARGER_INDEX),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: self.larger_index.len() as u32,
                        message_id: "two-sets-larger-index@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: LARGER_VOLUME.to_string(),
                    role: FileRole::from_filename(LARGER_VOLUME),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: self.larger_volume.len() as u32,
                        message_id: "two-sets-larger-volume@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: SMALLER_PAYLOAD.to_string(),
                    role: FileRole::from_filename(SMALLER_PAYLOAD),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![
                        segment_spec! {
                            number: 0,
                            bytes: smaller_segment,
                            message_id: "two-sets-smaller-0@example.com".to_string(),
                        },
                        segment_spec! {
                            number: 1,
                            bytes: smaller_segment,
                            message_id: "two-sets-smaller-1@example.com".to_string(),
                        },
                    ],
                },
                FileSpec {
                    filename: SMALLER_INDEX.to_string(),
                    role: FileRole::from_filename(SMALLER_INDEX),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: self.smaller_index.len() as u32,
                        message_id: "two-sets-smaller-index@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: SMALLER_VOLUME.to_string(),
                    role: FileRole::from_filename(SMALLER_VOLUME),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: self.smaller_volume.len() as u32,
                        message_id: "two-sets-smaller-volume@example.com".to_string(),
                    }],
                },
            ],
        }
    }

    /// Seed the job and land both index files, exactly as the download path
    /// does — an index is parsed because it finished arriving — without
    /// parsing either yet.
    async fn install(&self, pipeline: &mut Pipeline, job_id: JobId) -> PathBuf {
        let working_dir = insert_active_job(pipeline, job_id, self.spec()).await;
        write_and_complete_file(pipeline, job_id, 1, LARGER_INDEX, &self.larger_index).await;
        write_and_complete_file(pipeline, job_id, 4, SMALLER_INDEX, &self.smaller_index).await;
        working_dir
    }
}

async fn load_par2_index(pipeline: &mut Pipeline, job_id: JobId, file_index: u32) {
    pipeline
        .try_load_par2_metadata(job_id, NzbFileId { job_id, file_index })
        .await;
}

fn observe_recovery_prefix(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    set_id: par2_rs::RecoverySetId,
) {
    pipeline
        .ensure_par2_runtime(job_id)
        .files
        .entry(file_index)
        .or_default()
        .discovery = Par2DiscoveryState::PrefixProbed {
        set_ids: vec![set_id],
    };
}

fn served_set_describes(pipeline: &Pipeline, job_id: JobId, filename: &str) -> bool {
    pipeline
        .par2_set(job_id)
        .is_some_and(|set| set.files.values().any(|desc| desc.filename == filename))
}

const VOLUME_BOOTSTRAP_PAYLOAD: &str = "copper.aurora.bin";

const VOLUME_BOOTSTRAP_INDEX: &str = "copper.aurora.par2";

const VOLUME_BOOTSTRAP_VOLUME: &str = "copper.aurora.vol00+01.par2";

fn volume_only_par2_bootstrap_fixture() -> (JobSpec, Vec<u8>, par2_rs::RecoverySetId) {
    let payload = b"copper aurora payload";
    let metadata = build_test_par2_index(VOLUME_BOOTSTRAP_PAYLOAD, payload, 64);
    let recovery_set_id = par2_rs::Par2FileSet::from_files(&[&metadata])
        .expect("fixture metadata must parse")
        .recovery_set_id;
    let mut volume = metadata;
    volume.extend_from_slice(&build_test_par2_recovery_volume(
        *recovery_set_id.as_bytes(),
        &[(0, &[0xC3; 64])],
    ));
    let spec = JobSpec {
        name: "Volume Metadata Bootstrap".to_string(),
        password: None,
        total_bytes: (payload.len() + volume.len() + 1) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: VOLUME_BOOTSTRAP_PAYLOAD.to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: payload.len() as u32,
                    message_id: "volume-bootstrap-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: VOLUME_BOOTSTRAP_INDEX.to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 1,
                    message_id: "volume-bootstrap-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: VOLUME_BOOTSTRAP_VOLUME.to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 1,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: volume.len() as u32,
                    message_id: "volume-bootstrap-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    (spec, volume, recovery_set_id)
}

/// The name the recovery set speaks for. Nothing in the posting is called this:
/// the parts join into it.
const SPLIT_JOIN_JOINED_FILENAME: &str = "Ivory.Meadow.mkv";

/// A recovery volume in the posting, so the fail arithmetic has capacity to
/// spend before the repairer is ever asked for a verdict.
const SPLIT_JOIN_RECOVERY_FILENAME: &str = "Ivory.Meadow.mkv.vol00+02.par2";

const SPLIT_JOIN_RECOVERY_BLOCKS: u32 = 2;

fn split_join_payload(len: usize) -> Vec<u8> {
    (0..len).map(|value| (value % 251) as u8).collect()
}

/// One posted part of a plain split set.
struct SplitJoinPart {
    filename: String,
    /// Article sizes in posting order. More than one entry is what lets a part
    /// land short of its articles.
    segments: Vec<u32>,
    /// How many of those articles arrived.
    arrived_segments: usize,
    /// What the arrival actually left on disk.
    on_disk: Vec<u8>,
}

/// A part every article of which landed intact.
fn whole_split_join_part(filename: &str, bytes: &[u8]) -> SplitJoinPart {
    SplitJoinPart {
        filename: filename.to_string(),
        segments: vec![bytes.len() as u32],
        arrived_segments: 1,
        on_disk: bytes.to_vec(),
    }
}

/// A posting of plain split parts whose recovery set is computed over the file
/// the parts join into.
struct SplitJoinPosting {
    job_name: &'static str,
    joined: Vec<u8>,
    slice_size: u64,
    parts: Vec<SplitJoinPart>,
    /// Parts whose first 16 KiB the decode path captured. A part that begins
    /// where the joined file begins reproduces the joined description's 16 KiB
    /// hash exactly, which is how content binding finds it.
    prefix_captured: Vec<usize>,
    /// When set, the recovery set describes these files instead of the joined
    /// one — the ordinary shape, where the parts protect themselves.
    describes_parts: bool,
}

impl SplitJoinPosting {
    fn recovery_file_index(&self) -> u32 {
        self.parts.len() as u32
    }

    async fn install(&self, pipeline: &mut Pipeline, job_id: JobId) -> PathBuf {
        let recovery_bytes = vec![0xAAu8; 64];
        let mut files: Vec<FileSpec> = self
            .parts
            .iter()
            .enumerate()
            .map(|(index, part)| FileSpec {
                filename: part.filename.clone(),
                role: FileRole::from_filename(&part.filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: part
                    .segments
                    .iter()
                    .enumerate()
                    .map(|(ordinal, bytes)| {
                        segment_spec! {
                            number: ordinal as u32,
                            bytes: *bytes,
                            message_id: format!("split-join-{index}-{ordinal}@example.com"),
                        }
                    })
                    .collect(),
            })
            .collect();
        files.push(FileSpec {
            filename: SPLIT_JOIN_RECOVERY_FILENAME.to_string(),
            role: FileRole::from_filename(SPLIT_JOIN_RECOVERY_FILENAME),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: recovery_bytes.len() as u32,
                message_id: "split-join-recovery@example.com".to_string(),
            }],
        });

        let spec = JobSpec {
            name: self.job_name.to_string(),
            password: None,
            total_bytes: files
                .iter()
                .flat_map(|file| file.segments.iter())
                .map(|segment| u64::from(segment.bytes))
                .sum(),
            category: None,
            metadata: vec![],
            files,
        };
        let working_dir = insert_active_job(pipeline, job_id, spec).await;

        for (index, part) in self.parts.iter().enumerate() {
            let file_index = index as u32;
            tokio::fs::write(working_dir.join(&part.filename), &part.on_disk)
                .await
                .unwrap();
            let file_id = NzbFileId { job_id, file_index };
            {
                let state = pipeline.jobs.get_mut(&job_id).unwrap();
                let file = state.assembly.file_mut(file_id).unwrap();
                let mut offset = 0u64;
                for (ordinal, bytes) in part.segments.iter().enumerate().take(part.arrived_segments)
                {
                    file.record_placement(ordinal as u32, offset, *bytes);
                    file.commit_segment(ordinal as u32, *bytes).unwrap();
                    offset += u64::from(*bytes);
                }
            }
            if self.prefix_captured.contains(&index) {
                let window = part.on_disk.len().min(PAR2_HASH_16K_BYTES);
                pipeline
                    .file_prefix_16k
                    .insert(file_id, part.on_disk[..window].to_vec());
            }
            pipeline
                .refresh_archive_state_for_completed_file(job_id, file_id, true)
                .await;
        }

        write_and_complete_file(
            pipeline,
            job_id,
            self.recovery_file_index(),
            SPLIT_JOIN_RECOVERY_FILENAME,
            &recovery_bytes,
        )
        .await;

        let par2_set = if self.describes_parts {
            let described: Vec<(&str, &[u8])> = self
                .parts
                .iter()
                .map(|part| (part.filename.as_str(), part.on_disk.as_slice()))
                .collect();
            build_repairable_par2_set_for_files(
                &described,
                self.slice_size,
                SPLIT_JOIN_RECOVERY_BLOCKS as usize,
            )
        } else {
            build_repairable_par2_set(
                SPLIT_JOIN_JOINED_FILENAME,
                &self.joined,
                self.slice_size,
                SPLIT_JOIN_RECOVERY_BLOCKS as usize,
            )
        };
        install_test_par2_runtime(
            pipeline,
            job_id,
            par2_set,
            &[(
                self.recovery_file_index(),
                SPLIT_JOIN_RECOVERY_FILENAME,
                SPLIT_JOIN_RECOVERY_BLOCKS,
                true,
            )],
        );

        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state.download_queue = DownloadQueue::new();
            state.recovery_queue = DownloadQueue::new();
            state.status = JobStatus::Downloading;
            state.refresh_runtime_lanes_from_status();
        }

        working_dir
    }
}

/// Drive the completion gate the way a live pipeline would, resolving the
/// extraction tasks it spawns instead of racing them.
async fn settle_split_join_completion(pipeline: &mut Pipeline, job_id: JobId) {
    for _ in 0..12 {
        if matches!(
            job_status_for_assert(pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        pipeline.check_job_completion(job_id).await;
        while pipeline
            .inflight_extractions
            .get(&job_id)
            .is_some_and(|sets| !sets.is_empty())
        {
            let done = next_extraction_done(pipeline).await;
            pipeline.handle_extraction_done(done).await;
        }
        pump_pipeline_runtime_queues(pipeline).await;
    }
}

fn split_join_delivered_dir(pipeline: &Pipeline, job_name: &str) -> PathBuf {
    pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name))
}

fn delivered_entry_names(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.file_name().to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn split_join_failure_error(pipeline: &Pipeline, job_id: JobId) -> String {
    match job_status_for_assert(pipeline, job_id) {
        Some(JobStatus::Failed { error }) => error,
        _ => String::new(),
    }
}

/// The shape that outgrows the transient decode-matrix budget.
///
/// The workspace a repair plan needs is set by the damage, not by streaming
/// buffer tuning: it grows with `missing²` plus `missing × total`, and the
/// budget it is measured against has a floor of its own well above weaver's
/// configured limit. Working the arithmetic backwards, nothing under
/// ~16,384 total slices can reach that floor at any damage level, and at the
/// format's 32,768-slice ceiling it takes more than ~11,994 missing slices —
/// upwards of a third of the set gone, with recovery blocks for every one of
/// them. No real posting is shaped like this; the point of pinning it is that
/// the refusal is a *budget* decision and says so.
const MATRIX_BUDGET_FILENAME: &str = "silver.horizon.bin";

const MATRIX_BUDGET_SLICE_SIZE: u64 = 16;

const MATRIX_BUDGET_TOTAL_SLICES: usize = 32_768;

const MATRIX_BUDGET_MISSING_SLICES: usize = 13_000;

/// Payload bytes with no repeating structure, so no damaged window can be
/// mistaken for an intact slice and the missing count is exactly what the
/// fixture punched out.
fn matrix_budget_payload() -> Vec<u8> {
    let mut data = vec![0u8; MATRIX_BUDGET_TOTAL_SLICES * MATRIX_BUDGET_SLICE_SIZE as usize];
    let mut state = 0x2545_f491_4f6c_dd1du64;
    for chunk in data.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    data
}

/// The same payload with its first `MATRIX_BUDGET_MISSING_SLICES` slices
/// zeroed — aligned, so every surviving slice is still found where the set
/// describes it.
fn matrix_budget_damaged_payload(payload: &[u8]) -> Vec<u8> {
    let mut damaged = payload.to_vec();
    damaged[..MATRIX_BUDGET_MISSING_SLICES * MATRIX_BUDGET_SLICE_SIZE as usize].fill(0);
    damaged
}

/// Analysis only — never a repair. The budget decision is reached before any
/// planning, so this returns the verdict without spending a solve that, at this
/// shape, would be a 13,000-row field inversion.
fn analyze_with_memory_limit(
    working_dir: &std::path::Path,
    par2_set: &Par2FileSet,
    memory_limit: usize,
) -> par2_rs::Par2RepairOutcome {
    let mut options = par2_rs::Par2RepairerOptions::new(working_dir.to_path_buf(), Vec::new());
    options.file_set = Some(par2_set.clone());
    options.repair = false;
    options.memory_limit = Some(memory_limit);
    par2_rs::Par2Repairer::new(options)
        .verify_or_repair()
        .unwrap()
}

/// A job with no parsed recovery set and two PAR2 files it could promote for
/// metadata: an index and a second index, neither yet tried.
async fn metadata_promotion_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
) -> (PathBuf, SegmentId, SegmentId) {
    let payload_filename = "silver-horizon.mkv";
    let first_index = "silver-horizon.par2";
    let second_index = "silver-horizon.vol00+01.par2";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();

    let spec = JobSpec {
        name: job_name.to_string(),
        password: None,
        total_bytes: payload.len() as u64 + 128,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "metadata-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "metadata-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: first_index.to_string(),
                role: FileRole::from_filename(first_index),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "metadata-index-a@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: second_index.to_string(),
                role: FileRole::from_filename(second_index),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "metadata-index-b@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    let first_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 1,
        },
        segment_number: 0,
    };
    let second_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 2,
        },
        segment_number: 0,
    };
    (working_dir, first_segment, second_segment)
}

fn drain_promoted_segments(pipeline: &mut Pipeline, job_id: JobId) -> Vec<SegmentId> {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state
        .download_queue
        .drain_all()
        .into_iter()
        .map(|work| work.segment_id)
        .collect()
}

/// [`placement_par2_file_set`] with the per-slice IFSC CRC32s the whole-file-CRC
/// arm folds. The base helper ships none, which is the shape that proves the arm
/// refuses rather than guesses when the table is absent.
fn placement_par2_file_set_with_slice_checksums(files: &[(String, Vec<u8>)]) -> Par2FileSet {
    let mut set = placement_par2_file_set(files);
    let slice_size = set.slice_size;
    let file_ids = set.recovery_file_ids.clone();
    for (file_id, (_, bytes)) in file_ids.iter().zip(files.iter()) {
        let mut checksums = Vec::new();
        let mut offset = 0usize;
        while offset < bytes.len() {
            let end = (offset + slice_size as usize).min(bytes.len());
            let slice = &bytes[offset..end];
            let mut state = par2_rs::SliceChecksumState::new();
            state.update(slice);
            let (crc32, md5) =
                state.finalize(((slice.len() as u64) < slice_size).then_some(slice_size));
            checksums.push(par2_rs::SliceChecksum { crc32, md5 });
            offset = end;
        }
        set.slice_checksums.insert(*file_id, checksums);
    }
    set
}

/// The streamed state a metadata-early download leaves behind: the folded
/// whole-file CRC32, whether every article's declared yEnc part CRC verified,
/// and whatever digest that generation carries — usually none at all.
fn set_streamed_file_crc(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    crc32: u32,
    all_parts_crc_verified: bool,
    md5: Option<[u8; 16]>,
) {
    pipeline
        .ensure_par2_runtime(job_id)
        .completed_checksums
        .insert(
            NzbFileId { job_id, file_index },
            crate::pipeline::CompletedFileChecksum {
                md5,
                crc32,
                all_parts_crc_verified,
            },
        );
}

/// Stage a job whose files sit at their described names, served by a set that
/// carries slice checksums.
async fn stage_file_crc_shape(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    described: &[(&str, Vec<u8>)],
    on_disk: &[Vec<u8>],
) -> (PathBuf, Arc<Par2FileSet>) {
    assert_eq!(described.len(), on_disk.len());
    let files: Vec<(String, u32)> = described
        .iter()
        .zip(on_disk.iter())
        .map(|((name, _), disk)| ((*name).to_string(), disk.len() as u32))
        .collect();
    let spec = standalone_job_spec(job_name, &files);
    let working_dir = insert_active_job(pipeline, job_id, spec).await;
    for (index, ((name, _), disk)) in described.iter().zip(on_disk.iter()).enumerate() {
        write_and_complete_file(pipeline, job_id, index as u32, name, disk).await;
    }
    let described_pairs: Vec<(String, Vec<u8>)> = described
        .iter()
        .map(|(name, canonical)| ((*name).to_string(), canonical.clone()))
        .collect();
    install_test_par2_runtime(
        pipeline,
        job_id,
        placement_par2_file_set_with_slice_checksums(&described_pairs),
        &[],
    );
    let par2_set = Arc::clone(pipeline.par2_set(job_id).expect("served recovery set"));
    (working_dir, par2_set)
}

const SEEDED_SLICE_SIZE: u64 = 64;

const SEEDED_FILE_SLICES: usize = 4;

const SEEDED_INTACT: &str = "silver.horizon.e01.mkv";

const SEEDED_DAMAGED: &str = "silver.horizon.e02.mkv";

/// A two-payload job whose second file is damaged on disk, with a real PAR2
/// index beside them.
///
/// The first file is intact and — when `cover_intact_with_grid` — carries an
/// in-stream verdict for every one of its slices, which is the evidence the
/// authoritative pass is supposed to be able to act on. Neither file carries a
/// completed-file checksum, so no *committed* evidence can be built for either:
/// whatever the analysis manages to skip, it skipped on slice evidence alone.
async fn install_seeded_evidence_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    cover_intact_with_grid: bool,
) -> (PathBuf, Vec<u8>, Vec<u8>) {
    let slice_bytes = SEEDED_SLICE_SIZE as usize;
    let width = SEEDED_FILE_SLICES * slice_bytes;
    let intact: Vec<u8> = (0..width as u32).map(|value| (value % 251) as u8).collect();
    let other: Vec<u8> = (0..width as u32)
        .map(|value| ((value * 7 + 3) % 251) as u8)
        .collect();
    let mut damaged_on_disk = other.clone();
    damaged_on_disk[..slice_bytes].fill(0);

    let index_filename = "silver.horizon.par2";
    let par2_bytes = build_test_par2_index_for_files(
        &[(SEEDED_INTACT, &intact), (SEEDED_DAMAGED, &other)],
        SEEDED_SLICE_SIZE,
    );
    let spec = standalone_job_spec(
        job_name,
        &[
            (SEEDED_INTACT.to_string(), intact.len() as u32),
            (SEEDED_DAMAGED.to_string(), other.len() as u32),
            (index_filename.to_string(), par2_bytes.len() as u32),
        ],
    );
    let working_dir = insert_active_job(pipeline, job_id, spec).await;
    write_and_complete_file(pipeline, job_id, 0, SEEDED_INTACT, &intact).await;
    write_and_complete_file(pipeline, job_id, 1, SEEDED_DAMAGED, &damaged_on_disk).await;
    write_and_complete_file(pipeline, job_id, 2, index_filename, &par2_bytes).await;

    install_test_par2_runtime(
        pipeline,
        job_id,
        build_repairable_par2_set_for_files(
            &[(SEEDED_INTACT, &intact), (SEEDED_DAMAGED, &other)],
            SEEDED_SLICE_SIZE,
            0,
        ),
        &[(2, index_filename, 0, false)],
    );

    if cover_intact_with_grid {
        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        for slice_index in 0..SEEDED_FILE_SLICES {
            let start = slice_index * slice_bytes;
            let block = &intact[start..start + slice_bytes];
            let block_crc = par2_rs::checksum::crc32(block);
            pipeline.note_block_crc_segments(
                file_id,
                start as u64,
                slice_bytes as u64,
                block_crc,
                true,
                false,
                &[weaver_yenc::Segment {
                    file_offset: start as u64,
                    len: slice_bytes as u64,
                    crc32: block_crc,
                }],
            );
        }
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    (working_dir, intact, other)
}

/// Bytes the whole set would cost to read, so a bound can be stated in terms of
/// the fixture rather than a magic number.
fn seeded_evidence_total_payload_bytes(intact: &[u8], other: &[u8]) -> u64 {
    (intact.len() + other.len()) as u64
}

const PARKED_RECOVERY_SLICE_SIZE: u64 = 64;

const PARKED_RECOVERY_PAYLOAD: &str = "silver.horizon.e04.mkv";

const PARKED_RECOVERY_INDEX: &str = "silver.horizon.par2";

const PARKED_RECOVERY_VOLUME: &str = "silver.horizon.vol00+01.par2";

/// A damaged job whose only recovery volume is still parked.
///
/// The payload's second slice is zeroed on disk and the set as installed
/// carries no recovery at all, so the first authoritative pass has to promote
/// the volume and wait — which is the state this section is about. The volume's
/// bytes are handed back so the test can land it afterwards.
struct ParkedRecoveryFixture {
    payload_path: PathBuf,
    original_payload: Vec<u8>,
    recovery_volume: Vec<u8>,
    recovery_file_id: NzbFileId,
}

async fn install_parked_recovery_par2_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
) -> ParkedRecoveryFixture {
    let slice_size = PARKED_RECOVERY_SLICE_SIZE;
    let slice_bytes = slice_size as usize;
    let original_payload: Vec<u8> = (0..(2 * slice_bytes) as u32)
        .map(|value| (value % 251) as u8)
        .collect();
    let mut damaged_payload = original_payload.clone();
    damaged_payload[slice_bytes..].fill(0);

    // Built with its recovery block so the volume's bytes are real, then
    // installed without it: the set weaver holds is the one an index alone
    // describes, and the block arrives with the volume.
    let full_set =
        build_repairable_par2_set(PARKED_RECOVERY_PAYLOAD, &original_payload, slice_size, 1);
    let recovery_slice = full_set.recovery_slices[&0]
        .data
        .as_bytes()
        .expect("test recovery slices are built in memory")
        .to_vec();
    let recovery_volume = build_test_par2_recovery_volume(
        *full_set.recovery_set_id.as_bytes(),
        &[(0, &recovery_slice)],
    );
    let par2_bytes = build_test_par2_index(PARKED_RECOVERY_PAYLOAD, &original_payload, slice_size);

    let spec = JobSpec {
        name: job_name.to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_volume.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: PARKED_RECOVERY_PAYLOAD.to_string(),
                role: FileRole::from_filename(PARKED_RECOVERY_PAYLOAD),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: slice_bytes as u32,
                        message_id: "silver-horizon-parked-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: slice_bytes as u32,
                        message_id: "silver-horizon-parked-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: PARKED_RECOVERY_INDEX.to_string(),
                role: FileRole::from_filename(PARKED_RECOVERY_INDEX),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "silver-horizon-parked-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: PARKED_RECOVERY_VOLUME.to_string(),
                role: FileRole::from_filename(PARKED_RECOVERY_VOLUME),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_volume.len() as u32,
                    message_id: "silver-horizon-parked-volume@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(PARKED_RECOVERY_PAYLOAD), &damaged_payload)
        .await
        .unwrap();
    let recovery_file_id = NzbFileId {
        job_id,
        file_index: 2,
    };
    {
        let payload_file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
        for segment_number in 0..2 {
            let file = state.assembly.file_mut(payload_file_id).unwrap();
            file.record_placement(
                segment_number,
                u64::from(segment_number) * slice_size,
                slice_bytes as u32,
            );
            file.commit_segment(segment_number, slice_bytes as u32)
                .unwrap();
        }
        // The volume is in the parked pool, which is exactly where a promotion
        // has to find it.
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: recovery_file_id,
                segment_number: 0,
            },
            message_id: MessageId::new("silver-horizon-parked-volume@example.com"),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 1000,
            byte_estimate: recovery_volume.len() as u32,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        });
    }
    write_and_complete_file(pipeline, job_id, 1, PARKED_RECOVERY_INDEX, &par2_bytes).await;

    let mut installed_set = full_set;
    installed_set.recovery_slices.clear();
    install_test_par2_runtime(
        pipeline,
        job_id,
        installed_set,
        &[
            (1, PARKED_RECOVERY_INDEX, 0, false),
            (2, PARKED_RECOVERY_VOLUME, 1, false),
        ],
    );

    ParkedRecoveryFixture {
        payload_path: working_dir.join(PARKED_RECOVERY_PAYLOAD),
        original_payload,
        recovery_volume,
        recovery_file_id,
    }
}

/// Land the promoted volume the way the wire does: its work leaves the queue,
/// its bytes reach the disk, and its packets merge into the set.
async fn land_parked_recovery_volume(
    pipeline: &mut Pipeline,
    job_id: JobId,
    fixture: &ParkedRecoveryFixture,
) {
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
    }
    write_and_complete_file(
        pipeline,
        job_id,
        fixture.recovery_file_id.file_index,
        PARKED_RECOVERY_VOLUME,
        &fixture.recovery_volume,
    )
    .await;
    pipeline
        .try_load_par2_metadata(job_id, fixture.recovery_file_id)
        .await;
}

/// A damaged, fully-drained job whose next completion check submits a
/// damaged-path analysis ticket: the payload is on disk with its second block
/// corrupted, the index and one recovery volume are complete, and no wire work
/// remains to hold the gate shut.
async fn insert_damaged_job_ready_for_analysis(
    pipeline: &mut Pipeline,
    job_id: JobId,
    tag: &str,
) -> std::path::PathBuf {
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: format!("Silver Horizon {tag}"),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: format!("{tag}-payload-0@example.com"),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: format!("{tag}-payload-1@example.com"),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: format!("{tag}-index@example.com"),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: format!("{tag}-recovery@example.com"),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Repairing;
        state.refresh_runtime_lanes_from_status();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(1, 64)
            .unwrap();
    }
    write_and_complete_file(pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );
    working_dir
}

/// Waits for the detached analysis worker to hand its verdict back, without
/// letting the pipeline see it. Tests that want the verdict *dropped* need the
/// message in hand to prove the fence rejects it.
async fn next_par2_analysis_done(pipeline: &mut Pipeline) -> crate::pipeline::Par2AnalysisWorkDone {
    let done = tokio::time::timeout(Duration::from_secs(10), pipeline.repair_work_done_rx.recv())
        .await
        .expect("the detached analysis should finish")
        .expect("the analysis completion channel should stay open");
    let crate::pipeline::RepairWorkDone::Par2(done) = done else {
        panic!("PAR2-only fixture returned a different repair format");
    };
    done
}

mod decode_matrix_s_own;
mod ignorable_furniture_inside_recovery;
mod par2_session_io_errors;
mod unavailable_promoted_recovery_promotes;
