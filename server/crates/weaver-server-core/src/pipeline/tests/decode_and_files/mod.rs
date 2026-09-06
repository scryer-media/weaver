use super::*;

/// A segmented spec whose file streams the *running* file hash on every commit.
///
/// A `Standalone` file that declares a whole-file CRC32 takes the deferred
/// CRC-metadata arm instead, which is keyed by offset and so never consults the
/// running offset at all. Only the streaming arm can observe a duplicate being
/// fed twice, and a RAR volume in a set with no PAR2 is the ordinary shape that
/// reaches it.
fn streaming_hash_job_spec(name: &str, filename: &str, segment_sizes: &[u32]) -> JobSpec {
    let mut spec = segmented_job_spec(name, filename, segment_sizes);
    spec.files[0].role = FileRole::RarVolume { volume_number: 0 };
    spec
}

/// Submit one uuencode part.
///
/// Every argument is in DECODED units except the job spec's declared sizes,
/// which stay encoded exactly as an NZB would carry them — that mismatch is the
/// whole point of these tests.
async fn submit_uu_segment(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    data: &[u8],
    damaged: bool,
    ended: bool,
) {
    submit_uu_segment_named(
        pipeline,
        file_id,
        segment_number,
        data,
        damaged,
        ended,
        "silver-horizon.bin",
    )
    .await;
}

/// Submit one uuencode part, stating what its `begin` header called the file.
///
/// A real post states the name exactly once, on the part that opens the body,
/// and continuation parts carry none at all — which is what `""` means here.
#[allow(clippy::too_many_arguments)]
pub(super) async fn submit_uu_segment_named(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    data: &[u8],
    damaged: bool,
    ended: bool,
    begin_name: &str,
) {
    pipeline
        .handle_decode_success(
            DecodeResult {
                encoding: SegmentEncoding::Uu(crate::pipeline::UuSegmentFacts { damaged, ended }),
                segment_id: SegmentId {
                    file_id,
                    segment_number,
                },
                raw_size: data.len() as u64,
                yenc_layout: YencLayoutAssertions {
                    file_size: 0,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: true,
                part_crc_verified: false,
                part_crc: 0,
                expected_file_crc: None,
                segments: Vec::new(),
                data: DecodedChunk::from(data.to_vec()),
                yenc_name: begin_name.to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            },
            SegmentSource {
                source_server_idx: None,
                exclude_servers: Vec::new(),
            },
        )
        .await;
}

/// A job whose declared segment sizes are uuencode-encoded (~1.38x the decoded
/// bytes), which is what an NZB really carries for a uuencode post.
fn uu_job_spec(decoded_sizes: &[usize]) -> JobSpec {
    let declared: Vec<u32> = decoded_sizes
        .iter()
        .map(|len| (*len as f64 * 1.38).ceil() as u32)
        .collect();
    let mut spec = standalone_job_spec(
        "Silver Horizon UU",
        &[("silver-horizon.bin".to_string(), declared[0])],
    );
    let file = &mut spec.files[0];
    file.segments.clear();
    for (index, bytes) in declared.iter().enumerate() {
        file.segments.push(segment_spec! {
            number: index as u32,
            bytes: *bytes,
            message_id: format!("uu-{index}@example.com"),
        });
    }
    spec
}

/// Number of segments queued for download on a job.
fn queued_segment_count(pipeline: &Pipeline, job_id: JobId) -> usize {
    pipeline
        .jobs
        .get(&job_id)
        .map(|state| state.download_queue.len())
        .unwrap_or(0)
}

/// A job carrying both encodings: yEnc files whose declared segment sizes run
/// ~1.03x their decoded bytes, and one uuencode file whose declared sizes run
/// ~1.38x — exactly as the two encodings appear in a real NZB, since the
/// `<segment bytes>` attribute is always the ENCODED figure.
fn mixed_encoding_job_spec(
    yenc_files: &[(&str, &[usize])],
    uu_file: (&str, &[usize]),
) -> (JobSpec, Vec<u32>, u32) {
    let mut files = Vec::new();
    let mut yenc_indices = Vec::new();

    for (filename, decoded_sizes) in yenc_files {
        yenc_indices.push(files.len() as u32);
        let index = files.len();
        files.push(FileSpec {
            filename: (*filename).to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: decoded_sizes
                .iter()
                .enumerate()
                .map(|(ordinal, decoded)| {
                    segment_spec! {
                        number: ordinal as u32,
                        bytes: (*decoded as f64 * 1.03).ceil() as u32,
                        message_id: format!("mixed-y{index}-{ordinal}@example.com"),
                    }
                })
                .collect(),
        });
    }

    let uu_index = files.len() as u32;
    files.push(FileSpec {
        filename: uu_file.0.to_string(),
        role: FileRole::Standalone,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: uu_file
            .1
            .iter()
            .enumerate()
            .map(|(ordinal, decoded)| {
                segment_spec! {
                    number: ordinal as u32,
                    bytes: (*decoded as f64 * 1.38).ceil() as u32,
                    message_id: format!("mixed-u-{ordinal}@example.com"),
                }
            })
            .collect(),
    });

    let spec = JobSpec {
        name: "Silver Horizon Mixed".to_string(),
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
    (spec, yenc_indices, uu_index)
}

/// A job holding one file under `posted_name`, with a recovery set describing
/// `described` files by their real names, and `prefix` captured for file 0.
///
/// The posted name and the described names are deliberately free to disagree:
/// that disagreement is what an obfuscated post *is*.
async fn obfuscated_binding_fixture(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
    posted_name: &str,
    described: &[(&str, &[u8])],
    prefix: &[u8],
) -> (Pipeline, NzbFileId) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Silver Horizon", &[(posted_name.to_string(), 4096)]),
    )
    .await;
    let par2_set = build_repairable_par2_set_for_files(described, 1024, 1);
    let set_id = par2_set.recovery_set_id;
    let runtime = pipeline.ensure_par2_runtime(job_id);
    runtime.served = Some(set_id);
    runtime.ensure_set_runtime(set_id).set = Some(std::sync::Arc::new(par2_set));
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    pipeline.file_prefix_16k.insert(file_id, prefix.to_vec());
    (pipeline, file_id)
}

/// A payload whose first 16 KiB are distinctive, so a content match is a real
/// match rather than an accident of everything being zeros.
fn binding_payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

mod pump_decode_queue_releases;
mod uuencode_sequential_assembly;
