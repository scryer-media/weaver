// A multi-volume RAR set posted under obfuscated, extensionless hex names.
//
// Nothing in such a filename says which set a volume belongs to or where in
// it the volume sits, so the volumes have to be grouped by what their headers
// say. The fixture is the RARLAB-written five-volume encrypted set; only the
// names change.

use super::*;

const PASSWORD: &str = "testpass123";
const MEMBER: &str = "test_clip.mkv";

fn rarlab_five_volume_set() -> Vec<Vec<u8>> {
    (1..=5)
        .map(|part| rar5_fixture_bytes(&format!("rar5_enc_mv_video.part{part}.rar")))
        .collect()
}

// A different 32-hex name per volume, none of them carrying an extension or
// any hint of order. Deliberately not sorted in volume order either.
fn hex_names(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| {
            let seed = (index as u128 + 7).wrapping_mul(0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c835);
            format!("{seed:032x}")
        })
        .collect()
}

async fn complete_job_and_drive_extraction(
    pipeline: &mut Pipeline,
    job_id: JobId,
    files: &[(String, Vec<u8>)],
) {
    for (index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(pipeline, job_id, index as u32, filename, bytes).await;
    }
    drain_rar_refreshes(pipeline).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.check_job_completion(job_id).await;
    drain_rar_refreshes(pipeline).await;
    drive_extractions_to_terminal(pipeline, job_id, 64).await;
}

fn assert_completed_with_member(
    pipeline: &Pipeline,
    job_id: JobId,
    complete_dir: &Path,
    name: &str,
) {
    let status = job_status_for_assert(pipeline, job_id);
    assert_eq!(
        status,
        Some(JobStatus::Complete),
        "the hex-named set must extract as one set"
    );
    let destination = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(name));
    let extracted = std::fs::metadata(destination.join(MEMBER))
        .expect("the member spanning every volume is extracted");
    assert!(extracted.len() > 0);
}

#[tokio::test]
async fn extensionless_hex_volumes_group_into_one_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30471);
    let name = "Hex Named Volumes";
    let names = hex_names(5);
    let files: Vec<(String, Vec<u8>)> = names.into_iter().zip(rarlab_five_volume_set()).collect();
    for (filename, _) in &files {
        assert_eq!(FileRole::from_filename(filename), FileRole::Unknown);
    }
    let mut spec = rar_job_spec(name, &files);
    spec.password = Some(PASSWORD.to_string());
    insert_active_job(&mut pipeline, job_id, spec).await;

    complete_job_and_drive_extraction(&mut pipeline, job_id, &files).await;

    assert_completed_with_member(&pipeline, job_id, &complete_dir, name);
}

// A hex-named set whose first volume never arrived takes the missing-volume
// route rather than an extraction attempt that cannot open, and with no PAR2
// to repair from the failure names every volume seen and why none of them is
// volume 0.
#[tokio::test]
async fn extensionless_hex_volumes_without_a_first_volume_report_what_was_seen() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30473);
    let name = "Hex Named Volumes Missing First";
    let files: Vec<(String, Vec<u8>)> = hex_names(5)
        .into_iter()
        .zip(rarlab_five_volume_set())
        .skip(1)
        .collect();
    let mut spec = rar_job_spec(name, &files);
    spec.password = Some(PASSWORD.to_string());
    insert_active_job(&mut pipeline, job_id, spec).await;

    complete_job_and_drive_extraction(&mut pipeline, job_id, &files).await;

    let error = match job_status_for_assert(&pipeline, job_id) {
        Some(JobStatus::Failed { error }) => error,
        other => panic!("expected the job to fail without a first volume, got {other:?}"),
    };
    assert!(
        error.contains("no first RAR volume (volume 0) was found"),
        "{error}"
    );
    assert!(
        error.contains("no PAR2 metadata is available for repair"),
        "{error}"
    );
    assert!(
        !error.contains("cannot be opened without volume 0"),
        "the set must not reach an extraction attempt: {error}"
    );
    for (filename, _) in &files {
        assert!(
            error.contains(filename.as_str()),
            "{filename} missing from: {error}"
        );
    }
    assert!(error.contains("header states volume 1"), "{error}");
}

#[tokio::test]
async fn extensionless_hex_volumes_bound_through_par2_group_into_one_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30472);
    let name = "Hex Named Volumes Par2";
    let hex = hex_names(5);
    // PAR2 describes each volume under a name that is itself obfuscated: a
    // second hex name, again with nothing to say which volume is which. The
    // binding is the only thing tying a posted file to a described one.
    let described: Vec<(String, Vec<u8>)> = hex_names(10)
        .split_off(5)
        .into_iter()
        .zip(rarlab_five_volume_set())
        .collect();
    let files: Vec<(String, Vec<u8>)> = hex
        .into_iter()
        .zip(described.iter().map(|(_, bytes)| bytes.clone()))
        .collect();
    let mut spec = rar_job_spec(name, &files);
    spec.password = Some(PASSWORD.to_string());
    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&described),
        &[],
    );

    for (index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, index as u32, filename, bytes).await;
        pipeline.retry_par2_authoritative_identity(job_id).await;
    }
    complete_job_and_drive_extraction(&mut pipeline, job_id, &[]).await;

    assert_completed_with_member(&pipeline, job_id, &complete_dir, name);
}
