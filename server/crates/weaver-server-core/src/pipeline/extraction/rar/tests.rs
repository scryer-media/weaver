use super::*;

fn sample_manifest(temp_path: &std::path::Path) -> Vec<crate::ExtractionChunk> {
    vec![
        crate::ExtractionChunk {
            member_name: "movie.mkv".to_string(),
            volume_index: 0,
            bytes_written: 10,
            temp_path: temp_path.to_string_lossy().to_string(),
            start_offset: 0,
            end_offset: 10,
            verified: true,
            appended: false,
        },
        crate::ExtractionChunk {
            member_name: "movie.mkv".to_string(),
            volume_index: 1,
            bytes_written: 15,
            temp_path: temp_path.to_string_lossy().to_string(),
            start_offset: 10,
            end_offset: 25,
            verified: true,
            appended: false,
        },
    ]
}

#[test]
fn validate_member_extraction_checkpoint_accepts_contiguous_manifest() {
    let temp_dir = tempfile::tempdir().unwrap();
    let partial_path = temp_dir.path().join("movie.mkv.partial");
    std::fs::write(&partial_path, vec![0u8; 25]).unwrap();

    let checkpoint = Pipeline::validate_member_extraction_checkpoint(
        &sample_manifest(&partial_path),
        &partial_path,
        0,
        2,
    )
    .unwrap()
    .unwrap();

    assert_eq!(checkpoint.next_offset, 25);
    assert_eq!(checkpoint.completed_volumes, HashSet::from([0u32, 1u32]));
}

#[test]
fn validate_member_extraction_checkpoint_rejects_partial_size_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let partial_path = temp_dir.path().join("movie.mkv.partial");
    std::fs::write(&partial_path, vec![0u8; 20]).unwrap();

    let error = Pipeline::validate_member_extraction_checkpoint(
        &sample_manifest(&partial_path),
        &partial_path,
        0,
        2,
    )
    .unwrap_err();

    assert!(error.contains("manifest ends at 25"));
}

#[test]
fn validate_member_extraction_manifest_allows_finalize_reconcile_without_partial_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let partial_path = temp_dir.path().join("movie.mkv.partial");

    let manifest =
        Pipeline::validate_member_extraction_manifest(&sample_manifest(&partial_path), 0, 2)
            .unwrap()
            .unwrap();

    assert_eq!(manifest.next_offset, 25);
    assert_eq!(manifest.completed_volumes, HashSet::from([0u32, 1u32]));
}
