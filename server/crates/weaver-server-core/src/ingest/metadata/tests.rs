use super::{ORIGINAL_TITLE_METADATA_KEY, append_original_title_metadata, original_release_title};

#[test]
fn appends_original_title_metadata_once() {
    let metadata = append_original_title_metadata(
        vec![("priority".to_string(), "HIGH".to_string())],
        Some("Dune.2021.1080p.BluRay.x264.nzb"),
        None,
    );

    assert_eq!(metadata.len(), 2);
    assert_eq!(
        metadata[1],
        (
            ORIGINAL_TITLE_METADATA_KEY.to_string(),
            "Dune.2021.1080p.BluRay.x264".to_string()
        )
    );
}

#[test]
fn original_title_prefers_metadata() {
    let title = original_release_title(
        "Dune",
        &[(
            ORIGINAL_TITLE_METADATA_KEY.to_string(),
            "Dune.2021.1080p.BluRay.x264".to_string(),
        )],
    );

    assert_eq!(title, "Dune.2021.1080p.BluRay.x264");
}
