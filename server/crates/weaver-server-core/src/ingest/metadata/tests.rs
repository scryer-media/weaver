// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{
    ORIGINAL_TITLE_METADATA_KEY, POSTED_FILE_NAMES_METADATA_KEY, PostedFileName,
    append_original_title_metadata, append_posted_file_names_metadata, original_release_title,
    posted_file_names,
};

#[test]
fn appends_original_title_metadata_once() {
    let metadata = append_original_title_metadata(
        vec![("priority".to_string(), "HIGH".to_string())],
        Some("Glass Harbor.2021.1080p.BluRay.x264.nzb"),
        None,
    );

    assert_eq!(metadata.len(), 2);
    assert_eq!(
        metadata[1],
        (
            ORIGINAL_TITLE_METADATA_KEY.to_string(),
            "Glass Harbor.2021.1080p.BluRay.x264".to_string()
        )
    );
}

#[test]
fn original_title_prefers_metadata() {
    let title = original_release_title(
        "Glass Harbor",
        &[(
            ORIGINAL_TITLE_METADATA_KEY.to_string(),
            "Glass Harbor.2021.1080p.BluRay.x264".to_string(),
        )],
    );

    assert_eq!(title, "Glass Harbor.2021.1080p.BluRay.x264");
}

#[test]
fn posted_file_names_record_only_renamed_files() {
    let metadata = append_posted_file_names_metadata(
        Vec::new(),
        [
            ("Glass Harbor - 07.mkv", "Glass Harbor - 07.mkv"),
            ("Glass Harbor: Part 2.mkv", "Glass Harbor_ Part 2.mkv"),
            ("  ", "unknown"),
        ],
    );

    assert_eq!(
        posted_file_names(&metadata),
        vec![PostedFileName {
            name: "Glass Harbor_ Part 2.mkv".to_string(),
            posted_name: "Glass Harbor: Part 2.mkv".to_string(),
        }]
    );
}

#[test]
fn posted_file_names_skip_the_record_when_nothing_was_renamed() {
    let metadata = append_posted_file_names_metadata(Vec::new(), [("a.mkv", "a.mkv")]);

    assert!(metadata.is_empty());
    assert!(posted_file_names(&metadata).is_empty());
}

#[test]
fn posted_file_names_keep_an_existing_record() {
    let first = append_posted_file_names_metadata(Vec::new(), [("a:b.mkv", "a_b.mkv")]);

    let rebuilt = append_posted_file_names_metadata(first.clone(), [("c:d.mkv", "c_d.mkv")]);

    assert_eq!(rebuilt, first);
}

#[test]
fn posted_file_names_stop_recording_past_the_byte_cap() {
    let posted = "p".repeat(1000);
    let names = (0..200)
        .map(|index| format!("{index}.mkv"))
        .collect::<Vec<_>>();

    let metadata = append_posted_file_names_metadata(
        Vec::new(),
        names.iter().map(|name| (posted.as_str(), name.as_str())),
    );

    let recorded = posted_file_names(&metadata);
    assert!(!recorded.is_empty());
    assert!(recorded.len() < names.len());
    let value = &metadata
        .iter()
        .find(|(key, _)| key == POSTED_FILE_NAMES_METADATA_KEY)
        .unwrap()
        .1;
    assert!(value.len() < 70 * 1024);
}

#[test]
fn posted_file_names_read_nothing_from_a_malformed_record() {
    let metadata = vec![(POSTED_FILE_NAMES_METADATA_KEY.to_string(), "{".to_string())];

    assert!(posted_file_names(&metadata).is_empty());
}
