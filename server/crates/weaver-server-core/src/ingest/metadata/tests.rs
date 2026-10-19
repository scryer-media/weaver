// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{
    FULL_FILE_NAMES_MAX_BYTES, FULL_FILE_NAMES_METADATA_KEY, FileRename, FullFileName,
    ORIGINAL_TITLE_METADATA_KEY, append_full_file_names_metadata, append_original_title_metadata,
    full_file_names, full_file_names_record, original_release_title, record_file_renames,
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
fn full_file_names_record_only_renamed_files() {
    let metadata = append_full_file_names_metadata(
        Vec::new(),
        [
            ("Glass Harbor - 07.mkv", "Glass Harbor - 07.mkv"),
            ("Glass Harbor: Part 2.mkv", "Glass Harbor_ Part 2.mkv"),
            ("  ", "unknown"),
        ],
    );

    assert_eq!(
        full_file_names(&metadata),
        vec![FullFileName {
            name: "Glass Harbor_ Part 2.mkv".to_string(),
            full_name: "Glass Harbor: Part 2.mkv".to_string(),
        }]
    );
}

#[test]
fn full_file_names_skip_the_record_when_nothing_was_renamed() {
    let metadata = append_full_file_names_metadata(Vec::new(), [("a.mkv", "a.mkv")]);

    assert!(metadata.is_empty());
    assert!(full_file_names(&metadata).is_empty());
}

#[test]
fn full_file_names_keep_an_existing_record() {
    let first = append_full_file_names_metadata(Vec::new(), [("a:b.mkv", "a_b.mkv")]);

    let rebuilt = append_full_file_names_metadata(first.clone(), [("c:d.mkv", "c_d.mkv")]);

    assert_eq!(rebuilt, first);
}

#[test]
fn a_rename_replaces_the_posted_name_with_the_full_name_it_gave() {
    let mut metadata = append_full_file_names_metadata(
        Vec::new(),
        [("Glass Harbor: Part 2.bin", "Glass Harbor_ Part 2.bin")],
    );
    let full_name = format!("{}.mkv", "Glass Harbor ".repeat(30));
    let disk_name = format!("{}.mkv", "Glass Harbor ".repeat(15));

    let changed = record_file_renames(
        &mut metadata,
        &[FileRename {
            from: "Glass Harbor_ Part 2.bin".to_string(),
            to: disk_name.clone(),
            full_name: full_name.clone(),
        }],
    );

    assert!(changed);
    assert_eq!(
        full_file_names(&metadata),
        vec![FullFileName {
            name: disk_name,
            full_name,
        }]
    );
}

#[test]
fn a_rename_to_the_full_name_clears_the_entry() {
    let mut metadata = append_full_file_names_metadata(Vec::new(), [("a:b.bin", "a_b.bin")]);

    let changed = record_file_renames(
        &mut metadata,
        &[FileRename {
            from: "a_b.bin".to_string(),
            to: "Glass Harbor.mkv".to_string(),
            full_name: "Glass Harbor.mkv".to_string(),
        }],
    );

    assert!(changed);
    assert!(full_file_names_record(&metadata).is_none());
}

#[test]
fn a_rename_of_a_file_the_record_does_not_hold_leaves_it_alone() {
    let mut metadata = append_full_file_names_metadata(Vec::new(), [("a:b.mkv", "a_b.mkv")]);
    let before = metadata.clone();

    let changed = record_file_renames(
        &mut metadata,
        &[FileRename {
            from: "c.bin".to_string(),
            to: "c.mkv".to_string(),
            full_name: "c.mkv".to_string(),
        }],
    );

    assert!(!changed);
    assert_eq!(metadata, before);
}

#[test]
fn full_file_names_keep_the_stored_record_under_the_byte_cap() {
    // Every quote and backslash doubles in the stored JSON, so the cap must
    // count what is stored rather than the names themselves.
    let full_name = "\\\"".repeat(400);
    let names = (0..200)
        .map(|index| format!("{index}.mkv"))
        .collect::<Vec<_>>();

    let metadata = append_full_file_names_metadata(
        Vec::new(),
        names.iter().map(|name| (full_name.as_str(), name.as_str())),
    );

    let recorded = full_file_names(&metadata);
    assert!(!recorded.is_empty());
    assert!(recorded.len() < names.len());
    let stored = full_file_names_record(&metadata).unwrap();
    assert!(stored.len() <= FULL_FILE_NAMES_MAX_BYTES);
    assert!(recorded.iter().all(|entry| entry.full_name == full_name));
}

#[test]
fn full_file_names_read_nothing_from_a_malformed_record() {
    let metadata = vec![(FULL_FILE_NAMES_METADATA_KEY.to_string(), "{".to_string())];

    assert!(full_file_names(&metadata).is_empty());
}
