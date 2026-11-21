// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::{Path, PathBuf};

use super::*;

// Everything of a spec a test compares, article lists included.
fn shape(spec: &JobSpec) -> String {
    let files: Vec<_> = spec
        .files
        .iter()
        .map(|file| {
            let segments: Vec<_> = file
                .segments
                .iter()
                .map(|segment| {
                    (
                        segment.ordinal,
                        segment.article_number,
                        segment.bytes,
                        segment.message_ids.clone(),
                    )
                })
                .collect();
            format!(
                "{} {:?} {:?} {:?} {:?}",
                file.filename, file.role, file.groups, file.posted_at_epoch, segments
            )
        })
        .collect();
    format!(
        "{} {:?} {} {:?} {:?} {:?}",
        spec.name, spec.password, spec.total_bytes, spec.category, spec.metadata, files
    )
}

fn file(subject: &str, date: &str, groups: &[&str], segments: &[(u32, u32, &str)]) -> String {
    let groups: String = groups
        .iter()
        .map(|group| format!("<group>{group}</group>"))
        .collect();
    let segments: String = segments
        .iter()
        .map(|(number, bytes, id)| {
            format!(r#"<segment bytes="{bytes}" number="{number}">{id}</segment>"#)
        })
        .collect();
    format!(
        r#"<file poster="p@example.invalid" date="{date}" subject="{subject}"><groups>{groups}</groups><segments>{segments}</segments></file>"#
    )
}

// NZBs of every shape the outline has to carry: several files, two posted
// under one name, a file with no usable article, files with no date, no
// subject, repeated parts, a title, meta passwords, and none of these.
fn documents() -> Vec<String> {
    let multi = [
        file(
            "Show - &quot;show.part01.rar&quot; yEnc (1/3)",
            "1700000000",
            &["alt.binaries.a", "alt.binaries.b"],
            &[
                (2, 700, "b@x"),
                (1, 700, "a@x"),
                (1, 700, "a2@x"),
                (3, 50, "c@x"),
            ],
        ),
        file(
            "Show - &quot;show.part02.rar&quot; yEnc (1/2)",
            "0",
            &["alt.binaries.a"],
            &[(1, 700, "d@x"), (2, 10, "e@x")],
        ),
        file("dropped (1/1)", "5", &[], &[(1, 0, "")]),
        file(
            "Show - &quot;show.par2&quot; yEnc (1/1)",
            "bad",
            &["alt.binaries.a"],
            &[(1, 40, "f@x")],
        ),
        file(
            "Show - &quot;show.part01.rar&quot; yEnc (1/1)",
            "1",
            &["alt.binaries.a"],
            &[(1, 9, "g@x")],
        ),
        r#"<file date="3"><segments><segment bytes="5" number="0">h@x</segment></segments></file>"#
            .to_string(),
    ]
    .concat();
    vec![
        format!(
            r#"<?xml version="1.0"?><nzb><head><meta type="title">Some Title</meta><meta type="password">  </meta><meta type="password">secret</meta><meta type="password">other</meta></head>{multi}</nzb>"#
        ),
        format!(r#"<?xml version="1.0"?><nzb>{multi}</nzb>"#),
        format!(
            r#"<?xml version="1.0"?><nzb><head><meta type="TITLE"></meta></head>{}</nzb>"#,
            file("plain.bin (1/1)", "9", &["g"], &[(1, 3, "only@x")])
        ),
    ]
}

fn paths() -> Vec<PathBuf> {
    vec![
        PathBuf::from("/nzb/Release.Name.nzb"),
        PathBuf::from("/nzb/Release.Name{{pathpw}}.nzb"),
        PathBuf::from("upload"),
    ]
}

fn metadatas() -> Vec<Vec<(String, String)>> {
    let mut renamed = Vec::new();
    crate::ingest::set_renamed_title(&mut renamed, "Renamed");
    vec![
        Vec::new(),
        vec![("source".to_string(), "rss".to_string())],
        renamed,
    ]
}

// A waiting job built from its stored outline is the job the whole parse
// builds, less its lists, holding the summaries those lists give; the lists
// read back stream to the same lists the whole parse holds; and the
// passwords read with them are the ones the whole parse gives.
#[test]
fn an_outline_builds_the_job_the_whole_parse_builds() {
    for document in documents() {
        let stored = crate::ingest::compress_nzb_bytes(document.as_bytes()).unwrap();
        for nzb_path in paths() {
            for metadata in metadatas() {
                let category = Some("tv".to_string());
                let nzb = crate::ingest::parse_persisted_nzb_bytes(&stored).unwrap();
                let whole =
                    crate::ingest::nzb_to_spec(&nzb, &nzb_path, category.clone(), metadata.clone());
                let mut lean = whole.clone();
                let summaries = lean.drop_article_lists();

                let read = read_stored_outline(&stored, &nzb_path).unwrap();
                assert!(read.lists.is_empty());
                assert_eq!(read.outline, NzbOutline::of(&nzb));
                let (from_outline, outline_summaries) =
                    read.outline
                        .spec(&nzb_path, category.clone(), metadata.clone());
                assert_eq!(shape(&from_outline), shape(&lean), "{document}");
                assert_eq!(outline_summaries, summaries);

                let mut lists = read_stored_lists(&stored, &nzb_path).unwrap();
                assert_eq!(
                    format!("{:?}", lists.passwords),
                    format!(
                        "{:?}",
                        crate::ingest::nzb_password_candidates(&nzb, &nzb_path, None)
                    )
                );
                let spec = lists.spec(&nzb_path, category.clone(), metadata.clone());
                assert_eq!(shape(&spec), shape(&whole), "{document}");
            }
        }
    }
}

// A stored NZB kept uncompressed reads the same.
#[test]
fn an_uncompressed_stored_nzb_reads_the_same() {
    let document = documents().remove(0);
    let path = Path::new("a.nzb");
    let compressed = crate::ingest::compress_nzb_bytes(document.as_bytes()).unwrap();
    assert_eq!(
        read_stored_outline(document.as_bytes(), path)
            .unwrap()
            .outline,
        read_stored_outline(&compressed, path).unwrap().outline
    );
}

// A stored NZB that does not decode, or holds no file, is unreadable.
#[test]
fn an_unreadable_stored_nzb_is_an_error() {
    let path = Path::new("a.nzb");
    let mut broken = crate::ingest::compress_nzb_bytes(documents()[0].as_bytes()).unwrap();
    broken.truncate(broken.len() / 2);
    assert!(matches!(
        read_stored_outline(&broken, path),
        Err(PersistedNzbError::Io(_))
    ));
    assert!(read_stored_outline(&[], path).is_err());
    assert!(read_stored_lists(&[1, 2, 3], path).is_err());
}
