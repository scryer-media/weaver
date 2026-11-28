// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A scene "zip with diz" release posted as one RAR: a single-volume outer
// RAR posted as `toner.part01.rar` under an obfuscated header name, holding
// an nfo, a diz and a zip; the zip holds an nfo, a diz and a single-volume
// RAR4 archive whose one member is compressed. PAR2 recovery volumes sit
// beside the outer RAR with no index. Every byte arrives; the member of the
// innermost RAR is delivered whole and nothing is withheld.

use std::io::{Cursor, Write};

use ::zip::write::SimpleFileOptions;
use ::zip::{CompressionMethod, ZipWriter};
use sha2::Digest;
use weaver_graph::Why;
use weaver_graph::recovery::RepairOutcome;

use super::harness::{Outcome, Post, Scenario, par2_volumes_sliced, run};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::lines::Lines;
use crate::pipeline::tests::direct_store::multi_member_store_set;

const FOLDER: &str = "lantern beacon";
const OBFUSCATED: &str = "KIdiFABqmVMSlXYyMl3bpV04bduBST4d.rar";

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/../weaver-server-core/tests/fixtures/extraction_profiles/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

// The size and SHA-256 the RAR 7.23 reader gives `member` of `archive`.
fn expected(archive: &str, member: &str) -> (u64, String) {
    let text = String::from_utf8(fixture("expected.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    let entry = &json["archives"][archive][member];
    (
        entry["size"].as_u64().unwrap(),
        entry["sha256"].as_str().unwrap().to_owned(),
    )
}

fn zip(entries: &[(&str, Vec<u8>)], method: CompressionMethod) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, bytes) in entries {
        writer
            .start_file(
                *name,
                SimpleFileOptions::default().compression_method(method),
            )
            .unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

// The release: `inner` named `tn60025.rar` in a zip of `method`, the zip in
// a single-volume stored RAR5 posted as `toner.part01.rar` under an
// obfuscated header name, beside seven PAR2 recovery volumes and no index.
fn release(inner: Vec<u8>, method: CompressionMethod) -> Scenario {
    release_framed(inner, method, 4096)
}

fn release_framed(inner: Vec<u8>, method: CompressionMethod, frame: usize) -> Scenario {
    let zipped = zip(
        &[
            ("tn60025.nfo", b"toner release notes\r\n".to_vec()),
            ("file_id.diz", b"toner 1/1\r\n".to_vec()),
            ("tn60025.rar", inner),
        ],
        method,
    );
    let outer = multi_member_store_set(
        &[
            ("toner.nfo", b"toner outer notes\r\n".to_vec()),
            ("file_id.diz", b"toner outer 1/1\r\n".to_vec()),
            ("tn60025a.zip", zipped),
        ],
        1,
    )
    .remove(0)
    .1;
    let name = "toner.part01.rar";
    let mut posts = vec![Post::plain(name, outer.clone())];
    let recovery = par2_volumes_sliced(
        "toner",
        &[(name, outer.as_slice())],
        4096.min(frame as u64),
        14,
        7,
    );
    posts.extend(recovery.into_iter().filter(|post| {
        !matches!(
            post.role,
            weaver_model::files::FileRole::Par2 { is_index: true, .. }
        )
    }));
    assert_eq!(posts.len(), 8, "the outer RAR and seven recovery volumes");
    Scenario::new(posts).framed(frame).header(0, OBFUSCATED)
}

async fn delivers_member(
    scenario: Scenario,
    member: &str,
    (size, sha256): (u64, String),
    case: &str,
) -> Outcome {
    let (lines, guard) = Lines::capture();
    let outcome = run(&scenario).await;
    drop(guard);
    let names: Vec<&String> = outcome.files.keys().collect();
    let whole = outcome.files.iter().any(|(name, bytes)| {
        name.starts_with(&format!("{FOLDER}/"))
            && name.ends_with(&format!("/{member}"))
            && bytes.len() as u64 == size
            && hex::encode(sha2::Sha256::digest(bytes)) == sha256
    });
    let delivery = &outcome.graph().delivery;
    assert!(
        outcome.completed() && whole && delivery.withheld.is_empty(),
        "{case}: {:?} {:?} files {names:?} withheld {:?} damaged {:?} unrepaired {:?} refused {:#?}",
        outcome.status,
        outcome.failure,
        delivery.withheld,
        delivery.damaged,
        delivery.unrepaired,
        lines.of("object refused"),
    );
    outcome
}

// The innermost RAR4 compressed by RAR 6.24 (`-ma4 -m3`), its one member
// `compressible.txt`, in a deflated zip and in a stored one.
#[tokio::test]
async fn a_compressed_rar4_in_a_zip_in_an_outer_rar_delivers_its_member() {
    for (case, method) in [
        ("deflated zip", CompressionMethod::Deflated),
        ("stored zip", CompressionMethod::Stored),
    ] {
        delivers_member(
            release(fixture("rar4_lz.rar"), method),
            "compressible.txt",
            expected("rar4_lz.rar", "compressible.txt"),
            case,
        )
        .await;
    }
}

// `tn60025.pdf` of 3,761,193 bytes under the directory entries
// `home/victorien/ws/printing/.dev/releases` and each one above it,
// compressed by RAR 6.24 (`rar a -ma4 -r tn60025.rar home`, `-m3 -md=4M`),
// as the release's innermost archive holds its book. RAR 7.23 tests it
// "All OK".
fn dirs_rar4() -> Vec<u8> {
    std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar4/tn60025_dirs.rar"
    ))
    .unwrap()
}

fn book() -> (u64, String) {
    (
        3_761_193,
        "089810910ce73cb837483f36e496ae354652579d40367408bfbc8d2dce258887".to_owned(),
    )
}

#[tokio::test]
async fn a_compressed_rar4_under_directory_entries_in_a_zip_in_an_outer_rar_delivers_its_book() {
    for (case, method) in [
        ("deflated zip", CompressionMethod::Deflated),
        ("stored zip", CompressionMethod::Stored),
    ] {
        delivers_member(release(dirs_rar4(), method), "tn60025.pdf", book(), case).await;
    }
}

// The same archive posted alone at the root.
#[tokio::test]
async fn a_compressed_rar4_under_directory_entries_at_the_root_delivers_its_book() {
    delivers_member(
        Scenario::new(vec![Post::plain("tn60025.rar", dirs_rar4())]).framed(65536),
        "tn60025.pdf",
        book(),
        "at the root",
    )
    .await;
}

// The archive alone in a stored zip, longer than the zip's tail. The zip
// streams the member's first bytes at once and holds the ones in its last
// 65,557 bytes until its directory arrives. The first bytes bind the RAR
// store, which refuses the compressed member in that same delivery: the
// engine that takes the member over gets those bytes from the store, not
// from the member's file, which never had them.
#[tokio::test]
async fn a_compressed_rar4_longer_than_a_stored_zip_tail_delivers_its_book() {
    delivers_member(
        Scenario::new(vec![Post::plain(
            "tn60025a.zip",
            zip(&[("tn60025.rar", dirs_rar4())], CompressionMethod::Stored),
        )])
        .framed(65536),
        "tn60025.pdf",
        book(),
        "stored zip at the root",
    )
    .await;
}

// What the PAR2 set ruled.
fn par2_ruling(outcome: &Outcome) -> Option<RepairOutcome> {
    outcome
        .graph()
        .rulings
        .iter()
        .find(|(kind, _)| kind == "par2")
        .and_then(|(_, ruling)| *ruling)
}

// The release with a stored zip and the outer RAR posted before or after
// its recovery volumes, in articles of `frame` bytes. The recovery set holds
// 40 blocks of 1,696 bytes.
// The outer RAR of the release with a stored zip: the nfo, the diz and the
// zip that holds `inner` as `tn60025.rar`.
fn outer_rar(inner: Vec<u8>) -> Vec<u8> {
    let zipped = zip(
        &[
            ("tn60025.nfo", b"toner release notes\r\n".to_vec()),
            ("file_id.diz", b"toner 1/1\r\n".to_vec()),
            ("tn60025.rar", inner),
        ],
        CompressionMethod::Stored,
    );
    multi_member_store_set(
        &[
            ("toner.nfo", b"toner outer notes\r\n".to_vec()),
            ("file_id.diz", b"toner outer 1/1\r\n".to_vec()),
            ("tn60025a.zip", zipped),
        ],
        1,
    )
    .remove(0)
    .1
}

fn release_ordered(inner: Vec<u8>, frame: usize, outer_first: bool) -> Scenario {
    let outer = outer_rar(inner);
    let name = "toner.part01.rar";
    let mut posts: Vec<Post> =
        par2_volumes_sliced("toner", &[(name, outer.as_slice())], 1696, 40, 7)
            .into_iter()
            .filter(|post| {
                !matches!(
                    post.role,
                    weaver_model::files::FileRole::Par2 { is_index: true, .. }
                )
            })
            .collect();
    let at = match outer_first {
        true => {
            posts.insert(0, Post::plain(name, outer));
            0
        }
        false => {
            posts.push(Post::plain(name, outer));
            posts.len() as u32 - 1
        }
    };
    Scenario::new(posts)
        .framed(frame)
        .header(at, OBFUSCATED)
        .leased()
}

// A change to an archive's bytes before it was packed.
type Breaks = fn(&mut Vec<u8>);

// The innermost RAR is broken as it was made, and every byte of the outer
// RAR arrives as posted. The recovery set proves those bytes whole, so it
// repairs nothing and rules nothing impossible, nothing is marked damaged,
// and the innermost RAR fails as an archive that cannot be read, its error
// in the log under its name. The nfo and diz beside it are delivered whole,
// so the job is delivered in part, and what it withheld names the archive.
#[tokio::test]
async fn a_broken_rar4_in_a_zip_in_an_outer_rar_with_whole_bytes_is_withheld_as_an_archive() {
    // Each breaks the book's compressed stream: its data CRC fails.
    let cases: [(&str, Breaks); 2] = [
        ("data", |bytes| {
            for byte in &mut bytes[300_000..300_064] {
                *byte ^= 0xff;
            }
        }),
        ("huffman tables", |bytes| {
            for byte in &mut bytes[104..112] {
                *byte ^= 0xff;
            }
        }),
    ];
    for (change, broken) in cases {
        for outer_first in [true, false] {
            let case = format!("{change}, outer first {outer_first}");
            let mut inner = dirs_rar4();
            broken(&mut inner);
            let scenario = release_ordered(inner, 65536, outer_first);
            let (lines, guard) = Lines::capture_at(
                "weaver_server_pipeline::pipeline::graph",
                tracing::Level::WARN,
            );
            let outcome = run(&scenario).await;
            drop(guard);
            let delivery = &outcome.graph().delivery;
            let failed = lines.of("extraction failed");
            assert!(
                outcome.status == Some(JobStatus::Partial)
                    && par2_ruling(&outcome) == Some(RepairOutcome::Clean)
                    && delivery.damaged.is_empty()
                    && delivery.unrepaired.is_empty()
                    && delivery.withheld == [("tn60025.rar".to_owned(), Why::InvalidArchive)]
                    && !delivery
                        .whole
                        .iter()
                        .any(|name| name.ends_with("tn60025.rar"))
                    && outcome
                        .graph()
                        .withheld_error()
                        .is_some_and(|reason| reason.contains("tn60025.rar")),
                "{case}: {:?} ruled {:?} withheld {:?} whole {:?} damaged {:?} unrepaired {:?} reason {:?}",
                outcome.status,
                par2_ruling(&outcome),
                delivery.withheld,
                delivery.whole,
                delivery.damaged,
                delivery.unrepaired,
                outcome.graph().withheld_error(),
            );
            // The furniture is delivered whole.
            for name in ["tn60025.nfo", "file_id.diz", "toner.nfo"] {
                assert!(
                    delivery.whole.iter().any(|whole| whole == name),
                    "{case}: {name} delivered whole"
                );
            }
            assert!(
                !failed.is_empty()
                    && failed.iter().all(|line| {
                        // Not solid: the store keeps the set, and its run
                        // makes the book.
                        line.contains("kind=rar_store")
                            && line.contains("archive=tn60025.rar")
                            && line.contains("error=")
                    }),
                "{case}: the run's error is logged under its archive: {failed:?}"
            );
        }
    }
}

// The control: the innermost RAR is whole as made, and an article of the
// outer RAR arrives damaged. The recovery set repairs it, and the book is
// delivered whole.
#[tokio::test]
async fn a_damaged_outer_rar_over_a_whole_rar4_is_repaired_and_delivers_its_book() {
    for outer_first in [true, false] {
        let scenario = release_ordered(dirs_rar4(), 32768, outer_first);
        let outer = match outer_first {
            true => 0,
            false => scenario.posts.len() as u32 - 1,
        };
        let outcome = delivers_member(
            scenario.damaged(outer, 4),
            "tn60025.pdf",
            book(),
            &format!("outer first {outer_first}"),
        )
        .await;
        let delivery = &outcome.graph().delivery;
        assert!(
            matches!(par2_ruling(&outcome), Some(RepairOutcome::Repaired { .. }))
                && delivery.damaged.is_empty()
                && delivery.unrepaired.is_empty(),
            "outer first {outer_first}: ruled {:?} damaged {:?} unrepaired {:?}",
            par2_ruling(&outcome),
            delivery.damaged,
            delivery.unrepaired,
        );
    }
}
