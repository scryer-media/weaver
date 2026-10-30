// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Partial completions: one row per refusal reason an archive can give, at the
// root and nested two deep (a stored RAR holding the archive under test), and
// the mixed jobs whose status the delivery decides. Each row reads what the
// graph delivered, what it lists as withheld, damaged and unrepaired, the
// `object refused` and `member delivered` lines, and the job's status:
// Complete when nothing is short, Partial when some file is whole, else
// Failed. `Why::TooDeep` has its rows in `caps.rs`.

use std::io::Cursor;

use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
use sha2::Digest;
use weaver_graph::Why;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, try_run};
use super::restart::run_restarting;
use super::streams::{damaged_in_the_middle, gzip, prose, tar};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::lines::Lines;
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_store_set, header_encrypted_store_set,
};
use crate::pipeline::tests::direct_store::{multi_member_store_set, single_member_store_set};
use crate::pipeline::tests::{
    TEST_RAR5_SIG, build_test_rar_compressed_file_header, build_test_rar_directory_header,
    build_test_rar_end_header, build_test_rar_file_header, build_test_rar_file_header_with_extra,
    build_test_rar_main_header, encode_test_rar_vint, test_rar_compression_info,
};

const SECRET: &str = "quill-ember-secret";
const FOLDER: &str = "lantern beacon";

// Seeded xorshift bytes: nothing compresses them and no format sniffs them.
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

fn body() -> Vec<u8> {
    noise(7 * ARTICLE + 13, 5)
}

fn ember() -> Vec<u8> {
    noise(3 * ARTICLE, 9)
}

fn quill() -> Vec<u8> {
    payload(2 * ARTICLE + 9, 81)
}

type Volumes = Vec<(String, Vec<u8>)>;

fn posts(volumes: &Volumes) -> Vec<Post> {
    volumes
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect()
}

// The volumes as the members of one stored RAR, `lantern.rar`.
fn nest(volumes: &Volumes) -> Vec<Post> {
    let named: Vec<(&str, Vec<u8>)> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let outer = multi_member_store_set(&named, 1).remove(0).1;
    vec![Post::plain("lantern.rar", outer)]
}

// A plain file posted beside the archive, so a job with it whole is Partial
// rather than Failed when the archive is short.
fn beside(mut posted: Vec<Post>) -> Vec<Post> {
    posted.push(Post::plain("quill.bin", quill()));
    posted
}

fn one(name: &str, bytes: Vec<u8>) -> Volumes {
    vec![(name.to_string(), bytes)]
}

fn store(volumes: usize) -> Volumes {
    multi_member_store_set(&[("beacon.bin", body())], volumes)
}

fn encrypted(volumes: usize, seed: u64) -> Volumes {
    encrypted_store_set(
        "beacon.bin",
        &noise(4 * ARTICLE + 7, seed),
        volumes,
        SECRET,
        Some(SECRET),
        false,
    )
}

fn header_encrypted(volumes: usize) -> Volumes {
    header_encrypted_store_set(
        "beacon.bin",
        &body(),
        volumes,
        SECRET,
        HeaderCheck::For(SECRET),
    )
}

fn rar5(headers: impl FnOnce(&mut Vec<u8>)) -> Volumes {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&TEST_RAR5_SIG);
    bytes.extend_from_slice(&build_test_rar_main_header(0, None));
    headers(&mut bytes);
    bytes.extend_from_slice(&build_test_rar_end_header(false));
    one("silver.horizon.rar", bytes)
}

fn stored_file(bytes: &mut Vec<u8>, name: &str, file: &[u8]) {
    bytes.extend_from_slice(&build_test_rar_file_header(
        name,
        0,
        file.len() as u64,
        file.len() as u64,
        Some(par2_rs::checksum::crc32(file)),
    ));
    bytes.extend_from_slice(file);
}

// `beacon.bin`, then `link.bin`: a file-system redirection (RAR5 extra
// record 5, type 1: a Unix symlink) to it.
fn symlink_rar() -> Volumes {
    let target = "beacon.bin";
    let mut record = Vec::new();
    record.extend_from_slice(&encode_test_rar_vint(5));
    record.extend_from_slice(&encode_test_rar_vint(1));
    record.extend_from_slice(&encode_test_rar_vint(0));
    record.extend_from_slice(&encode_test_rar_vint(target.len() as u64));
    record.extend_from_slice(target.as_bytes());
    let mut extra = encode_test_rar_vint(record.len() as u64);
    extra.extend_from_slice(&record);
    rar5(|bytes| {
        stored_file(bytes, "beacon.bin", &body());
        bytes.extend_from_slice(&build_test_rar_file_header_with_extra(
            "link.bin",
            0,
            0,
            0,
            Some(par2_rs::checksum::crc32(&[])),
            &extra,
        ));
    })
}

fn directory_rar() -> Volumes {
    rar5(|bytes| {
        bytes.extend_from_slice(&build_test_rar_directory_header("gallery", 0o755, 1));
        stored_file(bytes, "gallery/beacon.bin", &body());
    })
}

// One member whose header names RAR5 compression method 3 over bytes that
// are the file as it is: no decoder reads them back.
fn miscompressed_rar() -> Volumes {
    let file = body();
    rar5(|bytes| {
        bytes.extend_from_slice(&build_test_rar_compressed_file_header(
            "beacon.bin",
            0,
            file.len() as u64,
            file.len() as u64,
            Some(par2_rs::checksum::crc32(&file)),
            test_rar_compression_info(3, false),
        ));
        bytes.extend_from_slice(&file);
    })
}

// A tar whose one header holds no number in its size field, its checksum
// made right again.
fn unsized_tar() -> Vec<u8> {
    let mut bytes = tar(&[("beacon.bin", body())]);
    bytes[124..136].copy_from_slice(b"no number!!\0");
    bytes[148..156].copy_from_slice(b"        ");
    let sum: u32 = bytes[..512].iter().map(|byte| u32::from(*byte)).sum();
    bytes[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    bytes
}

fn sevenz(methods: Vec<EncoderMethod>, entries: Vec<(ArchiveEntry, Option<Vec<u8>>)>) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(methods.into_iter().map(EncoderConfiguration::new).collect());
    for (entry, bytes) in entries {
        writer
            .push_archive_entry(entry, bytes.map(Cursor::new))
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn entry(name: &str, bytes: Vec<u8>) -> (ArchiveEntry, Option<Vec<u8>>) {
    let mut entry = ArchiveEntry::new_file(name);
    entry.size = bytes.len() as u64;
    (entry, Some(bytes))
}

fn two_files(method: EncoderMethod) -> Vec<u8> {
    sevenz(
        vec![method],
        vec![entry("beacon.bin", body()), entry("ember.bin", ember())],
    )
}

// `beacon.bin`, then `link.bin`: a Unix symlink to it in the high attribute
// bits, its body the target.
fn symlink_7z() -> Vec<u8> {
    let mut link = ArchiveEntry::new_file("link.bin");
    link.has_windows_attributes = true;
    link.windows_attributes = 0x8000 | (0o120777 << 16);
    link.size = 10;
    sevenz(
        vec![EncoderMethod::COPY],
        vec![
            entry("beacon.bin", body()),
            (link, Some(b"beacon.bin".to_vec())),
        ],
    )
}

fn climbing_7z() -> Vec<u8> {
    sevenz(
        vec![EncoderMethod::COPY],
        vec![
            entry("../outside.bin", body()),
            entry("beacon.bin", ember()),
        ],
    )
}

// A stored 7z cut into three `lantern.7z.NNN` parts, the last never posted.
fn sevenz_without_its_last_part() -> Volumes {
    let archive = two_files(EncoderMethod::COPY);
    let chunk = archive.len().div_ceil(3);
    archive
        .chunks(chunk)
        .enumerate()
        .take(2)
        .map(|(index, bytes)| (format!("lantern.7z.{:03}", index + 1), bytes.to_vec()))
        .collect()
}

fn articles(bytes: &[u8]) -> u32 {
    bytes.len().div_ceil(ARTICLE) as u32
}

enum Status {
    Complete,
    Partial,
    Failed(&'static str),
}

// What the graph run comes to. `status` is None only on a row whose
// refusal is the defect: the status follows from the refusal the fix gives.
struct Graph {
    status: Option<Status>,
    kind: Option<&'static str>,
    // Every file under the complete directory, with its bytes when they are
    // known; None for a file delivered damaged.
    files: Vec<(String, Option<Vec<u8>>)>,
    whole: &'static [&'static str],
    damaged: &'static [&'static str],
    unrepaired: &'static [&'static str],
    withheld: &'static [(&'static str, Why)],
    // `kind=<object> member=<name> why=<reason>` of each `object refused`.
    refused: &'static [&'static str],
    // `name=<name> from=<path> depth=<depth> whole=<bool>` of each
    // `member delivered`.
    delivered: &'static [&'static str],
}

fn sorted<T: Ord + Clone>(items: &[T]) -> Vec<T> {
    let mut items = items.to_vec();
    items.sort();
    items
}

fn tails(lines: Vec<String>, from: &str) -> Vec<String> {
    let mut tails: Vec<String> = lines
        .iter()
        .map(|line| line[line.find(from).unwrap_or(0)..].to_string())
        .collect();
    tails.sort();
    tails
}

async fn run_expecting(scenario: &Scenario, expect: Graph) -> Outcome {
    let (lines, guard) = Lines::capture();
    let outcome = match try_run(scenario).await {
        Ok(outcome) => outcome,
        Err(stall) => panic!("graph stalled at {:?}: {}", stall.status, stall.state),
    };
    drop(guard);
    let delivery = &outcome.graph().delivery;
    assert_eq!(
        tails(lines.of("object refused"), "kind="),
        sorted(
            &expect
                .refused
                .iter()
                .map(|line| line.to_string())
                .collect::<Vec<_>>()
        ),
        "graph: the refusals; {delivery:?}"
    );
    let named = |withheld: &[(String, Why)]| -> Vec<String> {
        sorted(
            &withheld
                .iter()
                .map(|(name, why)| format!("{name} {why:?}"))
                .collect::<Vec<_>>(),
        )
    };
    let withheld: Vec<(String, Why)> = expect
        .withheld
        .iter()
        .map(|(name, why)| (name.to_string(), *why))
        .collect();
    assert_eq!(
        named(&delivery.withheld),
        named(&withheld),
        "graph: withheld; {delivery:?}"
    );
    assert_eq!(
        sorted(&delivery.whole),
        sorted(expect.whole),
        "graph: whole"
    );
    assert_eq!(
        sorted(&delivery.damaged),
        sorted(expect.damaged),
        "graph: damaged"
    );
    assert_eq!(
        sorted(&delivery.unrepaired),
        sorted(expect.unrepaired),
        "graph: unrepaired"
    );
    assert_eq!(
        tails(lines.of("member delivered"), "name="),
        sorted(
            &expect
                .delivered
                .iter()
                .map(|line| line.to_string())
                .collect::<Vec<_>>()
        ),
        "graph: the deliveries"
    );
    match expect.status {
        None => {}
        Some(Status::Complete) => assert_eq!(outcome.status, Some(JobStatus::Complete)),
        Some(Status::Partial) => assert_eq!(outcome.status, Some(JobStatus::Partial)),
        Some(Status::Failed(error)) => assert_eq!(
            outcome.status,
            Some(JobStatus::Failed {
                error: error.to_string()
            })
        ),
    }
    if expect.status.is_some() {
        assert_eq!(
            outcome
                .failure_kind
                .as_ref()
                .map(|kind| format!("{kind:?}"))
                .as_deref(),
            expect.kind,
            "graph: the failure kind"
        );
    }
    let names: Vec<String> = outcome.files.keys().cloned().collect();
    let wanted: Vec<String> = sorted(
        &expect
            .files
            .iter()
            .map(|(name, _)| format!("{FOLDER}/{name}"))
            .collect::<Vec<_>>(),
    );
    assert_eq!(names, wanted, "graph: the files delivered");
    for (name, bytes) in &expect.files {
        if let Some(bytes) = bytes {
            assert!(
                outcome.files[&format!("{FOLDER}/{name}")] == *bytes,
                "graph: the bytes of {name}"
            );
        }
    }
    outcome
}

fn file(name: &str, bytes: Option<Vec<u8>>) -> (String, Option<Vec<u8>>) {
    (name.to_string(), bytes)
}

fn raw(volumes: &Volumes) -> Vec<(String, Option<Vec<u8>>)> {
    volumes
        .iter()
        .map(|(name, bytes)| file(name, Some(bytes.clone())))
        .collect()
}

fn with_quill(mut files: Vec<(String, Option<Vec<u8>>)>) -> Vec<(String, Option<Vec<u8>>)> {
    files.push(file("quill.bin", Some(quill())));
    files
}

const QUILL: &str = "name=quill.bin from=quill.bin depth=0 whole=true";

// Encrypted, content only: the headers read, the bytes do not.

#[tokio::test]
async fn an_encrypted_rar_with_no_password_beside_a_plain_file_ends_partial_with_the_archive_withheld()
 {
    let volumes = encrypted(2, 5);
    let scenario = Scenario::new(beside(posts(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Password"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[
                ("silver.horizon.part01.rar", Why::Encrypted),
                ("silver.horizon.part02.rar", Why::Encrypted),
            ],
            refused: &[
                "kind=rar_store member=silver.horizon.part01.rar why=encrypted",
                "kind=rar_store member=silver.horizon.part02.rar why=encrypted",
            ],
            delivered: &[QUILL],
        },
    )
    .await;
}

#[tokio::test]
async fn an_encrypted_rar_inside_a_stored_rar_ends_partial_with_its_volume_withheld() {
    let volumes = encrypted(1, 5);
    let scenario = Scenario::new(beside(nest(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Password"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[("silver.horizon.part01.rar", Why::Encrypted)],
            refused: &["kind=rar_store member=silver.horizon.part01.rar why=encrypted"],
            delivered: &[QUILL],
        },
    )
    .await;
}

// The set's volumes, met one after another inside the outer member, belong
// to one object as they do at the root.
#[tokio::test]
async fn an_encrypted_two_volume_rar_inside_a_stored_rar_withholds_both_volumes_encrypted() {
    let volumes = encrypted(2, 5);
    let scenario = Scenario::new(beside(nest(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Password"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[
                ("silver.horizon.part01.rar", Why::Encrypted),
                ("silver.horizon.part02.rar", Why::Encrypted),
            ],
            refused: &[
                "kind=rar_store member=silver.horizon.part01.rar why=encrypted",
                "kind=rar_store member=silver.horizon.part02.rar why=encrypted",
            ],
            delivered: &[QUILL],
        },
    )
    .await;
}

// Encrypted headers: nothing names the members.

#[tokio::test]
async fn a_header_encrypted_rar_with_no_password_ends_partial_with_the_archive_withheld() {
    let volumes = header_encrypted(2);
    let scenario = Scenario::new(beside(posts(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Password"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[
                ("silver.horizon.part01.rar", Why::EncryptedHeader),
                ("silver.horizon.part02.rar", Why::EncryptedHeader),
            ],
            refused: &[
                "kind=rar_store member=silver.horizon.part01.rar why=encrypted_header",
                "kind=rar_store member=silver.horizon.part02.rar why=encrypted_header",
            ],
            delivered: &[QUILL],
        },
    )
    .await;
}

#[tokio::test]
async fn a_header_encrypted_rar_inside_a_stored_rar_ends_partial_with_its_volumes_withheld() {
    let volumes = header_encrypted(2);
    let scenario = Scenario::new(beside(nest(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Password"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[
                ("silver.horizon.part01.rar", Why::EncryptedHeader),
                ("silver.horizon.part02.rar", Why::EncryptedHeader),
            ],
            refused: &[
                "kind=rar_store member=silver.horizon.part01.rar why=encrypted_header",
                "kind=rar_store member=silver.horizon.part02.rar why=encrypted_header",
            ],
            delivered: &[QUILL],
        },
    )
    .await;
}

// Compressed: the store hands the archive to its engine.

#[tokio::test]
async fn an_lzma2_7z_is_refused_compressed_by_the_store_and_delivered_whole_by_the_engine() {
    let scenario = Scenario::new(beside(vec![Post::plain(
        "lantern.7z",
        two_files(EncoderMethod::LZMA2),
    )]));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![
                file("beacon.bin", Some(body())),
                file("ember.bin", Some(ember())),
            ]),
            whole: &["beacon.bin", "ember.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=sevenz_store member=lantern.7z why=compressed"],
            delivered: &[
                "name=beacon.bin from=beacon.bin depth=1 whole=true",
                "name=ember.bin from=ember.bin depth=1 whole=true",
                QUILL,
            ],
        },
    )
    .await;
}

#[tokio::test]
async fn an_lzma2_7z_inside_a_stored_rar_is_refused_compressed_and_delivered_whole_by_the_engine() {
    let scenario = Scenario::new(beside(nest(&one(
        "lantern.7z",
        two_files(EncoderMethod::LZMA2),
    ))));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![
                file("beacon.bin", Some(body())),
                file("ember.bin", Some(ember())),
            ]),
            whole: &["beacon.bin", "ember.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=sevenz_store member=lantern.7z why=compressed"],
            delivered: &[
                "name=beacon.bin from=beacon.bin depth=2 whole=true",
                "name=ember.bin from=ember.bin depth=2 whole=true",
                QUILL,
            ],
        },
    )
    .await;
}

// The store refuses the member compressed; the engine it goes to cannot
// decode it, and the volume is delivered damaged as it stands.
#[tokio::test]
async fn a_rar_whose_member_names_a_method_its_bytes_do_not_decode_is_delivered_damaged_unrepairable_after_the_store_refuses_it_compressed()
 {
    let volumes = miscompressed_rar();
    let scenario = Scenario::new(beside(posts(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(vec![file("silver.horizon.rar", None)]),
            whole: &["quill.bin"],
            damaged: &["silver.horizon.rar"],
            unrepaired: &[],
            withheld: &[],
            refused: &[
                "kind=rar_engine member=silver.horizon.rar why=unrepairable",
                "kind=rar_store member=silver.horizon.rar why=compressed",
            ],
            delivered: &[
                "name=silver.horizon.rar from=silver.horizon.rar depth=0 whole=false",
                QUILL,
            ],
        },
    )
    .await;
}

// One level down the same member is delivered the same way, its volume
// damaged at depth 1.
#[tokio::test]
async fn a_rar_inside_a_stored_rar_whose_member_names_a_method_its_bytes_do_not_decode_is_delivered_damaged_unrepairable()
 {
    let volumes = miscompressed_rar();
    let scenario = Scenario::new(beside(nest(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(vec![file("silver.horizon.rar", None)]),
            whole: &["quill.bin"],
            damaged: &["silver.horizon.rar"],
            unrepaired: &[],
            withheld: &[],
            refused: &[
                "kind=rar_engine member=silver.horizon.rar why=unrepairable",
                "kind=rar_store member=silver.horizon.rar why=compressed",
            ],
            delivered: &[
                "name=silver.horizon.rar from=silver.horizon.rar depth=1 whole=false",
                QUILL,
            ],
        },
    )
    .await;
}

#[tokio::test]
async fn a_rar_holding_a_symlink_delivers_its_file_and_refuses_the_link_not_a_file() {
    let scenario = Scenario::new(beside(posts(&symlink_rar())));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![file("beacon.bin", Some(body()))]),
            whole: &["beacon.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=rar_store member=silver.horizon.rar why=not_a_file"],
            delivered: &["name=beacon.bin from=beacon.bin depth=1 whole=true", QUILL],
        },
    )
    .await;
}

#[tokio::test]
async fn a_rar_holding_a_symlink_inside_a_stored_rar_delivers_its_file_and_refuses_the_link_not_a_file()
 {
    let scenario = Scenario::new(beside(nest(&symlink_rar())));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![file("beacon.bin", Some(body()))]),
            whole: &["beacon.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=rar_store member=silver.horizon.rar why=not_a_file"],
            delivered: &["name=beacon.bin from=beacon.bin depth=2 whole=true", QUILL],
        },
    )
    .await;
}

#[tokio::test]
async fn a_7z_holding_a_symlink_delivers_its_file_and_drops_the_link() {
    let scenario = Scenario::new(beside(vec![Post::plain("lantern.7z", symlink_7z())]));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![file("beacon.bin", Some(body()))]),
            whole: &["beacon.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=sevenz_store member=lantern.7z why=not_a_file"],
            delivered: &["name=beacon.bin from=beacon.bin depth=1 whole=true", QUILL],
        },
    )
    .await;
}

#[tokio::test]
async fn a_7z_holding_a_symlink_inside_a_stored_rar_delivers_its_file_and_drops_the_link() {
    let scenario = Scenario::new(beside(nest(&one("lantern.7z", symlink_7z()))));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![file("beacon.bin", Some(body()))]),
            whole: &["beacon.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=sevenz_store member=lantern.7z why=not_a_file"],
            delivered: &["name=beacon.bin from=beacon.bin depth=2 whole=true", QUILL],
        },
    )
    .await;
}

// A directory entry is placed, not refused: it is listed among the whole
// deliveries under its own name.
#[tokio::test]
async fn a_rar_whose_member_is_a_directory_entry_delivers_the_file_and_lists_the_directory_whole() {
    let scenario = Scenario::new(beside(posts(&directory_rar())));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![file("gallery/beacon.bin", Some(body()))]),
            whole: &["gallery/", "gallery/beacon.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &[],
            delivered: &[
                "name=gallery/ from=gallery/ depth=1 whole=true",
                "name=gallery/beacon.bin from=gallery/beacon.bin depth=1 whole=true",
                QUILL,
            ],
        },
    )
    .await;
}

#[tokio::test]
async fn a_rar_whose_member_is_a_directory_entry_inside_a_stored_rar_delivers_the_file_and_lists_the_directory_whole()
 {
    let scenario = Scenario::new(beside(nest(&directory_rar())));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![file("gallery/beacon.bin", Some(body()))]),
            whole: &["gallery/", "gallery/beacon.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &[],
            delivered: &[
                "name=gallery/ from=gallery/ depth=2 whole=true",
                "name=gallery/beacon.bin from=gallery/beacon.bin depth=2 whole=true",
                QUILL,
            ],
        },
    )
    .await;
}

// First bytes lost: what would name the members never came, so the volumes
// that did are delivered as they stand.

#[tokio::test]
async fn a_rar_whose_first_volume_never_arrives_delivers_the_tail_volumes_first_bytes_lost() {
    let mut volumes = store(3);
    volumes.remove(0);
    let scenario = Scenario::new(beside(posts(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &["silver.horizon.part02.rar", "silver.horizon.part03.rar"],
            unrepaired: &[],
            withheld: &[],
            refused: &[
                "kind=rar_store member=silver.horizon.part02.rar why=first_bytes_lost",
                "kind=rar_store member=silver.horizon.part03.rar why=first_bytes_lost",
            ],
            delivered: &[
                "name=silver.horizon.part02.rar from=silver.horizon.part02.rar depth=0 whole=false",
                "name=silver.horizon.part03.rar from=silver.horizon.part03.rar depth=0 whole=false",
                QUILL,
            ],
        },
    )
    .await;
}

#[tokio::test]
async fn a_rar_inside_a_stored_rar_whose_first_volume_never_arrives_delivers_the_tail_volumes_first_bytes_lost()
 {
    let mut volumes = store(3);
    volumes.remove(0);
    let scenario = Scenario::new(beside(nest(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &["silver.horizon.part02.rar", "silver.horizon.part03.rar"],
            unrepaired: &[],
            withheld: &[],
            refused: &[
                "kind=rar_store member=silver.horizon.part02.rar why=first_bytes_lost",
                "kind=rar_store member=silver.horizon.part03.rar why=first_bytes_lost",
            ],
            delivered: &[
                "name=silver.horizon.part02.rar from=silver.horizon.part02.rar depth=1 whole=false",
                "name=silver.horizon.part03.rar from=silver.horizon.part03.rar depth=1 whole=false",
                QUILL,
            ],
        },
    )
    .await;
}

// Articles of eight bytes: the first holds the signature alone, the second,
// lost, the main header. The store is bound and never learns the set.
#[tokio::test]
async fn a_rar_whose_first_volumes_main_header_article_never_arrives_delivers_its_volumes_first_bytes_lost()
 {
    let volumes = store(3);
    let scenario = Scenario::new(beside(posts(&volumes))).framed(8).lost(0, 1);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: vec![
                file("quill.bin", Some(quill())),
                file("silver.horizon.part01.rar", None),
                file("silver.horizon.part02.rar", Some(volumes[1].1.clone())),
                file("silver.horizon.part03.rar", Some(volumes[2].1.clone())),
            ],
            whole: &["quill.bin"],
            damaged: &[
                "silver.horizon.part01.rar",
                "silver.horizon.part02.rar",
                "silver.horizon.part03.rar",
            ],
            unrepaired: &[],
            withheld: &[],
            refused: &[
                "kind=rar_store member=silver.horizon.part01.rar why=first_bytes_lost",
                "kind=rar_store member=silver.horizon.part02.rar why=first_bytes_lost",
                "kind=rar_store member=silver.horizon.part03.rar why=first_bytes_lost",
            ],
            delivered: &[
                "name=silver.horizon.part01.rar from=silver.horizon.part01.rar depth=0 whole=false",
                "name=silver.horizon.part02.rar from=silver.horizon.part02.rar depth=0 whole=false",
                "name=silver.horizon.part03.rar from=silver.horizon.part03.rar depth=0 whole=false",
                QUILL,
            ],
        },
    )
    .await;
}

// Unrepairable: damage no recovery object covers.

#[tokio::test]
async fn a_rar_volume_whose_middle_article_never_arrives_is_delivered_damaged_unrepairable() {
    let volumes = store(1);
    let scenario = Scenario::new(beside(posts(&volumes))).lost(0, 3);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(vec![file("silver.horizon.part01.rar", None)]),
            whole: &["quill.bin"],
            damaged: &["silver.horizon.part01.rar"],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=rar_store member=silver.horizon.part01.rar why=unrepairable"],
            delivered: &[
                "name=silver.horizon.part01.rar from=silver.horizon.part01.rar depth=0 whole=false",
                QUILL,
            ],
        },
    )
    .await;
}

// The lost article lies in the inner volume's member data, as at the root;
// the refusal stops where the damage first had no cover, the inner store,
// and its volume is delivered damaged.
#[tokio::test]
async fn a_rar_inside_a_stored_rar_whose_middle_article_never_arrives_delivers_the_inner_volume_damaged_unrepairable()
 {
    let volumes = store(1);
    let scenario = Scenario::new(beside(nest(&volumes))).lost(0, 3);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(vec![file("silver.horizon.part01.rar", None)]),
            whole: &["quill.bin"],
            damaged: &["silver.horizon.part01.rar"],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=rar_store member=silver.horizon.part01.rar why=unrepairable"],
            delivered: &[
                "name=silver.horizon.part01.rar from=silver.horizon.part01.rar depth=1 whole=false",
                QUILL,
            ],
        },
    )
    .await;
}

// The lost article holds the end header alone: the member is whole and its
// checksum agrees.
#[tokio::test]
async fn a_rar_volume_whose_end_header_article_never_arrives_delivers_its_member_whole() {
    let volumes = store(1);
    let last = articles(&volumes[0].1) - 1;
    let scenario = Scenario::new(beside(posts(&volumes))).lost(0, last);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![file("beacon.bin", Some(body()))]),
            whole: &["beacon.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &[],
            delivered: &["name=beacon.bin from=beacon.bin depth=1 whole=true", QUILL],
        },
    )
    .await;
}

// Unsupported: the format is known, the shape is not.

#[tokio::test]
async fn a_tar_whose_entry_size_is_not_a_number_is_delivered_raw_unsupported() {
    let scenario = Scenario::new(beside(vec![Post::plain("lantern.tar", unsized_tar())]));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![file("lantern.tar", Some(unsized_tar()))]),
            whole: &["lantern.tar", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=tar member=lantern.tar why=unsupported"],
            delivered: &[
                "name=lantern.tar from=lantern.tar depth=0 whole=true",
                QUILL,
            ],
        },
    )
    .await;
}

#[tokio::test]
async fn a_tar_inside_a_stored_rar_whose_entry_size_is_not_a_number_is_delivered_raw_unsupported() {
    let scenario = Scenario::new(beside(nest(&one("lantern.tar", unsized_tar()))));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![file("lantern.tar", Some(unsized_tar()))]),
            whole: &["lantern.tar", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=tar member=lantern.tar why=unsupported"],
            delivered: &[
                "name=lantern.tar from=lantern.tar depth=1 whole=true",
                QUILL,
            ],
        },
    )
    .await;
}

// A file posted as a compressed stream whose contents came out damaged:
// the stream is refused for its own reason, the file is listed damaged, and
// with nothing usable the job fails.

fn damaged_gzip() -> Vec<u8> {
    damaged_in_the_middle(gzip(&prose()))
}

#[tokio::test]
async fn a_gzip_whose_contents_come_out_damaged_fails_with_the_posted_file_damaged() {
    let scenario = Scenario::new(vec![Post::plain("lantern.mkv.gz", damaged_gzip())]);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Failed(
                "no file was delivered whole; 1 short: lantern.mkv.gz",
            )),
            kind: Some("Unpack"),
            files: vec![],
            whole: &[],
            damaged: &["lantern.mkv.gz"],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=stream_gz member=lantern.mkv.gz why=unrepairable"],
            delivered: &[],
        },
    )
    .await;
}

// One level down the stored RAR opened, and the stream it produced is
// refused the same way: listed damaged, and the job fails.
#[tokio::test]
async fn a_gzip_inside_a_stored_rar_whose_contents_come_out_damaged_fails_with_the_stream_damaged()
{
    let scenario = Scenario::new(nest(&one("lantern.mkv.gz", damaged_gzip())));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Failed(
                "no file was delivered whole; 1 short: lantern.mkv.gz",
            )),
            kind: Some("Unpack"),
            files: vec![],
            whole: &[],
            damaged: &["lantern.mkv.gz"],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=stream_gz member=lantern.mkv.gz why=unrepairable"],
            delivered: &[],
        },
    )
    .await;
}

// The store refuses a path that climbs out of the namespace; the engine
// delivers the entry under its last component.
#[tokio::test]
async fn a_7z_whose_entry_path_climbs_out_is_refused_unsupported_and_delivered_under_its_last_name()
{
    let scenario = Scenario::new(beside(vec![Post::plain("lantern.7z", climbing_7z())]));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![
                file("outside.bin", Some(body())),
                file("beacon.bin", Some(ember())),
            ]),
            whole: &["beacon.bin", "outside.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=sevenz_store member=lantern.7z why=unsupported"],
            delivered: &[
                "name=beacon.bin from=beacon.bin depth=1 whole=true",
                "name=outside.bin from=../outside.bin depth=1 whole=true",
                QUILL,
            ],
        },
    )
    .await;
}

#[tokio::test]
async fn a_7z_inside_a_stored_rar_whose_entry_path_climbs_out_is_refused_unsupported_and_delivered_under_its_last_name()
 {
    let scenario = Scenario::new(beside(nest(&one("lantern.7z", climbing_7z()))));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: with_quill(vec![
                file("outside.bin", Some(body())),
                file("beacon.bin", Some(ember())),
            ]),
            whole: &["beacon.bin", "outside.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=sevenz_store member=lantern.7z why=unsupported"],
            delivered: &[
                "name=beacon.bin from=beacon.bin depth=2 whole=true",
                "name=outside.bin from=../outside.bin depth=2 whole=true",
                QUILL,
            ],
        },
    )
    .await;
}

// Unmapped: every volume is in, and the object never placed what it named.

#[tokio::test]
async fn a_rar_whose_last_volume_never_arrives_withholds_its_volumes_unmapped() {
    let mut volumes = store(3);
    volumes.pop();
    let scenario = Scenario::new(beside(posts(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[
                ("silver.horizon.part01.rar", Why::Unmapped),
                ("silver.horizon.part02.rar", Why::Unmapped),
            ],
            refused: &[
                "kind=rar_store member=silver.horizon.part01.rar why=unmapped",
                "kind=rar_store member=silver.horizon.part02.rar why=unmapped",
            ],
            delivered: &[QUILL],
        },
    )
    .await;
}

#[tokio::test]
async fn a_rar_inside_a_stored_rar_whose_last_volume_never_arrives_withholds_its_volumes_unmapped()
{
    let mut volumes = store(3);
    volumes.pop();
    let scenario = Scenario::new(beside(nest(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[
                ("silver.horizon.part01.rar", Why::Unmapped),
                ("silver.horizon.part02.rar", Why::Unmapped),
            ],
            refused: &[
                "kind=rar_store member=silver.horizon.part01.rar why=unmapped",
                "kind=rar_store member=silver.horizon.part02.rar why=unmapped",
            ],
            delivered: &[QUILL],
        },
    )
    .await;
}

#[tokio::test]
async fn a_7z_whose_last_part_never_arrives_withholds_its_parts_unmapped() {
    let volumes = sevenz_without_its_last_part();
    let scenario = Scenario::new(beside(posts(&volumes)));
    // The numbered pieces join into one file, and the 7z store beneath it
    // withholds that file; a piece is never a file of its own.
    let joined: Vec<u8> = volumes
        .iter()
        .flat_map(|(_, bytes)| bytes.clone())
        .collect();
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(vec![file("lantern.7z", Some(joined))]),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[("lantern.7z", Why::Unmapped)],
            refused: &["kind=sevenz_store member=lantern.7z why=unmapped"],
            delivered: &[QUILL],
        },
    )
    .await;
}

#[tokio::test]
async fn a_7z_inside_a_stored_rar_whose_last_part_never_arrives_withholds_its_parts_unmapped() {
    let volumes = sevenz_without_its_last_part();
    let scenario = Scenario::new(beside(nest(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[
                ("lantern.7z.001", Why::Unmapped),
                ("lantern.7z.002", Why::Unmapped),
            ],
            refused: &[
                "kind=sevenz_store member=lantern.7z.001 why=unmapped",
                "kind=sevenz_store member=lantern.7z.002 why=unmapped",
            ],
            delivered: &[QUILL],
        },
    )
    .await;
}

// Unrepaired: a set covers the member and has too few blocks to rebuild it.

fn under_one_block(name: &str, base: &str, bytes: Vec<u8>) -> Vec<Post> {
    let mut posted = vec![Post::plain(name, bytes.clone())];
    posted.extend(par2_set(base, &[(name, &bytes)], 1));
    posted
}

#[tokio::test]
async fn a_member_a_set_covers_but_cannot_rebuild_is_listed_unrepaired_and_the_job_fails() {
    let scenario = Scenario::new(under_one_block(
        "ember.bin",
        "ember",
        noise(6 * ARTICLE + 5, 33),
    ))
    .lost(0, 1)
    .lost(0, 3);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Failed(
                "PAR2 recovery failed for 1 set(s): ember.par2 (ember.bin): not repairable: 2 damaged slices, only 1 recovery blocks reachable",
            )),
            kind: Some("Repair"),
            files: vec![],
            whole: &[],
            damaged: &[],
            unrepaired: &["ember.bin"],
            withheld: &[],
            refused: &[],
            delivered: &[],
        },
    )
    .await;
}

#[tokio::test]
async fn a_stored_rar_a_set_covers_but_cannot_rebuild_is_listed_unrepaired_and_the_job_fails() {
    let outer = nest(&store(1)).remove(0).bytes;
    let scenario = Scenario::new(under_one_block("lantern.rar", "lantern", outer))
        .lost(0, 3)
        .lost(0, 5);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Failed(
                "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern.rar): not repairable: 2 damaged slices, only 1 recovery blocks reachable",
            )),
            kind: Some("Repair"),
            files: vec![],
            whole: &[],
            damaged: &[],
            unrepaired: &["silver.horizon.part01.rar"],
            withheld: &[],
            refused: &["kind=rar_store member=silver.horizon.part01.rar why=unrepairable"],
            delivered: &[],
        },
    )
    .await;
}

// Quiet: once nothing more will arrive and no repair will come, each
// object that cannot finish rules for itself, from its own state and what
// the tree records of its members. The server tells it and chooses no
// reason.

// A stored 7z whose article inside `beacon.bin` never arrives, with no
// PAR2: the store's member is left damaged and nothing repaired it, so the
// store refuses itself unrepairable. `ember.bin`, closed whole, stays
// delivered.
#[tokio::test]
async fn a_stored_7z_with_a_damaged_member_and_no_par2_refuses_itself_unrepairable() {
    let archive = two_files(EncoderMethod::COPY);
    let scenario = Scenario::new(beside(vec![Post::plain("lantern.7z", archive)])).lost(0, 3);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(vec![
                file("ember.bin", Some(ember())),
                file("lantern.7z", None),
            ]),
            whole: &["ember.bin", "quill.bin"],
            damaged: &["lantern.7z"],
            unrepaired: &[],
            withheld: &[],
            refused: &["kind=sevenz_store member=lantern.7z why=unrepairable"],
            delivered: &[
                "name=ember.bin from=ember.bin depth=1 whole=true",
                "name=lantern.7z from=lantern.7z depth=0 whole=false",
                QUILL,
            ],
        },
    )
    .await;
}

#[tokio::test]
async fn a_security_invalid_zip_is_never_a_whole_posted_file() {
    let mut archive = super::zip::archive(super::zip::stored());
    let locations: Vec<usize> = archive
        .windows(9)
        .enumerate()
        .filter_map(|(at, bytes)| (bytes == b"quill.bin").then_some(at))
        .collect();
    assert_eq!(locations.len(), 2, "local and central names");
    for at in locations {
        archive[at..at + 9].copy_from_slice(b"ember.bin");
    }
    for sibling in [false, true] {
        let mut posted = vec![Post::plain("duplicates.zip", archive.clone())];
        if sibling {
            posted.push(Post::plain("survivor.bin", quill()));
        }
        let outcome = super::harness::run(&Scenario::new(posted)).await;
        if sibling {
            assert_eq!(outcome.status, Some(JobStatus::Partial));
            assert_eq!(outcome.graph().delivery.whole, ["survivor.bin"]);
            assert_eq!(outcome.files[&format!("{FOLDER}/survivor.bin")], quill());
        } else {
            assert!(matches!(outcome.status, Some(JobStatus::Failed { .. })));
            assert!(outcome.graph().delivery.whole.is_empty());
        }
        assert_eq!(
            outcome.graph().delivery.withheld,
            [("duplicates.zip".into(), Why::InvalidArchive)]
        );
        assert!(
            !outcome
                .files
                .keys()
                .any(|name| name.ends_with("ember.bin") || name.ends_with("ember (1).bin"))
        );
        for (name, bytes) in outcome.files.iter().chain(outcome.retained_files.iter()) {
            if name.ends_with("duplicates.zip") {
                assert_eq!(bytes, &archive);
            }
        }
    }
}

// A stored zip whose last article, all directory and end record, never
// arrives. Nothing can bring the directory forward, so once the job is
// quiet the zip hears the loss and walks its local headers instead; both
// members are found whole, and with nothing short the zip refuses nothing.
#[tokio::test]
async fn a_stored_zip_whose_central_directory_article_never_arrives_rules_for_itself() {
    let archive = super::zip::archive(super::zip::stored());
    let last = articles(&archive) - 1;
    let scenario = Scenario::new(vec![Post::plain("lantern.zip", archive)]).lost(0, last);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Complete),
            kind: None,
            files: vec![
                file("ember.bin", Some(payload(5 * ARTICLE + 13, 51))),
                file("quill.bin", Some(payload(3 * ARTICLE - 5, 52))),
            ],
            whole: &["ember.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[],
            refused: &[],
            delivered: &[
                "name=ember.bin from=ember.bin depth=1 whole=true",
                "name=quill.bin from=quill.bin depth=1 whole=true",
            ],
        },
    )
    .await;
}

// A header-encrypted RAR that writes no password check: the store cannot
// tell a right password from a wrong one and hands the set to its engine,
// whose run asks for one. The job has none to give, and once it is quiet
// the engine, still asking, refuses itself for its shut headers.
#[tokio::test]
async fn a_header_encrypted_rar_with_no_check_and_no_password_refuses_itself_encrypted_header_at_quiet()
 {
    let volumes = header_encrypted_store_set("beacon.bin", &body(), 1, SECRET, HeaderCheck::Absent);
    let scenario = Scenario::new(beside(posts(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Password"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[("silver.horizon.part01.rar", Why::EncryptedHeader)],
            refused: &[
                "kind=rar_engine member=silver.horizon.part01.rar why=encrypted_header",
                "kind=rar_store member=silver.horizon.part01.rar why=encrypted_header",
            ],
            delivered: &[QUILL],
        },
    )
    .await;
}

// The first volume of a two-volume RAR, the second never posted: the store
// announces the member the first volume starts and never places the rest of
// it. Nothing posted was lost, so once the job is quiet the store refuses
// itself unmapped.
#[tokio::test]
async fn a_rar_that_announced_a_member_it_never_placed_refuses_itself_unmapped() {
    let mut volumes = store(2);
    volumes.pop();
    let scenario = Scenario::new(beside(posts(&volumes)));
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Unpack"),
            files: with_quill(raw(&volumes)),
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[("silver.horizon.part01.rar", Why::Unmapped)],
            refused: &["kind=rar_store member=silver.horizon.part01.rar why=unmapped"],
            delivered: &[QUILL],
        },
    )
    .await;
}

// Mixed jobs: the status follows from the delivery as a whole.

fn three_one_withheld() -> Scenario {
    let mut posted = vec![Post::plain("lantern.rar", encrypted(1, 5).remove(0).1)];
    posted.push(Post::plain("quill.bin", quill()));
    posted.push(Post::plain("ember.bin", ember()));
    Scenario::new(posted)
}

#[tokio::test]
async fn a_job_of_three_members_one_withheld_ends_partial_with_two_delivered() {
    let scenario = three_one_withheld();
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Password"),
            files: vec![
                file("ember.bin", Some(ember())),
                file("lantern.rar", Some(encrypted(1, 5).remove(0).1)),
                file("quill.bin", Some(quill())),
            ],
            whole: &["ember.bin", "quill.bin"],
            damaged: &[],
            unrepaired: &[],
            withheld: &[("lantern.rar", Why::Encrypted)],
            refused: &["kind=rar_store member=lantern.rar why=encrypted"],
            delivered: &["name=ember.bin from=ember.bin depth=0 whole=true", QUILL],
        },
    )
    .await;
}

#[tokio::test]
async fn a_job_of_three_members_all_withheld_fails() {
    let posted: Vec<Post> = [("lantern.rar", 5), ("quill.rar", 6), ("ember.rar", 7)]
        .into_iter()
        .map(|(name, seed)| Post::plain(name, encrypted(1, seed).remove(0).1))
        .collect();
    let scenario = Scenario::new(posted);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Failed(
                "WEAVER_PASSWORD_REQUIRED: lantern.rar, quill.rar, ember.rar needs a password and none was given",
            )),
            kind: Some("Password"),
            files: vec![],
            whole: &[],
            damaged: &[],
            unrepaired: &[],
            withheld: &[
                ("ember.rar", Why::Encrypted),
                ("lantern.rar", Why::Encrypted),
                ("quill.rar", Why::Encrypted),
            ],
            refused: &[
                "kind=rar_store member=ember.rar why=encrypted",
                "kind=rar_store member=lantern.rar why=encrypted",
                "kind=rar_store member=quill.rar why=encrypted",
            ],
            delivered: &[],
        },
    )
    .await;
}

// An encrypted archive withheld, a damaged one delivered as it stands, and
// one plain file: the plain file is the only whole one.
#[tokio::test]
async fn a_job_whose_only_whole_file_is_a_withheld_archives_sibling_ends_partial() {
    let locked = encrypted(1, 5).remove(0).1;
    let mut posted = vec![Post::plain("lantern.rar", locked.clone())];
    posted.extend(posts(&store(1)));
    posted.push(Post::plain("quill.bin", quill()));
    let scenario = Scenario::new(posted).lost(1, 3);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Password"),
            files: vec![
                file("lantern.rar", Some(locked)),
                file("quill.bin", Some(quill())),
                file("silver.horizon.part01.rar", None),
            ],
            whole: &["quill.bin"],
            damaged: &["silver.horizon.part01.rar"],
            unrepaired: &[],
            withheld: &[("lantern.rar", Why::Encrypted)],
            refused: &[
                "kind=rar_store member=lantern.rar why=encrypted",
                "kind=rar_store member=silver.horizon.part01.rar why=unrepairable",
            ],
            delivered: &[
                "name=silver.horizon.part01.rar from=silver.horizon.part01.rar depth=0 whole=false",
                QUILL,
            ],
        },
    )
    .await;
}

#[tokio::test]
async fn two_sets_one_short_of_blocks_end_partial_with_the_other_sets_files_whole() {
    let short = noise(6 * ARTICLE + 5, 33);
    let whole = noise(5 * ARTICLE + 3, 44);
    let mut posted = under_one_block("ember.bin", "ember", short.clone());
    posted.extend(under_one_block("quill.bin", "quill", whole.clone()));
    let quill_index = posted
        .iter()
        .position(|post| post.name == "quill.bin")
        .unwrap() as u32;
    let scenario = Scenario::new(posted)
        .lost(0, 1)
        .lost(0, 3)
        .lost(quill_index, 2);
    let graph = run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Partial),
            kind: Some("Repair"),
            files: vec![file("ember.bin", None), file("quill.bin", Some(whole))],
            whole: &["quill.bin"],
            damaged: &[],
            unrepaired: &["ember.bin"],
            withheld: &[],
            refused: &[],
            delivered: &["name=quill.bin from=quill.bin depth=0 whole=true"],
        },
    )
    .await;
    assert!(
        graph.files[&format!("{FOLDER}/ember.bin")] != short,
        "the short set's file is delivered as it stands, not rebuilt"
    );
}

// Restored after every article, the job comes to the same status and the
// same files as the run that never restarted.
#[tokio::test]
async fn a_partial_job_restarted_after_every_article_ends_partial_with_the_same_lists() {
    let scenario = three_one_withheld();
    let whole = match try_run(&scenario).await {
        Ok(outcome) => outcome,
        Err(stall) => panic!("graph stalled at {:?}: {}", stall.status, stall.state),
    };
    assert_eq!(whole.status, Some(JobStatus::Partial));
    let restarted = run_restarting(&scenario, &mut |_| {}).await;
    assert!(restarted.restarts > 0, "the run restarted");
    assert_eq!(restarted.status, whole.status, "the status");
    assert_eq!(
        restarted.files.keys().collect::<Vec<_>>(),
        whole.files.keys().collect::<Vec<_>>(),
        "the files"
    );
    assert!(restarted.files == whole.files, "the bytes");
}

// A stored RAR set posted uuencoded beside a PAR2 set, served last article
// first, an article of its first volume never arriving: the parts after the
// hole arrive, and wait, before the cursor reaches the hole. The cursor
// passes it, the parts behind it are placed one hole early, and the set
// rebuilds the volume, so the member comes out whole.
#[tokio::test]
async fn a_uuencoded_volume_whose_article_is_lost_behind_parts_already_in_is_repaired() {
    let body = noise(9 * ARTICLE + 20, 61);
    let volumes = single_member_store_set("ember.bin", &body, 2);
    let described: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = volumes
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_set("ember", &described, 8));
    let scenario = Scenario::new(posts).uuencoded().lost(0, 3).backwards();
    let outcome = try_run(&scenario).await.expect("the job settles");
    assert!(outcome.completed(), "{:?}", outcome.failure);
    assert!(
        outcome.files.get(&format!("{FOLDER}/ember.bin")) == Some(&body),
        "{:?}",
        outcome.files.keys()
    );
}

// A uuencoded file whose fourth article never arrives, with nothing to
// rebuild it. The loss is heard before the articles after it arrive, so they
// are placed where the order puts them, one hole early, and the file is
// damaged from the hole to its end: not whole, so the job fails.
#[tokio::test]
async fn a_uuencoded_file_whose_middle_article_never_arrives_is_damaged_to_its_end() {
    let scenario = Scenario::new(vec![Post::plain("ember.bin", noise(7 * ARTICLE + 20, 61))])
        .uuencoded()
        .lost(0, 3);
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Failed(
                "no file was delivered whole; 1 short: ember.bin",
            )),
            kind: Some("Unpack"),
            files: vec![],
            whole: &[],
            damaged: &["ember.bin"],
            unrepaired: &[],
            withheld: &[],
            refused: &[],
            delivered: &[],
        },
    )
    .await;
}

// The same file served last article first: the parts after the hole
// arrive, and wait, before the loss is heard; once the cursor reaches the
// hole it passes it, and they are placed as when the loss came first.
#[tokio::test]
async fn a_uuencoded_file_whose_middle_article_is_lost_behind_parts_already_in_is_damaged_to_its_end()
 {
    let scenario = Scenario::new(vec![Post::plain("ember.bin", noise(7 * ARTICLE + 20, 61))])
        .uuencoded()
        .lost(0, 3)
        .backwards();
    run_expecting(
        &scenario,
        Graph {
            status: Some(Status::Failed(
                "no file was delivered whole; 1 short: ember.bin",
            )),
            kind: Some("Unpack"),
            files: vec![],
            whole: &[],
            damaged: &["ember.bin"],
            unrepaired: &[],
            withheld: &[],
            refused: &[],
            delivered: &[],
        },
    )
    .await;
}

// The same file beside a PAR2 set: the hole is passed, the parts behind it
// are placed, and the set rebuilds the file whole.
#[tokio::test]
async fn a_uuencoded_file_whose_middle_article_is_lost_behind_parts_already_in_is_repaired() {
    let body = noise(7 * ARTICLE + 20, 61);
    let mut posts = vec![Post::plain("ember.bin", body.clone())];
    posts.extend(par2_set("ember", &[("ember.bin", body.as_slice())], 8));
    let scenario = Scenario::new(posts).uuencoded().lost(0, 3).backwards();
    let outcome = try_run(&scenario).await.expect("the job settles");
    assert!(outcome.completed(), "{:?}", outcome.failure);
    assert!(
        outcome.files.get(&format!("{FOLDER}/ember.bin")) == Some(&body),
        "{:?}",
        outcome.files.keys()
    );
}

// A compressed archive under a recovery set that describes the loss and
// cannot mend it (an index with no recovery blocks). The set rules the
// volume Impossible the moment the loss is heard, while the members the
// loss never touched are whole below it, or still to be produced by the
// engine: the job is judged by what is delivered when it ends, Partial,
// never failed early by the ruling on the root volume alone.

fn fixture_bytes(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/tests/fixtures/extraction_profiles/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(path).unwrap()
}

// `archive` posted as `compressed.rar`, described by a PAR2 index that
// carries no recovery block, posted first so the set is known when the
// loss is heard.
fn starved(archive: &str) -> Vec<Post> {
    let bytes = fixture_bytes(archive);
    let index = crate::pipeline::tests::build_test_par2_index_for_files(
        &[("compressed.rar", bytes.as_slice())],
        ARTICLE as u64,
    );
    vec![
        Post::plain("silver.horizon.par2", index),
        Post::plain("compressed.rar", bytes),
    ]
}

async fn run_partial_with_whole(scenario: &Scenario, whole: &[&str]) -> Outcome {
    let outcome = match try_run(scenario).await {
        Ok(outcome) => outcome,
        Err(stall) => panic!("graph stalled at {:?}: {}", stall.status, stall.state),
    };
    let delivery = &outcome.graph().delivery;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Partial),
        "{:?} {delivery:?}",
        outcome.failure
    );
    assert_eq!(
        outcome.failure_kind,
        Some(crate::history::HistoryFailureKind::Repair),
        "{:?}",
        outcome.failure
    );
    for name in whole {
        assert!(
            delivery.whole.iter().any(|delivered| delivered == name),
            "{name} whole; {delivery:?}"
        );
        assert!(
            outcome.files.contains_key(&format!("{FOLDER}/{name}")),
            "{name} delivered; {:?}",
            outcome.files.keys()
        );
    }
    outcome
}

// Three members in one volume of four articles, the last article lost: it
// holds the third member's bytes; the first two members' bytes lie in the
// articles that arrive.
#[tokio::test]
async fn a_mixed_rar_under_a_starved_par2_delivers_the_members_the_loss_never_touched() {
    let scenario = Scenario::new(starved("rar4_multifile_lz.rar"))
        .framed(66)
        .lost(1, 3);
    let outcome = run_partial_with_whole(&scenario, &["hello.txt", "second.txt"]).await;
    assert_eq!(
        hex::encode(sha2::Sha256::digest(
            &outcome.files[&format!("{FOLDER}/hello.txt")]
        )),
        "d9014c4624844aa5bac314773d6b689ad467fa4e1d1a50a1b8a99d5a95f72ff5"
    );
}

// A solid stream of three members in one volume of four articles, the last
// article lost: the engine decodes the first two members whole before the
// loss stops it.
#[tokio::test]
async fn a_solid_rar_under_a_starved_par2_delivers_the_members_before_the_loss() {
    let scenario = Scenario::new(starved("rar5_solid_small.rar"))
        .framed(116)
        .lost(1, 3);
    run_partial_with_whole(&scenario, &["part0.bin", "part1.bin"]).await;
}

// The same with the headers encrypted: the engine opens the stream with
// the job's password and the loss is heard before it has decoded a member.
#[tokio::test]
async fn a_header_encrypted_solid_rar_under_a_starved_par2_delivers_the_members_before_the_loss() {
    let scenario = Scenario::new(starved("rar5_solid_headers_small.rar"))
        .framed(200)
        .lost(1, 3)
        .password("moonlit-harbour");
    run_partial_with_whole(&scenario, &["part0.bin"]).await;
}
