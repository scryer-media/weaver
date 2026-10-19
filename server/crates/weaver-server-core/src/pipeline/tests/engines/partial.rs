// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Partial completions: one row per refusal reason an archive can give, at the
// root and nested two deep (a stored RAR holding the archive under test), and
// the mixed jobs whose status the delivery decides. Each row reads what the
// graph delivered, what it lists as withheld, damaged and unrepaired, the
// `object refused` and `member delivered` lines, and the job's status:
// Complete when nothing is short, Partial when some file is whole, else
// Failed. What classic does with the same job is asserted beside it, so a
// divergence is visible. `Why::TooDeep` has its rows in `caps.rs`.

use std::io::Cursor;

use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
use weaver_graph::Why;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, try_run};
use super::restart::run_restarting;
use super::streams::tar;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::Engine;
use crate::pipeline::graph::lines::Lines;
use crate::pipeline::tests::direct_store::multi_member_store_set;
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_store_set, header_encrypted_store_set,
};
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

async fn under_graph(scenario: &Scenario, expect: Graph) -> Outcome {
    let (lines, guard) = Lines::capture();
    let outcome = match try_run(scenario, Engine::Graph).await {
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

enum Classic {
    // The job stops moving at this status.
    Stalls(JobStatus),
    // The job fails with an error that starts so.
    Fails(&'static str),
    // The job completes with exactly these files.
    Completes(&'static [&'static str]),
}

async fn under_classic(scenario: &Scenario, expect: Classic) {
    let result = try_run(scenario, Engine::Classic).await;
    match (expect, result) {
        (Classic::Stalls(status), Err(stall)) => {
            assert_eq!(stall.status, Some(status), "classic: {}", stall.state)
        }
        (Classic::Fails(prefix), Ok(outcome)) => {
            let error = match &outcome.status {
                Some(JobStatus::Failed { error }) => error.clone(),
                other => panic!("classic: {other:?}, not a failure"),
            };
            assert!(error.starts_with(prefix), "classic: {error}");
        }
        (Classic::Completes(files), Ok(outcome)) => {
            assert_eq!(
                outcome.status,
                Some(JobStatus::Complete),
                "classic: {:?}",
                outcome.failure
            );
            let wanted: Vec<String> = sorted(
                &files
                    .iter()
                    .map(|name| format!("{FOLDER}/{name}"))
                    .collect::<Vec<_>>(),
            );
            assert_eq!(
                outcome.files.keys().cloned().collect::<Vec<_>>(),
                wanted,
                "classic"
            );
        }
        (_, Err(stall)) => panic!("classic stalled at {:?}: {}", stall.status, stall.state),
        (_, Ok(outcome)) => panic!("classic: {:?} {:?}", outcome.status, outcome.files.keys()),
    }
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
async fn an_encrypted_rar_with_no_password_beside_a_plain_file_ends_partial_with_the_archive_withheld_under_graph_and_stalls_downloading_under_classic()
 {
    let volumes = encrypted(2, 5);
    let scenario = Scenario::new(beside(posts(&volumes)));
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Downloading)).await;
}

#[tokio::test]
async fn an_encrypted_rar_inside_a_stored_rar_ends_partial_with_its_volume_withheld_under_graph_and_stalls_extracting_under_classic()
 {
    let volumes = encrypted(1, 5);
    let scenario = Scenario::new(beside(nest(&volumes)));
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Extracting)).await;
}

// The set's volumes, met one after another inside the outer member, belong
// to one object as they do at the root.
#[tokio::test]
async fn an_encrypted_two_volume_rar_inside_a_stored_rar_withholds_both_volumes_encrypted_under_graph_and_stalls_extracting_under_classic()
 {
    let volumes = encrypted(2, 5);
    let scenario = Scenario::new(beside(nest(&volumes)));
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Extracting)).await;
}

// Encrypted headers: nothing names the members.

#[tokio::test]
async fn a_header_encrypted_rar_with_no_password_ends_partial_with_the_archive_withheld_under_graph_and_fails_on_the_password_under_classic()
 {
    let volumes = header_encrypted(2);
    let scenario = Scenario::new(beside(posts(&volumes)));
    under_graph(
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
    under_classic(&scenario, Classic::Fails("WEAVER_PASSWORD_REQUIRED")).await;
}

#[tokio::test]
async fn a_header_encrypted_rar_inside_a_stored_rar_ends_partial_with_its_volumes_withheld_under_graph_and_fails_to_start_under_classic()
 {
    let volumes = header_encrypted(2);
    let scenario = Scenario::new(beside(nest(&volumes)));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("nested archive extraction could not be started"),
    )
    .await;
}

// Compressed: the store hands the archive to its engine.

#[tokio::test]
async fn an_lzma2_7z_is_refused_compressed_by_the_store_and_delivered_whole_by_the_engine_under_both_engines()
 {
    let scenario = Scenario::new(beside(vec![Post::plain(
        "lantern.7z",
        two_files(EncoderMethod::LZMA2),
    )]));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Completes(&["beacon.bin", "ember.bin", "quill.bin"]),
    )
    .await;
}

#[tokio::test]
async fn an_lzma2_7z_inside_a_stored_rar_is_refused_compressed_and_delivered_whole_by_the_engine_under_graph_under_both_engines()
 {
    let scenario = Scenario::new(beside(nest(&one(
        "lantern.7z",
        two_files(EncoderMethod::LZMA2),
    ))));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Completes(&["beacon.bin", "ember.bin", "quill.bin"]),
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
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Downloading)).await;
}

// One level down the same member is delivered the same way, its volume
// damaged at depth 1.
#[tokio::test]
async fn a_rar_inside_a_stored_rar_whose_member_names_a_method_its_bytes_do_not_decode_is_delivered_damaged_unrepairable()
 {
    let volumes = miscompressed_rar();
    let scenario = Scenario::new(beside(nest(&volumes)));
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Extracting)).await;
}

#[tokio::test]
async fn a_rar_holding_a_symlink_delivers_its_file_and_refuses_the_link_not_a_file_under_graph_and_stalls_downloading_under_classic()
 {
    let scenario = Scenario::new(beside(posts(&symlink_rar())));
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Downloading)).await;
}

#[tokio::test]
async fn a_rar_holding_a_symlink_inside_a_stored_rar_delivers_its_file_and_refuses_the_link_not_a_file_under_graph_and_stalls_extracting_under_classic()
 {
    let scenario = Scenario::new(beside(nest(&symlink_rar())));
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Extracting)).await;
}

#[tokio::test]
async fn a_7z_holding_a_symlink_delivers_its_file_and_drops_the_link_under_graph_and_writes_the_link_as_a_file_under_classic()
 {
    let scenario = Scenario::new(beside(vec![Post::plain("lantern.7z", symlink_7z())]));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Completes(&["beacon.bin", "link.bin", "quill.bin"]),
    )
    .await;
}

#[tokio::test]
async fn a_7z_holding_a_symlink_inside_a_stored_rar_delivers_its_file_and_drops_the_link_under_graph_and_writes_the_link_as_a_file_under_classic()
 {
    let scenario = Scenario::new(beside(nest(&one("lantern.7z", symlink_7z()))));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Completes(&["beacon.bin", "link.bin", "quill.bin"]),
    )
    .await;
}

// A directory entry is placed, not refused: it is listed among the whole
// deliveries under its own name.
#[tokio::test]
async fn a_rar_whose_member_is_a_directory_entry_delivers_the_file_and_lists_the_directory_whole_under_graph_and_stalls_downloading_under_classic()
 {
    let scenario = Scenario::new(beside(posts(&directory_rar())));
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Downloading)).await;
}

#[tokio::test]
async fn a_rar_whose_member_is_a_directory_entry_inside_a_stored_rar_delivers_the_file_and_lists_the_directory_whole_under_graph_and_stalls_extracting_under_classic()
 {
    let scenario = Scenario::new(beside(nest(&directory_rar())));
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Extracting)).await;
}

// First bytes lost: what would name the members never came, so the volumes
// that did are delivered as they stand.

#[tokio::test]
async fn a_rar_whose_first_volume_never_arrives_delivers_the_tail_volumes_first_bytes_lost() {
    let mut volumes = store(3);
    volumes.remove(0);
    let scenario = Scenario::new(beside(posts(&volumes)));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("no first RAR volume (volume 0) was found"),
    )
    .await;
}

#[tokio::test]
async fn a_rar_inside_a_stored_rar_whose_first_volume_never_arrives_delivers_the_tail_volumes_first_bytes_lost()
 {
    let mut volumes = store(3);
    volumes.remove(0);
    let scenario = Scenario::new(beside(nest(&volumes)));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("no first RAR volume (volume 0) was found"),
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
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("archive file silver.horizon.part01.rar is incomplete"),
    )
    .await;
}

// Unrepairable: damage no recovery object covers.

#[tokio::test]
async fn a_rar_volume_whose_middle_article_never_arrives_is_delivered_damaged_unrepairable_under_graph_and_fails_incomplete_under_classic()
 {
    let volumes = store(1);
    let scenario = Scenario::new(beside(posts(&volumes))).lost(0, 3);
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("archive file silver.horizon.part01.rar is incomplete"),
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
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("archive file lantern.rar is incomplete"),
    )
    .await;
}

// The lost article holds the end header alone: the member is whole and its
// checksum agrees.
#[tokio::test]
async fn a_rar_volume_whose_end_header_article_never_arrives_delivers_its_member_whole_under_graph_and_fails_incomplete_under_classic()
 {
    let volumes = store(1);
    let last = articles(&volumes[0].1) - 1;
    let scenario = Scenario::new(beside(posts(&volumes))).lost(0, last);
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("archive file silver.horizon.part01.rar is incomplete"),
    )
    .await;
}

// Unsupported: the format is known, the shape is not.

#[tokio::test]
async fn a_tar_whose_entry_size_is_not_a_number_is_delivered_raw_unsupported_under_graph_and_fails_under_classic()
 {
    let scenario = Scenario::new(beside(vec![Post::plain("lantern.tar", unsized_tar())]));
    under_graph(
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
    under_classic(&scenario, Classic::Fails("failed to read tar entry")).await;
}

#[tokio::test]
async fn a_tar_inside_a_stored_rar_whose_entry_size_is_not_a_number_is_delivered_raw_unsupported_under_graph_and_fails_under_classic()
 {
    let scenario = Scenario::new(beside(nest(&one("lantern.tar", unsized_tar()))));
    under_graph(
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
    under_classic(&scenario, Classic::Fails("failed to read tar entry")).await;
}

// The store refuses a path that climbs out of the namespace; the engine
// delivers the entry under its last component.
#[tokio::test]
async fn a_7z_whose_entry_path_climbs_out_is_refused_unsupported_and_delivered_under_its_last_name_under_graph_and_rejected_unsafe_under_classic()
 {
    let scenario = Scenario::new(beside(vec![Post::plain("lantern.7z", climbing_7z())]));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("WEAVER_EXTRACTION_REJECTED[unsafe_path]"),
    )
    .await;
}

#[tokio::test]
async fn a_7z_inside_a_stored_rar_whose_entry_path_climbs_out_is_refused_unsupported_and_delivered_under_its_last_name_under_graph_and_rejected_unsafe_under_classic()
 {
    let scenario = Scenario::new(beside(nest(&one("lantern.7z", climbing_7z()))));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("WEAVER_EXTRACTION_REJECTED[unsafe_path]"),
    )
    .await;
}

// Unmapped: every volume is in, and the object never placed what it named.

#[tokio::test]
async fn a_rar_whose_last_volume_never_arrives_withholds_its_volumes_unmapped_under_graph_and_stalls_downloading_under_classic()
 {
    let mut volumes = store(3);
    volumes.pop();
    let scenario = Scenario::new(beside(posts(&volumes)));
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Downloading)).await;
}

#[tokio::test]
async fn a_rar_inside_a_stored_rar_whose_last_volume_never_arrives_withholds_its_volumes_unmapped_under_graph_and_stalls_extracting_under_classic()
 {
    let mut volumes = store(3);
    volumes.pop();
    let scenario = Scenario::new(beside(nest(&volumes)));
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Extracting)).await;
}

#[tokio::test]
async fn a_7z_whose_last_part_never_arrives_withholds_its_parts_unmapped_under_graph_and_fails_missing_a_part_under_classic()
 {
    let volumes = sevenz_without_its_last_part();
    let scenario = Scenario::new(beside(posts(&volumes)));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("failed to read 7z archive: missing 7z part"),
    )
    .await;
}

#[tokio::test]
async fn a_7z_inside_a_stored_rar_whose_last_part_never_arrives_withholds_its_parts_unmapped_under_graph_and_fails_missing_a_part_under_classic()
 {
    let volumes = sevenz_without_its_last_part();
    let scenario = Scenario::new(beside(nest(&volumes)));
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("failed to read 7z archive: missing 7z part"),
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
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails(
            "PAR2 recovery failed for 1 set(s): ember.par2 (ember.bin): not repairable: 2 damaged slices",
        ),
    )
    .await;
}

#[tokio::test]
async fn a_stored_rar_a_set_covers_but_cannot_rebuild_is_listed_unrepaired_and_the_job_fails() {
    let outer = nest(&store(1)).remove(0).bytes;
    let scenario = Scenario::new(under_one_block("lantern.rar", "lantern", outer))
        .lost(0, 3)
        .lost(0, 5);
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails(
            "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern.rar): not repairable: 2 damaged slices",
        ),
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
    under_graph(
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Downloading)).await;
}

#[tokio::test]
async fn a_job_of_three_members_all_withheld_fails() {
    let posted: Vec<Post> = [("lantern.rar", 5), ("quill.rar", 6), ("ember.rar", 7)]
        .into_iter()
        .map(|(name, seed)| Post::plain(name, encrypted(1, seed).remove(0).1))
        .collect();
    let scenario = Scenario::new(posted);
    under_graph(
        &scenario,
        Graph {
            status: Some(Status::Failed(
                "lantern.rar, quill.rar, ember.rar needs a password and none was given",
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
    under_classic(&scenario, Classic::Stalls(JobStatus::Downloading)).await;
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
    under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails("archive file silver.horizon.part01.rar is incomplete"),
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
    let graph = under_graph(
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
    under_classic(
        &scenario,
        Classic::Fails(
            "PAR2 recovery failed for 1 set(s): ember.par2 (ember.bin): not repairable: 2 damaged slices",
        ),
    )
    .await;
}

// Restored after every article, the job comes to the same status and the
// same files as the run that never restarted.
#[tokio::test]
async fn a_partial_job_restarted_after_every_article_ends_partial_with_the_same_lists() {
    let scenario = three_one_withheld();
    let whole = match try_run(&scenario, Engine::Graph).await {
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
