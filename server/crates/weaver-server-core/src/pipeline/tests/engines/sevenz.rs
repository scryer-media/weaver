// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// 7z containers under both engines: stored whole, split and posted header
// first, compressed, enciphered with and without the job's password, and
// damaged under a PAR2 set.

use std::io::Cursor;

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};

use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_same, par2_set, payload, run_both,
};

const SECRET: &str = "quill-ember-secret";

// The members every fixture holds, by name.
fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("ember.bin", payload(5 * ARTICLE + 11, 41)),
        ("quill.bin", payload(3 * ARTICLE - 3, 42)),
    ]
}

// One container of `members()` coded by `method`, enciphered under
// `password` when there is one.
fn container(method: EncoderConfiguration, password: Option<&str>) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    let mut methods = vec![method];
    if let Some(password) = password {
        // Encryption applies to coded bytes, so it sits at the output end.
        methods.insert(0, AesEncoderOptions::new(Password::new(password)).into());
    }
    writer.set_content_methods(methods);
    for (name, bytes) in members() {
        let mut entry = ArchiveEntry::new_file(name);
        entry.size = bytes.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(bytes)))
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn stored(password: Option<&str>) -> Vec<u8> {
    container(EncoderConfiguration::new(EncoderMethod::COPY), password)
}

// A pure byte split at a fixed size into `.7z.001` onward, the last volume
// the short tail.
fn split(archive: &[u8], count: usize) -> Vec<Post> {
    let chunk = archive.len().div_ceil(count);
    archive
        .chunks(chunk)
        .zip(1..)
        .map(|(bytes, number)| Post::plain(&format!("lantern.7z.{number:03}"), bytes.to_vec()))
        .collect()
}

// Every delivered file lands in the job's folder.
fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// Both engines end the job the same way and deliver the same files, though
// classic fetched some articles twice on the way and graph none.
fn assert_same_delivered_classic_refetching(classic: &Outcome, graph: &Outcome) {
    assert!(
        graph.refetched().is_empty(),
        "graph asked for an article twice: {:?}",
        graph.refetched()
    );
    assert!(!classic.refetched().is_empty(), "classic refetched nothing");
    assert_eq!(classic.status, graph.status, "status");
    assert_eq!(classic.failure_kind, graph.failure_kind, "failure kind");
    assert_eq!(classic.files, graph.files, "delivered files");
    assert_eq!(classic.claims, graph.claims, "claims");
}

// The members, every byte as written, and nothing else.
fn assert_members(outcome: &Outcome) {
    assert!(
        outcome.completed(),
        "{}: {:?}",
        outcome.engine,
        outcome.failure
    );
    let expected = members();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(name, _)| delivered(name))
            .collect::<Vec<_>>(),
        "{}: delivered names",
        outcome.engine
    );
    for (name, bytes) in expected {
        assert!(
            outcome.files[&delivered(name)] == bytes,
            "{}: {name} bytes",
            outcome.engine
        );
    }
}

#[tokio::test]
async fn a_stored_7z_delivers_its_members_under_both_engines() {
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", stored(None))]);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&graph);
    assert_members(&classic);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn a_stored_7z_split_into_three_volumes_delivers_its_members_under_both_engines() {
    let scenario = Scenario::new(split(&stored(None), 3));
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&graph);
    assert_members(&classic);
    assert_same(&classic, &graph);
}

// The header volume is file 0, so it is answered before any of the body:
// the body arrives after its map and still reaches the members.
#[tokio::test]
async fn a_split_7z_posted_header_volume_first_delivers_its_members_under_both_engines() {
    let mut volumes = split(&stored(None), 3);
    volumes.rotate_right(1);
    assert_eq!(volumes[0].name, "lantern.7z.003");
    let scenario = Scenario::new(volumes);
    let (classic, graph) = run_both(&scenario).await;
    // `.003` has no signature and is a leaf until `.001` binds the store,
    // which is then offered it as it would have been had it come second.
    assert_members(&graph);
    assert_members(&classic);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn an_lzma2_7z_is_extracted_under_both_engines_and_only_classic_refetches() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)]);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&graph);
    assert_members(&classic);
    // Classic's direct store cannot place coded bytes and demotes, which
    // fetches the archive again; graph binds the 7z engine in the stream.
    assert_same_delivered_classic_refetching(&classic, &graph);
}

#[tokio::test]
async fn an_aes_stored_7z_with_the_job_password_delivers_its_members_under_both_engines() {
    let scenario =
        Scenario::new(vec![Post::plain("lantern.7z", stored(Some(SECRET)))]).password(SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&graph);
    assert_members(&classic);
    assert_same(&classic, &graph);
}

// Neither engine has a password claim to leave: classic fails the job for
// the password after fetching the set again, graph leaves the container
// whole and completes without naming the password.
#[tokio::test]
async fn an_aes_stored_7z_without_a_password_fails_for_it_under_classic_and_is_left_whole_under_graph()
 {
    let archive = stored(Some(SECRET));
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive.clone())]);
    let (classic, graph) = run_both(&scenario).await;

    assert!(
        matches!(classic.status, Some(JobStatus::Failed { .. })),
        "classic: {:?}",
        classic.status
    );
    assert_eq!(
        classic.failure_kind,
        Some(HistoryFailureKind::Password),
        "classic: failure kind"
    );
    assert!(
        classic.files.is_empty(),
        "classic: {:?}",
        classic.files.keys()
    );

    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
    assert!(graph.completed(), "graph: {:?}", graph.failure);
    assert_eq!(graph.failure_kind, None, "graph: failure kind");
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("lantern.7z")],
        "graph: delivered names"
    );
    assert!(
        graph.files[&delivered("lantern.7z")] == archive,
        "graph: container bytes"
    );
}

#[tokio::test]
async fn a_damaged_stored_7z_under_par2_is_repaired_and_delivered_under_both_engines_and_only_classic_refetches()
 {
    let archive = stored(None);
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_set("lantern", &[("lantern.7z", &archive)], 2));
    // The second article is member bytes: the signature header is the first
    // article's, and the map is at the tail.
    let scenario = Scenario::new(posts).damaged(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&graph);
    assert_members(&classic);
    // Classic demotes the damaged set and fetches the article again; graph
    // repairs the volume in the stream.
    assert_same_delivered_classic_refetching(&classic, &graph);
}

// The coded stream damaged beside a PAR2 set: the engine waits at the
// damaged article for its repair, then decodes the members once, and the
// job ends with nothing parked. Divergence: classic demotes and fetches the
// container's first article again; graph repairs in the stream.
#[tokio::test]
async fn a_damaged_lzma2_7z_under_par2_is_repaired_in_the_stream_and_decoded_under_both_engines() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_set("lantern", &[("lantern.7z", &archive)], 2));
    // The second article is coded bytes: the signature header is the first
    // article's, and the end header is at the tail.
    let scenario = Scenario::new(posts).damaged(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&graph);
    assert_members(&classic);
    assert!(
        kinds(&graph).contains(&"sevenz_engine"),
        "{:?}",
        kinds(&graph)
    );
    assert_eq!(classic.refetched(), [(0, 0)]);
    assert_same_delivered_classic_refetching(&classic, &graph);
    assert_eq!(graph.reads, 12, "the slices the repair read back");
}

// The kinds of object the graph bound, in name order.
fn kinds(graph: &Outcome) -> Vec<&str> {
    graph.graph().kinds.iter().map(String::as_str).collect()
}

// How many candidates each node was offered.
fn offered(graph: &Outcome) -> Vec<usize> {
    graph.graph().offered.values().copied().collect()
}

fn aes_stored() -> Scenario {
    Scenario::new(vec![Post::plain("lantern.7z", stored(Some(SECRET)))])
}

// The password reaches the job only as a candidate. The store proves it by
// the smallest member's checksum and asks once; classic tries the candidate
// itself, and both deliver alike.
#[tokio::test]
async fn an_aes_stored_7z_opened_by_a_password_candidate_delivers_under_both_engines() {
    let (classic, graph) = run_both(&aes_stored().global_password(SECRET)).await;
    assert_members(&classic);
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(offered(&graph), [1], "one try for the one candidate");
    assert_same(&classic, &graph);
}

// A wrong candidate and then the right one: the proof refutes the first, the
// store asks again, and each candidate is offered once.
#[tokio::test]
async fn an_aes_stored_7z_tries_a_wrong_candidate_once_then_the_right_one_under_both_engines() {
    let scenario = aes_stored()
        .global_password("cinder-wrong-guess")
        .global_password(SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&classic);
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(
        offered(&graph),
        [2],
        "one try for each of the two candidates"
    );
    assert_same(&classic, &graph);
}

// AES over the members and the end header alike, as 7zz writes `-mhe=on`.
fn sealed_header() -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![
        AesEncoderOptions::new(Password::new(SECRET)).into(),
        EncoderConfiguration::new(EncoderMethod::COPY),
    ]);
    writer.set_encrypt_header(true);
    for (name, bytes) in members() {
        let mut entry = ArchiveEntry::new_file(name);
        entry.size = bytes.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(bytes)))
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn sealed() -> Scenario {
    Scenario::new(vec![Post::plain("lantern.7z", sealed_header())])
}

// The job's own password is its first candidate: the store, which cannot
// parse the header without one, asks and is offered it once.
#[tokio::test]
async fn an_aes_7z_with_encrypted_headers_and_the_job_password_delivers_under_both_engines() {
    let (classic, graph) = run_both(&sealed().password(SECRET)).await;
    assert_members(&classic);
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(offered(&graph), [1], "the job's password, once");
    assert_same(&classic, &graph);
}

// The header refutes the wrong candidate by failing to parse, and opens on
// the right one.
#[tokio::test]
async fn an_aes_7z_with_encrypted_headers_opened_by_the_second_candidate_delivers_under_both_engines()
 {
    let scenario = sealed()
        .global_password("cinder-wrong-guess")
        .global_password(SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&classic);
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(
        offered(&graph),
        [2],
        "one try for each of the two candidates"
    );
    assert_same(&classic, &graph);
}

fn lzma2() -> Scenario {
    Scenario::new(vec![Post::plain(
        "lantern.7z",
        container(Lzma2Options::from_level(1).into(), None),
    )])
}

// The LZMA2 container cut into articles of every size from the whole file
// down to a byte (4 KiB is the whole 401-byte file again): one outcome and
// one set of files under both engines. Divergence: from 64-byte articles
// down, classic's direct store demotes and fetches the container again;
// graph never refetches.
#[tokio::test]
async fn an_lzma2_7z_delivers_alike_in_every_frame_size_under_both_engines() {
    let mut seen = None;
    for size in [usize::MAX, 4096, 64, 1] {
        let (classic, graph) = run_both(&lzma2().framed(size)).await;
        assert_members(&classic);
        assert_members(&graph);
        assert!(
            kinds(&graph).contains(&"sevenz_engine"),
            "{:?}",
            kinds(&graph)
        );
        if size >= 4096 {
            assert_same(&classic, &graph);
        } else {
            assert_same_delivered_classic_refetching(&classic, &graph);
        }
        let files = seen.get_or_insert_with(|| graph.files.clone());
        assert_eq!(*files, graph.files, "frames of {size} bytes");
    }
}

// The packed streams end where the end header starts, which the signature
// header names.
fn packed_end(archive: &[u8]) -> usize {
    32 + u64::from_le_bytes(archive[12..20].try_into().unwrap()) as usize
}

// In 16-byte articles, the last article wholly inside the packed streams:
// quill.bin's, the second folder, touching no header.
fn last_packed_article(archive: &[u8]) -> u32 {
    (packed_end(archive) / 16 - 1) as u32
}

// quill.bin's stream damaged with no PAR2. The engine closes ember.bin whole,
// then fails quill.bin's CRC: graph delivers ember.bin as written, leaves
// quill.bin out, puts the container as posted in its place, and ends the job
// Partial. Divergence: classic fails the job (Unpack), delivers nothing and
// fetches every article before the damaged one again.
#[tokio::test]
async fn an_lzma2_7z_with_a_damaged_member_and_no_par2_delivers_the_members_it_closed_under_graph()
{
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let damaged = last_packed_article(&archive);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive.clone())])
        .framed(16)
        .damaged(0, damaged);
    let (classic, graph) = run_both(&scenario).await;

    assert!(
        matches!(classic.status, Some(JobStatus::Failed { .. })),
        "classic: {:?}",
        classic.status
    );
    assert_eq!(classic.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(
        classic.files.is_empty(),
        "classic: {:?}",
        classic.files.keys()
    );
    assert_eq!(
        classic.refetched(),
        (0..damaged).map(|at| (0, at)).collect::<Vec<_>>()
    );

    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "graph: {:?}",
        graph.failure
    );
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
    assert!(
        kinds(&graph).contains(&"sevenz_engine"),
        "{:?}",
        kinds(&graph)
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        [delivered("ember.bin"), delivered("lantern.7z")]
    );
    assert!(
        graph.files[&delivered("ember.bin")] == members()[0].1,
        "ember.bin closed before the failure, delivered whole"
    );
    assert_eq!(
        graph.files[&delivered("lantern.7z")].len(),
        archive.len(),
        "the container as posted"
    );
    assert_eq!(
        graph.graph().claims.get("lantern.7z"),
        Some(&Claim::Damaged)
    );
}

// The last article of a container, counting from zero.
fn last_article(archive: &[u8]) -> u32 {
    (archive.len().div_ceil(ARTICLE) - 1) as u32
}

fn beside_par2(archive: &[u8], blocks: u32) -> Scenario {
    let mut posts = vec![Post::plain("lantern.7z", archive.to_vec())];
    posts.extend(par2_set("lantern", &[("lantern.7z", archive)], blocks));
    Scenario::new(posts)
}

// The end header cut short and posted whole: every volume closes and the
// store never maps. Graph refuses it (`Why::Unmapped`) and delivers the
// container as it stands in place of its members; the object failed, so the
// job is Partial for unpacking, never Complete with its members missing.
// Classic fails the job: it cannot read the archive.
#[tokio::test]
async fn a_7z_whose_end_header_is_cut_short_is_refused_with_the_container_delivered_under_graph() {
    let mut archive = stored(None);
    archive.truncate(archive.len() - 8);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive.clone())]);
    let (classic, graph) = run_both(&scenario).await;
    assert!(
        matches!(classic.status, Some(JobStatus::Failed { .. })),
        "classic: {:?}",
        classic.status
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        [delivered("lantern.7z")],
        "graph: {:?}",
        graph.status
    );
    assert!(
        graph.files[&delivered("lantern.7z")] == archive,
        "the container as posted"
    );
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

// The end header damaged beside PAR2: the repair reads the container back
// from what the namespace holds for it, rewrites the header, the store maps
// from the repaired bytes and the members are decoded. Classic repairs and
// extracts it too.
#[tokio::test]
async fn an_lzma2_7z_with_a_damaged_end_header_beside_par2_is_repaired_and_decoded_under_graph() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let scenario = beside_par2(&archive, 2).damaged(0, last_article(&archive));
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&classic);
    assert_members(&graph);
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

// More of the end header damaged than PAR2 can rebuild: the store never
// maps and the repair is impossible, so graph fails the job for the repair
// rather than ending Complete with nothing. Classic fails it for the repair
// too.
#[tokio::test]
async fn an_lzma2_7z_with_its_end_header_damaged_past_par2_is_refused_under_graph() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let last = last_article(&archive);
    let scenario = beside_par2(&archive, 1)
        .damaged(0, last)
        .damaged(0, last - 1);
    let (classic, graph) = run_both(&scenario).await;
    assert!(
        matches!(classic.status, Some(JobStatus::Failed { .. })),
        "classic: {:?}",
        classic.status
    );
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "graph: {:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

// The first article damaged beside PAR2: the front cannot be sniffed, so
// the container is a leaf until the repair rewrites its front; then it is
// identified again by the one offer every unowned member gets, the store
// binds, hands the coded folders to the engine, and the members are
// decoded. Classic repairs and extracts it too.
#[tokio::test]
async fn an_lzma2_7z_with_a_damaged_first_article_beside_par2_binds_its_store_after_the_repair_under_graph()
 {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let scenario = beside_par2(&archive, 2).damaged(0, 0);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&classic);
    assert_members(&graph);
    assert!(
        kinds(&graph).contains(&"sevenz_engine"),
        "{:?}",
        kinds(&graph)
    );
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}
