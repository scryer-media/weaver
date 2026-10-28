// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// 7z containers: stored whole, split and posted header
// first, compressed, enciphered with and without the job's password, and
// damaged under a PAR2 set.

use std::collections::BTreeMap;
use std::io::Cursor;

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};

use weaver_graph::recovery::RepairOutcome;

use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::multi_member_store_set;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{Entry, build_7z_shaped};

use super::harness::{
    ARTICLE, Claim, JOB, Outcome, Post, Scenario, admit, assert_fetched_once, collect, kinds,
    par2_set, par2_volumes_sliced, payload, run, run_unprompted,
};
use super::refused_then_repaired::{drive_to_finish, serve_posts};
use crate::pipeline::tests::{job_status_for_assert, retire_pipeline_database};

pub(super) const SECRET: &str = "quill-ember-secret";

// The members every fixture holds, by name.
pub(super) fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("ember.bin", payload(5 * ARTICLE + 11, 41)),
        ("quill.bin", payload(3 * ARTICLE - 3, 42)),
    ]
}

// One container of `members()` coded by `method`, enciphered under
// `password` when there is one.
pub(super) fn container(method: EncoderConfiguration, password: Option<&str>) -> Vec<u8> {
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
pub(super) fn split(archive: &[u8], count: usize) -> Vec<Post> {
    let chunk = archive.len().div_ceil(count);
    archive
        .chunks(chunk)
        .zip(1..)
        .map(|(bytes, number)| Post::plain(&format!("lantern.7z.{number:03}"), bytes.to_vec()))
        .collect()
}

// Every delivered file lands in the job's folder.
pub(super) fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// The members, every byte as written, and nothing else.
fn assert_members(outcome: &Outcome) {
    assert!(outcome.completed(), "{:?}", outcome.failure);
    let expected = members();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(name, _)| delivered(name))
            .collect::<Vec<_>>(),
        "delivered names",
    );
    for (name, bytes) in expected {
        assert!(outcome.files[&delivered(name)] == bytes, "{name} bytes",);
    }
}

#[tokio::test]
async fn a_stored_7z_delivers_its_members() {
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", stored(None))]);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

#[tokio::test]
async fn a_stored_7z_split_into_three_volumes_delivers_its_members() {
    let scenario = Scenario::new(split(&stored(None), 3));
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

// A split set renamed to numbers of its own, counted from ten: the first
// volume's signature names the set, and the others are numbered from it.
#[tokio::test]
async fn a_split_7z_under_obfuscated_numbered_names_delivers_its_members() {
    let volumes = split(&stored(None), 3)
        .into_iter()
        .zip(10..)
        .map(|(post, number)| Post::plain(&format!("a1b2c3d4.{number}"), post.bytes))
        .collect();
    let graph = run(&Scenario::new(volumes)).await;
    assert_members(&graph);
}

// A split set's pieces under hash names that number nothing, the second
// posted first, and the PAR2 set that names them last. Only the first
// piece's signature says what it is; the others have no signature, so they
// wait unowned until the PAR2 set names them, and the store founded on the
// first claims each by the name it is given. Nothing is fetched twice.
fn hashed(volumes: &[Post]) -> Vec<Post> {
    let mut posts: Vec<Post> = volumes
        .iter()
        .zip(0u32..)
        .map(|(post, index)| {
            Post::plain(&format!("{:032x}", 0x5eed_0000 + index), post.bytes.clone())
        })
        .collect();
    posts.swap(0, 1);
    posts
}

fn hashed_with_par2_last() -> Vec<Post> {
    let volumes = split(&stored(None), 3);
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let mut posts = hashed(&volumes);
    posts.extend(par2_set("lantern", &files, 2));
    posts
}

// The members, and the PAR2 set's ruling that every piece was whole under
// the names it gave.
fn assert_named_by_par2(graph: &Outcome) {
    let view = graph.graph();
    assert!(view.delivery.withheld.is_empty(), "{:?}", view.delivery);
    assert_eq!(view.outcomes, vec![Some(RepairOutcome::Clean)]);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

#[tokio::test]
async fn a_split_7z_under_hash_names_named_by_par2_last_delivers_its_members() {
    let graph = run(&Scenario::new(hashed_with_par2_last())).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
    assert_named_by_par2(&graph);
}

// The same set one level down: its pieces and its PAR2 set are the members
// of one stored RAR, and the names the PAR2 set gives are claimed in that
// namespace as they are at the top. The PAR2 files are members of the
// archive like any other, so they are delivered beside the 7z's members.
#[tokio::test]
async fn a_split_7z_under_hash_names_inside_a_stored_rar_named_by_par2_last_delivers() {
    let inner = hashed_with_par2_last();
    let named: Vec<(&str, Vec<u8>)> = inner
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.clone()))
        .collect();
    let outer = multi_member_store_set(&named, 1).remove(0).1;
    let scenario = Scenario::new(vec![Post::plain("lantern.rar", outer)]);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let mut expected: Vec<String> = members().iter().map(|(name, _)| delivered(name)).collect();
    expected.extend(
        inner
            .iter()
            .filter(|post| post.name.ends_with(".par2"))
            .map(|post| delivered(&post.name)),
    );
    expected.sort();
    assert_eq!(graph.files.keys().cloned().collect::<Vec<_>>(), expected);
    for (name, bytes) in members() {
        assert!(graph.files[&delivered(name)] == bytes, "{name} bytes");
    }
    assert_named_by_par2(&graph);
}

// A coded split set whose middle volume was never posted, PAR2 beside: the
// engine takes the volumes on either side, the repair rebuilds the middle
// one, and the run reads through it to the members.
#[tokio::test]
async fn a_coded_split_7z_missing_its_middle_volume_is_rebuilt_by_par2_and_decoded() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let volumes = split(&archive, 3);
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let slices = volumes[1].bytes.len().div_ceil(ARTICLE) as u32;
    let mut posts = par2_set("lantern", &files, slices + 1);
    posts.extend(
        volumes
            .iter()
            .filter(|post| post.name != volumes[1].name)
            .map(|post| Post::plain(&post.name, post.bytes.clone())),
    );
    let graph = run(&Scenario::new(posts)).await;
    assert_members(&graph);
}

// The header volume is file 0, so it is answered before any of the body:
// the body arrives after its map and still reaches the members.
#[tokio::test]
async fn a_split_7z_posted_header_volume_first_delivers_its_members() {
    let mut volumes = split(&stored(None), 3);
    volumes.rotate_right(1);
    assert_eq!(volumes[0].name, "lantern.7z.003");
    let scenario = Scenario::new(volumes);
    let graph = run(&scenario).await;
    // `.003` has no signature and is a leaf until `.001` binds the store,
    // which is then offered it as it would have been had it come second.
    assert_members(&graph);
    assert_fetched_once(&graph);
}

#[tokio::test]
async fn an_lzma2_7z_is_extracted() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)]);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

#[tokio::test]
async fn an_aes_stored_7z_with_the_job_password_delivers_its_members() {
    let scenario =
        Scenario::new(vec![Post::plain("lantern.7z", stored(Some(SECRET)))]).password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

// The candidates are tried in their order: the job's own, then the
// settings'. The one the archive opened with is what the history keeps,
// sealed, and never as a public attribute or in the clear.
#[tokio::test]
async fn archive_password_sources_are_ordered_and_validated_password_is_private() {
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", stored(Some(SECRET)))])
        .password("filename-key")
        .global_password("global-key")
        .global_password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_eq!(graph.validated_password.as_deref(), Some(SECRET));
    let stored = graph
        .history_metadata
        .as_deref()
        .expect("the history row keeps the job's metadata");
    assert!(!stored.contains(SECRET), "{stored}");
    let metadata = crate::history::attributes::parse_history_metadata(Some(stored));
    assert!(metadata.iter().any(
        |(key, _)| key == crate::history::attributes::VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY
    ));
    assert!(
        !crate::public_history_attributes(&metadata)
            .iter()
            .any(|(key, _)| key.contains("validated_archive_password"))
    );
}

// An archive that needs no password leaves none in the history.
#[tokio::test]
async fn an_archive_with_no_password_keeps_none_in_the_history() {
    let scenario =
        Scenario::new(vec![Post::plain("lantern.7z", stored(None))]).password("filename-key");
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_eq!(graph.validated_password, None);
}

// No password for an AES-stored 7z: the store gives the container back
// withheld, since nothing opened it, so nothing is whole and the job fails
// naming the password it needs, with no refetch.
#[tokio::test]
async fn an_aes_stored_7z_without_a_password_fails_for_it() {
    let archive = stored(Some(SECRET));
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)]);
    let graph = run(&scenario).await;

    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert_eq!(
        graph.failure_kind,
        Some(HistoryFailureKind::Password),
        "failure kind",
    );
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    assert_eq!(
        graph.failure.as_deref(),
        Some("WEAVER_PASSWORD_REQUIRED: lantern.7z needs a password and none was given")
    );
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

#[tokio::test]
async fn a_damaged_stored_7z_under_par2_is_repaired_and_delivered() {
    let archive = stored(None);
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_set("lantern", &[("lantern.7z", &archive)], 2));
    // The second article is member bytes: the signature header is the first
    // article's, and the map is at the tail.
    let scenario = Scenario::new(posts).damaged(0, 1);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

// The same under AES: the damaged article is cipher bytes, and the repair
// reads the volume back through the store's keys and chain.
#[tokio::test]
async fn a_damaged_aes_stored_7z_under_par2_is_repaired_and_delivered() {
    let archive = stored(Some(SECRET));
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_set("lantern", &[("lantern.7z", &archive)], 2));
    let scenario = Scenario::new(posts).damaged(0, 1).password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

// The coded stream damaged beside a PAR2 set: the engine waits at the
// damaged article for its repair, then decodes the members once, and the
// job ends with nothing parked.
#[tokio::test]
async fn a_damaged_lzma2_7z_under_par2_is_repaired_in_the_stream_and_decoded() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_set("lantern", &[("lantern.7z", &archive)], 2));
    // The second article is coded bytes: the signature header is the first
    // article's, and the end header is at the tail.
    let scenario = Scenario::new(posts).damaged(0, 1);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
    assert!(
        kinds(&graph).contains(&"sevenz_engine"),
        "{:?}",
        kinds(&graph)
    );
    // An 11-slice archive (661 bytes): the count follows its size.
    assert_eq!(graph.reads, 20, "the slices the repair read back");
}

// How many candidates each node was offered.
fn offered(graph: &Outcome) -> Vec<usize> {
    graph.graph().offered.values().copied().collect()
}

fn aes_stored() -> Scenario {
    Scenario::new(vec![Post::plain("lantern.7z", stored(Some(SECRET)))])
}

// The password reaches the job only as a candidate. The store proves it by
// the smallest member's checksum and asks once.
#[tokio::test]
async fn an_aes_stored_7z_opened_by_a_password_candidate_delivers() {
    let graph = run(&aes_stored().global_password(SECRET)).await;
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(offered(&graph), [1], "one try for the one candidate");
    assert_fetched_once(&graph);
}

// A wrong candidate and then the right one: the proof refutes the first, the
// store asks again, and each candidate is offered once.
#[tokio::test]
async fn an_aes_stored_7z_tries_a_wrong_candidate_once_then_the_right_one() {
    let scenario = aes_stored()
        .global_password("cinder-wrong-guess")
        .global_password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(
        offered(&graph),
        [2],
        "one try for each of the two candidates"
    );
    assert_fetched_once(&graph);
}

// Every candidate wrong for a coded AES 7z: each run under a wrong one
// writes until the checksum refutes it, and none of what a refused run
// wrote stays. The job fails for the password, and its working directory
// never holds more than the posted archive.
#[tokio::test]
async fn a_coded_aes_7z_no_candidate_opens_keeps_no_more_than_was_posted() {
    let archive = container(Lzma2Options::from_level(1).into(), Some(SECRET));
    let posted = archive.len() as u64;
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)])
        .global_password("cinder-wrong-guess")
        .global_password("cinder-wrong-again");
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Password));
    assert!(
        graph.graph().scratch <= posted,
        "the working directory held {} bytes, {posted} were posted",
        graph.graph().scratch
    );
}

// AES over the members, coded by `method`, and over the end header alike,
// as 7zz writes `-mhe=on`.
pub(super) fn sealed_header(method: EncoderConfiguration) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![
        AesEncoderOptions::new(Password::new(SECRET)).into(),
        method,
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
    Scenario::new(vec![Post::plain(
        "lantern.7z",
        sealed_header(EncoderConfiguration::new(EncoderMethod::COPY)),
    )])
}

// The job's own password is its first candidate: the store, which cannot
// parse the header without one, asks and is offered it once.
#[tokio::test]
async fn an_aes_7z_with_encrypted_headers_and_the_job_password_delivers() {
    let graph = run(&sealed().password(SECRET)).await;
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(offered(&graph), [1], "the job's password, once");
    assert_fetched_once(&graph);
}

// The header refutes the wrong candidate by failing to parse, and opens on
// the right one.
#[tokio::test]
async fn an_aes_7z_with_encrypted_headers_opened_by_the_second_candidate_delivers() {
    let scenario = sealed()
        .global_password("cinder-wrong-guess")
        .global_password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(
        offered(&graph),
        [2],
        "one try for each of the two candidates"
    );
    assert_fetched_once(&graph);
}

// The last volume and the first are whole before the second has come:
// `.003` has no signature and is a leaf until `.001` binds the store, which
// is handed it closed. The store asks for the header's password on the
// bytes it is handed, and the job's password answers before the container
// is ruled shut, though the volume that holds the header is already in.
#[tokio::test]
async fn a_split_7z_with_encrypted_headers_whose_last_volume_came_first_opens_on_the_job_password()
{
    let scenario = Scenario::new(split(
        &sealed_header(EncoderConfiguration::new(EncoderMethod::COPY)),
        3,
    ))
    .password(SECRET);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    while pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        .is_some()
    {}
    serve_posts(&mut pipeline, &scenario, 2..3).await;
    serve_posts(&mut pipeline, &scenario, 0..1).await;
    serve_posts(&mut pipeline, &scenario, 1..2).await;
    drive_to_finish(&mut pipeline, &scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;

    assert_eq!(status, Some(JobStatus::Complete), "{:?}", files.keys());
    let expected = members();
    assert_eq!(
        files.keys().cloned().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(name, _)| delivered(name))
            .collect::<Vec<_>>()
    );
    for (name, bytes) in expected {
        assert!(files[&delivered(name)] == bytes, "{name} bytes");
    }
}

fn lzma2() -> Scenario {
    Scenario::new(vec![Post::plain(
        "lantern.7z",
        container(Lzma2Options::from_level(1).into(), None),
    )])
}

// The LZMA2 container cut into articles of every size from the whole file
// down to a byte (4 KiB is the whole 401-byte file again): one outcome and
// one set of files, and nothing fetched twice.
#[tokio::test]
async fn an_lzma2_7z_delivers_alike_in_every_frame_size() {
    let mut seen = None;
    for size in [usize::MAX, 4096, 64, 1] {
        let graph = run(&lzma2().framed(size)).await;
        assert_members(&graph);
        assert!(
            kinds(&graph).contains(&"sevenz_engine"),
            "{:?}",
            kinds(&graph)
        );
        assert_fetched_once(&graph);
        let files = seen.get_or_insert_with(|| graph.files.clone());
        assert_eq!(*files, graph.files, "frames of {size} bytes");
    }
}

// The three parts served last article first, as a crowd of workers may:
// the end header that maps the members is in hand first, the signature
// header that finds it last.
#[tokio::test]
async fn a_stored_7z_served_last_article_first_delivers_its_members() {
    let scenario = Scenario::new(split(&stored(None), 3)).backwards();
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// The same frames served last article first: the folder's packed stream is
// in hand before the headers that say the container needs the engine.
#[tokio::test]
async fn an_lzma2_7z_served_last_article_first_delivers_alike_in_every_frame_size() {
    let mut seen = None;
    for size in [usize::MAX, 4096, 64, 1] {
        let graph = run(&lzma2().framed(size).backwards()).await;
        assert_members(&graph);
        assert!(
            kinds(&graph).contains(&"sevenz_engine"),
            "{:?}",
            kinds(&graph)
        );
        assert_fetched_once(&graph);
        let files = seen.get_or_insert_with(|| graph.files.clone());
        assert_eq!(*files, graph.files, "frames of {size} bytes");
    }
}

// The packed streams end where the end header starts, which the signature
// header names.
pub(super) fn packed_end(archive: &[u8]) -> usize {
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
// Partial.
#[tokio::test]
async fn an_lzma2_7z_with_a_damaged_member_and_no_par2_delivers_the_members_it_closed() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let damaged = last_packed_article(&archive);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive.clone())])
        .framed(16)
        .damaged(0, damaged);
    let graph = run(&scenario).await;

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

// A single 7z whose head arrived wrong under a good article checksum, with
// no recovery: no store founds on it, so it reaches the end whole as
// posted. A whole archive is not a file the user opens, and nothing else
// came out of it, so the job fails and publishes nothing.
#[tokio::test]
async fn a_single_7z_with_a_damaged_head_and_no_recovery_fails() {
    let (name, ember) = members().swap_remove(0);
    let mut archive = build_7z_shaped(
        &[Entry::file(name, ember.clone())],
        EncoderMethod::COPY,
        None,
        false,
    );
    for byte in &mut archive[..ARTICLE] {
        *byte ^= 0x5a;
    }
    let graph = run(&Scenario::new(vec![Post::plain("lantern.7z", archive)])).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "graph: {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.files.keys().collect::<Vec<_>>()
    );
    assert!(
        graph.files.get(&delivered(name)) != Some(&ember),
        "ember.bin never comes out of a container no store founds on"
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
// store never maps. Graph refuses it (`Why::Unmapped`) and gives the
// container back withheld, as it stands, in place of its members; the
// object failed and nothing is whole, so the job fails for unpacking, worded
// by the refusal, never Complete with its members missing.
#[tokio::test]
async fn a_7z_whose_end_header_is_cut_short_is_refused_and_fails() {
    let mut archive = stored(None);
    archive.truncate(archive.len() - 8);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)]);
    let graph = run(&scenario).await;
    assert_eq!(
        graph.failure.as_deref(),
        Some("lantern.7z never placed every member it named")
    );
    assert!(graph.files.is_empty(), "graph: {:?}", graph.files.keys());
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

// The end header damaged beside PAR2: the repair reads the container back
// from what the namespace holds for it, rewrites the header, the store maps
// from the repaired bytes and the members are decoded.
#[tokio::test]
async fn an_lzma2_7z_with_a_damaged_end_header_beside_par2_is_repaired_and_decoded() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let scenario = beside_par2(&archive, 2).damaged(0, last_article(&archive));
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

// More of the end header damaged than PAR2 can rebuild: the store never
// maps and the repair is impossible, so graph fails the job for the repair
// rather than ending Complete with nothing.
#[tokio::test]
async fn an_lzma2_7z_with_its_end_header_damaged_past_par2_is_refused() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let last = last_article(&archive);
    let scenario = beside_par2(&archive, 1)
        .damaged(0, last)
        .damaged(0, last - 1);
    let graph = run(&scenario).await;
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
// decoded.
#[tokio::test]
async fn an_lzma2_7z_with_a_damaged_first_article_beside_par2_binds_its_store_after_the_repair() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let scenario = beside_par2(&archive, 2).damaged(0, 0);
    let graph = run(&scenario).await;
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

// A split 7z whose last volume, which holds the end header, is cut short:
// the store never maps, and every volume it holds is refused with the set.
// No volume of a container is a delivery of its own, so nothing is whole
// and the job fails for unpacking.
#[tokio::test]
async fn a_split_7z_whose_last_volume_is_cut_short_delivers_no_volume_whole() {
    let mut volumes = split(&stored(None), 3);
    let last = volumes.last_mut().unwrap();
    last.bytes.truncate(last.bytes.len() - 8);
    let graph = run(&Scenario::new(volumes)).await;
    let view = graph.graph();
    assert!(view.delivery.whole.is_empty(), "{:?}", view.delivery);
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Unpack));
}

// A coded split set beside PAR2 with one article of a middle volume
// damaged: the recovery blocks it needs are in by the time the payload is,
// so the set rules and repairs in the step its last byte arrives, the run
// reads on through the repaired bytes, and the delivery is scheduled by the
// job's own steps, never by a look the pipeline takes when it falls idle.
async fn damaged_split_rules_unprompted(count: usize, slice: u64, blocks: u32, volumes: u32) {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let parts = split(&archive, count);
    let files: Vec<(&str, &[u8])> = parts
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let mut posts = par2_volumes_sliced("lantern", &files, slice, blocks, volumes);
    let recovery: Vec<Post> = posts.split_off(1);
    let first = posts.len() as u32;
    posts.extend(parts);
    posts.extend(recovery);
    let scenario = Scenario::new(posts).damaged(first + 1, 1);
    let (outcomes, moved) = run_unprompted(&scenario).await;
    assert!(
        matches!(outcomes.as_slice(), [Some(RepairOutcome::Repaired { .. })]),
        "{outcomes:?}"
    );
    assert!(moved, "the delivery waited for an idle look");
}

#[tokio::test]
async fn a_damaged_split_lzma2_7z_under_par2_rules_and_delivers_without_an_idle_look() {
    damaged_split_rules_unprompted(3, ARTICLE as u64, 2, 1).await;
}

#[tokio::test]
async fn a_damaged_six_volume_lzma2_7z_under_five_par2_volumes_rules_and_delivers_without_an_idle_look()
 {
    damaged_split_rules_unprompted(6, ARTICLE as u64 / 4, 16, 5).await;
}

// An AES-stored 7z whose one file is larger than any a password is proved
// against, offered a wrong candidate before the right one: the graph's store
// opens the wrong one on trial, its checksum fails on verified bytes, the
// folder's bytes come again and open under the right one, and the file is
// delivered whole with nothing fetched twice.
#[tokio::test]
async fn a_large_aes_stored_7z_offered_a_wrong_password_first_delivers() {
    let file = payload(4 * 1024 * 1024 + 1, 61);
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![
        AesEncoderOptions::new(Password::new(SECRET))
            .with_num_cycles_power(4)
            .into(),
        EncoderConfiguration::new(EncoderMethod::COPY),
    ]);
    let mut entry = ArchiveEntry::new_file("lantern.bin");
    entry.size = file.len() as u64;
    writer
        .push_archive_entry(entry, Some(Cursor::new(file.clone())))
        .unwrap();
    let archive = writer.finish().unwrap().into_inner();
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)])
        .framed(256 << 10)
        .global_password("not-the-secret")
        .global_password(SECRET);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("lantern.bin")]
    );
    assert!(graph.files[&delivered("lantern.bin")] == file, "bytes");
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A 7z coded by `method` holding an empty directory beside a member.
fn with_empty_directory(method: EncoderConfiguration) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![method]);
    writer
        .push_archive_entry::<Cursor<Vec<u8>>>(ArchiveEntry::new_directory("hollow"), None)
        .unwrap();
    let (name, bytes) = members().remove(0);
    let mut entry = ArchiveEntry::new_file(name);
    entry.size = bytes.len() as u64;
    writer
        .push_archive_entry(entry, Some(Cursor::new(bytes)))
        .unwrap();
    writer.finish().unwrap().into_inner()
}

// The empty directory is delivered as a directory, stored through the store
// and LZMA2 through the engine.
#[tokio::test]
async fn a_7z_holding_an_empty_directory_delivers_it() {
    let methods: [EncoderConfiguration; 2] = [
        EncoderConfiguration::new(EncoderMethod::COPY),
        Lzma2Options::from_level(1).into(),
    ];
    for method in methods {
        let scenario = Scenario::new(vec![Post::plain(
            "lantern.7z",
            with_empty_directory(method),
        )]);
        let graph = run(&scenario).await;
        assert!(graph.completed(), "{:?}", graph.failure);
        assert_eq!(
            graph.empty.iter().collect::<Vec<_>>(),
            [&delivered("hollow/")],
            "empty directories"
        );
        assert_fetched_once(&graph);
    }
}
