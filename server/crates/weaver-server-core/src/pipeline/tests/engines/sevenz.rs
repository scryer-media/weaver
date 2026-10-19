// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// 7z containers under both engines: stored whole, split and posted header
// first, compressed, enciphered with and without the job's password, and
// damaged under a PAR2 set.

use std::io::Cursor;

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};

use weaver_graph::recovery::RepairOutcome;

use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::multi_member_store_set;

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_same, kinds, par2_set, payload, run, run_both,
    try_run,
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

// A split set renamed to numbers of its own, counted from ten: the first
// volume's signature names the set, and the others are numbered from it.
#[tokio::test]
async fn a_split_7z_under_obfuscated_numbered_names_delivers_its_members() {
    let volumes = split(&stored(None), 3)
        .into_iter()
        .zip(10..)
        .map(|(post, number)| Post::plain(&format!("a1b2c3d4.{number}"), post.bytes))
        .collect();
    let graph = run(&Scenario::new(volumes), Engine::Graph).await;
    assert_members(&graph);
}

// A split set's pieces under hash names that number nothing, the second
// posted first, and the PAR2 set that names them last. Only the first
// piece's signature says what it is; the others have no signature, so they
// wait unowned until the PAR2 set names them, and the store founded on the
// first claims each by the name it is given. Nothing is fetched twice.
// Classic renames by the PAR2 set before it reads the set, and delivers the
// same.
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
async fn a_split_7z_under_hash_names_named_by_par2_last_delivers_its_members_under_both_engines() {
    let (classic, graph) = run_both(&Scenario::new(hashed_with_par2_last())).await;
    assert_members(&classic);
    assert_members(&graph);
    assert_same(&classic, &graph);
    assert_named_by_par2(&graph);
}

// The same set one level down: its pieces and its PAR2 set are the members
// of one stored RAR, and the names the PAR2 set gives are claimed in that
// namespace as they are at the top. The PAR2 files are members of the
// archive like any other, so they are delivered beside the 7z's members.
// Divergence: classic extracts the RAR and never renames what came out of
// it, so it cannot read the 7z and fails the job.
#[tokio::test]
async fn a_split_7z_under_hash_names_inside_a_stored_rar_named_by_par2_last_delivers_under_graph() {
    let inner = hashed_with_par2_last();
    let named: Vec<(&str, Vec<u8>)> = inner
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.clone()))
        .collect();
    let outer = multi_member_store_set(&named, 1).remove(0).1;
    let scenario = Scenario::new(vec![Post::plain("lantern.rar", outer)]);
    let classic = try_run(&scenario, Engine::Classic).await.unwrap();
    assert!(
        matches!(classic.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        classic.status
    );
    let graph = run(&scenario, Engine::Graph).await;
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
    let graph = run(&Scenario::new(posts), Engine::Graph).await;
    assert_members(&graph);
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

// No password for an AES-stored 7z: classic fails the job for the password
// after fetching the set again. The graph's store gives the container back
// withheld, since nothing opened it, so nothing is whole and the job fails
// naming the password it needs, with no refetch.
#[tokio::test]
async fn an_aes_stored_7z_without_a_password_fails_for_it_under_both_engines() {
    let archive = stored(Some(SECRET));
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)]);
    let (classic, graph) = run_both(&scenario).await;

    for outcome in [&classic, &graph] {
        assert!(
            matches!(outcome.status, Some(JobStatus::Failed { .. })),
            "{}: {:?}",
            outcome.engine,
            outcome.status
        );
        assert_eq!(
            outcome.failure_kind,
            Some(HistoryFailureKind::Password),
            "{}: failure kind",
            outcome.engine
        );
        assert!(
            outcome.files.is_empty(),
            "{}: {:?}",
            outcome.engine,
            outcome.files.keys()
        );
    }
    assert_eq!(
        graph.failure.as_deref(),
        Some("lantern.7z needs a password and none was given")
    );
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
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

// The three parts served last article first, as a crowd of workers may:
// the end header that maps the members is in hand first, the signature
// header that finds it last. Graph only: the batch the harness serves
// backwards is in no queue classic can see.
#[tokio::test]
async fn a_stored_7z_served_last_article_first_delivers_its_members_under_graph() {
    let scenario = Scenario::new(split(&stored(None), 3)).backwards();
    let graph = run(&scenario, Engine::Graph).await;
    assert_members(&graph);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// The same frames served last article first: the folder's packed stream is
// in hand before the headers that say the container needs the engine.
#[tokio::test]
async fn an_lzma2_7z_served_last_article_first_delivers_alike_in_every_frame_size_under_both_engines()
 {
    let mut seen = None;
    for size in [usize::MAX, 4096, 64, 1] {
        let (classic, graph) = run_both(&lzma2().framed(size).backwards()).await;
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
// store never maps. Graph refuses it (`Why::Unmapped`) and gives the
// container back withheld, as it stands, in place of its members; the
// object failed and nothing is whole, so the job fails for unpacking, worded
// by the refusal, never Complete with its members missing. Classic fails the job: it cannot read the archive.
#[tokio::test]
async fn a_7z_whose_end_header_is_cut_short_is_refused_and_fails_under_graph() {
    let mut archive = stored(None);
    archive.truncate(archive.len() - 8);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)]);
    let (classic, graph) = run_both(&scenario).await;
    assert!(
        matches!(classic.status, Some(JobStatus::Failed { .. })),
        "classic: {:?}",
        classic.status
    );
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

// A split 7z whose last volume, which holds the end header, is cut short:
// the store never maps, and every volume it holds is refused with the set.
// No volume of a container is a delivery of its own, so nothing is whole
// and the job fails for unpacking.
#[tokio::test]
async fn a_split_7z_whose_last_volume_is_cut_short_delivers_no_volume_whole_under_graph() {
    let mut volumes = split(&stored(None), 3);
    let last = volumes.last_mut().unwrap();
    last.bytes.truncate(last.bytes.len() - 8);
    let graph = run(&Scenario::new(volumes), Engine::Graph).await;
    let view = graph.graph();
    assert!(view.delivery.whole.is_empty(), "{:?}", view.delivery);
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Unpack));
}
