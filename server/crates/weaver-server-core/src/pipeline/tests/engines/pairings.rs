// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Child archives one deep that no other row builds: a 7z crossing both
// volume boundaries of a three-volume RAR, and that set repaired at the
// root; an AES 7z offered only a wrong candidate; two levels each opened by
// its own password; stores and streams bound out of a compressed 7z's
// output, its headers encrypted or not; a 7z inside a header-encrypted RAR;
// a 7z of a directory, an empty file and an empty directory; two streams of
// one name in two directories; and an xz cut short inside a store.

use std::io::Cursor;

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};
use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Outcome, Post, Scenario, kinds, par2_set, payload, run};
use super::sevenz::SECRET;
use super::streams::{gzip, xz};
use crate::history::HistoryFailureKind;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::multi_member_store_set;
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_store_set, header_encrypted_store_set,
};

const WRONG: &str = "cinder-wrong-guess";
const INNER: &str = "hollow-crest-inner";

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

enum Entry<'a> {
    File(&'a str, &'a [u8]),
    Dir(&'a str),
}

fn copy() -> EncoderConfiguration {
    EncoderConfiguration::new(EncoderMethod::COPY)
}

fn lzma2() -> EncoderConfiguration {
    Lzma2Options::from_level(1).into()
}

// A 7z of `entries` coded by `method`, enciphered under `password` when
// there is one, and its end header with it when `sealed`.
fn sevenz(
    entries: &[Entry<'_>],
    method: EncoderConfiguration,
    password: Option<&str>,
    sealed: bool,
) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    let mut methods = vec![method];
    if let Some(password) = password {
        // Encryption applies to coded bytes, so it sits at the output end.
        methods.insert(0, AesEncoderOptions::new(Password::new(password)).into());
    }
    writer.set_content_methods(methods);
    writer.set_encrypt_header(sealed);
    for entry in entries {
        match entry {
            Entry::File(name, bytes) => writer
                .push_archive_entry(
                    ArchiveEntry::new_file(name),
                    Some(Cursor::new(bytes.to_vec())),
                )
                .unwrap(),
            Entry::Dir(name) => writer
                .push_archive_entry::<Cursor<Vec<u8>>>(ArchiveEntry::new_directory(name), None)
                .unwrap(),
        };
    }
    writer.finish().unwrap().into_inner()
}

// A stored RAR5 volume of `entries`, in order.
fn rar(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    multi_member_store_set(entries, 1).remove(0).1
}

fn posts(volumes: &[(String, Vec<u8>)]) -> Vec<Post> {
    volumes
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect()
}

// The job completed, delivered exactly `files` byte for byte and whole,
// withheld nothing, and asked for no article twice.
fn assert_whole(graph: &Outcome, files: &[(&str, &[u8])]) {
    assert!(
        graph.completed(),
        "{:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().delivery
    );
    let mut names: Vec<String> = files.iter().map(|(name, _)| delivered(name)).collect();
    names.sort();
    assert_eq!(graph.files.keys().cloned().collect::<Vec<_>>(), names);
    for (name, bytes) in files {
        assert!(graph.files[&delivered(name)] == *bytes, "{name}");
    }
    let delivery = &graph.graph().delivery;
    assert!(delivery.withheld.is_empty(), "{delivery:?}");
    assert!(
        delivery.damaged.is_empty() && delivery.unrepaired.is_empty(),
        "{delivery:?}"
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

fn extras() -> Vec<u8> {
    payload(ARTICLE + 9, 71)
}

fn glass() -> Vec<u8> {
    payload(6 * ARTICLE + 13, 72)
}

// `glass.extras.bin` and then `inner.7z`, a stored 7z of `glass.bin`, in a
// stored RAR5 of three volumes: the 7z starts inside the first volume and
// ends inside the third.
fn spanning() -> Vec<(String, Vec<u8>)> {
    let glass = glass();
    let inner = sevenz(&[Entry::File("glass.bin", &glass)], copy(), None, false);
    let total = ARTICLE + 9 + inner.len();
    let chunk = total.div_ceil(3);
    assert!(
        ARTICLE + 9 < chunk && total > 2 * chunk,
        "the 7z spans all three"
    );
    multi_member_store_set(&[("glass.extras.bin", extras()), ("inner.7z", inner)], 3)
}

#[tokio::test]
async fn a_stored_7z_crossing_both_volume_boundaries_of_a_three_volume_rar_delivers_under_graph() {
    let graph = run(&Scenario::new(posts(&spanning())), Engine::Graph).await;
    assert_whole(
        &graph,
        &[("glass.bin", &glass()), ("glass.extras.bin", &extras())],
    );
    assert_eq!(kinds(&graph), ["rar_store", "sevenz_store"]);
}

// The spanning set with a PAR2 set over its volumes at the root, an article
// of the middle volume, all of it the 7z's bytes, served damaged: the set
// repairs the volume and the 7z read through it delivers its member whole.
#[tokio::test]
async fn a_root_par2_set_repairs_a_volume_a_child_7z_crosses_and_the_child_delivers_under_graph() {
    let volumes = spanning();
    let described: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posted = posts(&volumes);
    posted.extend(par2_set("lantern", &described, 2));
    let graph = run(&Scenario::new(posted).damaged(1, 1), Engine::Graph).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    for (name, bytes) in [("glass.bin", glass()), ("glass.extras.bin", extras())] {
        assert!(graph.files[&delivered(name)] == bytes, "{name}");
    }
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(graph.graph().delivery.withheld.is_empty());
}

// An AES stored 7z whose only candidate is wrong ends one deep as it does at
// the root: never completed, on the password, the archive withheld, and
// the same files delivered.
#[tokio::test]
async fn an_aes_stored_7z_one_deep_offered_only_a_wrong_candidate_ends_as_at_the_root_under_graph()
{
    let vesper = payload(4 * ARTICLE + 1, 73);
    let locked = sevenz(
        &[Entry::File("vesper.bin", &vesper)],
        copy(),
        Some(SECRET),
        false,
    );
    let root = run(
        &Scenario::new(vec![Post::plain("cinder.7z", locked.clone())]).global_password(WRONG),
        Engine::Graph,
    )
    .await;
    let nested = run(
        &Scenario::new(vec![Post::plain(
            "lantern.rar",
            rar(&[("cinder.7z", locked)]),
        )])
        .global_password(WRONG),
        Engine::Graph,
    )
    .await;
    for outcome in [&root, &nested] {
        assert!(!outcome.completed(), "{:?}", outcome.status);
        assert_eq!(outcome.failure_kind, Some(HistoryFailureKind::Password));
        let withheld = &outcome.graph().delivery.withheld;
        assert_eq!(withheld.len(), 1, "{withheld:?}");
        assert_eq!(withheld[0].0, "cinder.7z");
        assert!(!outcome.files.contains_key(&delivered("vesper.bin")));
    }
    assert_eq!(nested.status, root.status);
    assert_eq!(
        nested.graph().delivery.withheld,
        root.graph().delivery.withheld
    );
    assert!(nested.files == root.files, "the files the root delivers");
}

// A compressed AES 7z under the job's password holding a stored AES 7z
// under another, a candidate: each level opens with its own password.
#[tokio::test]
async fn two_levels_under_two_passwords_each_open_with_their_own_under_graph() {
    let hollow = payload(4 * ARTICLE + 3, 74);
    let inner = sevenz(
        &[Entry::File("hollow.bin", &hollow)],
        copy(),
        Some(INNER),
        false,
    );
    let outer = sevenz(
        &[
            Entry::File("inner.7z", &inner),
            Entry::File("hollow.extras.bin", &extras()),
        ],
        lzma2(),
        Some(SECRET),
        false,
    );
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)])
        .password(SECRET)
        .global_password(INNER);
    let graph = run(&scenario, Engine::Graph).await;
    assert_whole(
        &graph,
        &[("hollow.bin", &hollow), ("hollow.extras.bin", &extras())],
    );
    assert!(
        kinds(&graph).contains(&"sevenz_engine"),
        "{:?}",
        kinds(&graph)
    );
}

// A compressed 7z with encrypted headers holding an encrypted RAR5, both
// under the job's password: the engine's output binds the RAR, and the
// password opens it too.
#[tokio::test]
async fn an_encrypted_rar5_inside_a_header_encrypted_compressed_7z_delivers_under_graph() {
    let jade = payload(4 * ARTICLE + 5, 75);
    let (_, inner) =
        encrypted_store_set("jade.bin", &jade, 1, SECRET, Some(SECRET), false).remove(0);
    let outer = sevenz(
        &[Entry::File("inner.rar", &inner)],
        lzma2(),
        Some(SECRET),
        true,
    );
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]).password(SECRET);
    let graph = run(&scenario, Engine::Graph).await;
    assert_whole(&graph, &[("jade.bin", &jade)]);
    assert!(kinds(&graph).contains(&"rar_store"), "{:?}", kinds(&graph));
}

// A compressed 7z with encrypted headers holding a member and a stored 7z,
// opened by `scenario`'s passwords: the stored 7z binds out of the
// engine's output and delivers its member.
async fn store_out_of_a_sealed_engine(scenario: impl Fn(Scenario) -> Scenario) {
    let umber = payload(4 * ARTICLE + 7, 76);
    let inner = sevenz(&[Entry::File("umber.bin", &umber)], copy(), None, false);
    let outer = sevenz(
        &[
            Entry::File("umber.extras.bin", &extras()),
            Entry::File("inner.7z", &inner),
        ],
        lzma2(),
        Some(SECRET),
        true,
    );
    let graph = run(
        &scenario(Scenario::new(vec![Post::plain("lantern.7z", outer)])),
        Engine::Graph,
    )
    .await;
    assert_whole(
        &graph,
        &[("umber.bin", &umber), ("umber.extras.bin", &extras())],
    );
    assert!(
        kinds(&graph).contains(&"sevenz_store"),
        "{:?}",
        kinds(&graph)
    );
}

#[tokio::test]
async fn a_stored_7z_inside_a_header_encrypted_compressed_7z_opened_by_the_job_password_delivers_under_graph()
 {
    store_out_of_a_sealed_engine(|scenario| scenario.password(SECRET)).await;
}

#[tokio::test]
async fn a_stored_7z_inside_a_header_encrypted_compressed_7z_opened_by_a_candidate_after_a_wrong_job_password_delivers_under_graph()
 {
    store_out_of_a_sealed_engine(|scenario| scenario.password(WRONG).global_password(SECRET)).await;
}

// An AES 7z as the one member of a header-encrypted RAR5 store, both under
// the job's password: the store deciphers the member and the 7z bound out
// of it opens on the same password.
#[tokio::test]
async fn an_aes_7z_inside_a_header_encrypted_rar5_delivers_under_graph() {
    let iron = payload(4 * ARTICLE + 11, 77);
    let inner = sevenz(
        &[Entry::File("iron.bin", &iron)],
        copy(),
        Some(SECRET),
        false,
    );
    let volumes =
        header_encrypted_store_set("inner.7z", &inner, 1, SECRET, HeaderCheck::For(SECRET));
    let graph = run(
        &Scenario::new(posts(&volumes)).password(SECRET),
        Engine::Graph,
    )
    .await;
    assert_whole(&graph, &[("iron.bin", &iron)]);
    assert_eq!(kinds(&graph), ["rar_store", "sevenz_store"]);
}

// A stored 7z one deep holding a directory with a member, an empty file and
// an empty directory: every one is delivered at depth two, the empty file
// empty and the directories as directories.
#[tokio::test]
async fn a_7z_one_deep_delivers_its_directory_empty_file_and_empty_directory_under_graph() {
    let pine = payload(4 * ARTICLE + 13, 78);
    let pack = sevenz(
        &[
            Entry::Dir("pack"),
            Entry::File("pack/pine.bin", &pine),
            Entry::File("pack/empty.marker", &[]),
            Entry::Dir("pack/hollow"),
        ],
        copy(),
        None,
        false,
    );
    let scenario = Scenario::new(vec![Post::plain("lantern.rar", rar(&[("pack.7z", pack)]))]);
    let graph = run(&scenario, Engine::Graph).await;
    assert_whole(
        &graph,
        &[("pack/empty.marker", &[]), ("pack/pine.bin", &pine)],
    );
    let delivery = &graph.graph().delivery;
    let mut whole = delivery.whole.clone();
    whole.sort();
    assert_eq!(
        whole,
        [
            "pack/",
            "pack/empty.marker",
            "pack/hollow/",
            "pack/pine.bin"
        ],
        "{delivery:?}"
    );
    assert!(
        delivery
            .files
            .iter()
            .all(|file| file.depth == 2 && file.whole),
        "{delivery:?}"
    );
}

// Two gzip streams of one name in two directories of a stored 7z: each
// decodes into its own directory and neither overwrites the other.
#[tokio::test]
async fn streams_of_one_name_in_two_directories_both_land_under_graph() {
    let first = payload(2 * ARTICLE + 1, 79);
    let second = payload(2 * ARTICLE + 1, 80);
    let (first_gz, second_gz) = (gzip(&first), gzip(&second));
    let store = sevenz(
        &[
            Entry::File("CD1/subs.srt.gz", &first_gz),
            Entry::File("CD2/subs.srt.gz", &second_gz),
        ],
        copy(),
        None,
        false,
    );
    let graph = run(
        &Scenario::new(vec![Post::plain("lantern.7z", store)]),
        Engine::Graph,
    )
    .await;
    assert_whole(
        &graph,
        &[("CD1/subs.srt", &first), ("CD2/subs.srt", &second)],
    );
}

// An xz cut to half its length inside a stored RAR: the stream ends with
// the store's bytes, so the job ends without waiting for bytes no article
// will bring, and the file it never finished is not delivered whole.
#[tokio::test]
async fn a_truncated_xz_inside_a_store_ends_the_job_without_its_file_whole_under_graph() {
    let mut compressed = xz(&payload(4 * ARTICLE + 15, 81));
    compressed.truncate(compressed.len() / 2);
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.rar",
        rar(&[("leaf.bin.xz", compressed)]),
    )]);
    let graph = run(&scenario, Engine::Graph).await;
    assert!(!graph.completed(), "{:?}", graph.status);
    let delivery = &graph.graph().delivery;
    assert!(
        !delivery.whole.iter().any(|name| name == "leaf.bin"),
        "{delivery:?}"
    );
}

// An xz member of a compressed 7z: the stream binds out of the engine's
// output and decodes beside the member next to it.
#[tokio::test]
async fn an_xz_member_of_a_compressed_7z_decodes_and_lands_under_graph() {
    let first = payload(3 * ARTICLE + 2, 82);
    let held = payload(3 * ARTICLE + 4, 83);
    let stream = xz(&held);
    let outer = sevenz(
        &[
            Entry::File("first.bin", &first),
            Entry::File("inner.xz", &stream),
        ],
        lzma2(),
        None,
        false,
    );
    let graph = run(
        &Scenario::new(vec![Post::plain("lantern.7z", outer)]),
        Engine::Graph,
    )
    .await;
    assert_whole(&graph, &[("first.bin", &first), ("inner", &held)]);
    assert!(kinds(&graph).contains(&"stream_xz"), "{:?}", kinds(&graph));
}
