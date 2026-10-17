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

use super::harness::{ARTICLE, Outcome, Post, Scenario, assert_same, par2_set, payload, run_both};

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
    // Graph completes with only `lantern.7z.003` delivered raw: the
    // continuation is sniffed a leaf before `.001` binds the store.
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
