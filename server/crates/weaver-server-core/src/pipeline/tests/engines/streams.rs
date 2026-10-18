// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Compressed streams and tars under both engines: one file compressed by
// each stream kind the objects read, a tar of two members, the same tar
// gzipped, and a compressed subtitle posted beside the plain one it holds.

use std::io::Write;

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_same, par2_set, payload, run, run_both,
};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::Engine;

fn ember() -> Vec<u8> {
    payload(9 * ARTICLE + 21, 61)
}

fn quill() -> Vec<u8> {
    payload(4 * ARTICLE - 3, 62)
}

pub(super) fn xz(bytes: &[u8]) -> Vec<u8> {
    let props = lzma_turbo::LzmaEncProps::xz_preset(0, false).unwrap();
    let mut encoder = lzma_turbo::XzWriter::new(Vec::new(), &props).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

pub(super) fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

pub(super) fn deflate(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

pub(super) fn bzip2(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

pub(super) fn zstd(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(bytes, 1).unwrap()
}

pub(super) fn brotli(bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    {
        let mut encoder = brotli::CompressorWriter::new(&mut output, 4096, 1, 18);
        encoder.write_all(bytes).unwrap();
    }
    output
}

pub(super) fn tar(members: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (name, bytes) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, name, bytes.as_slice())
            .unwrap();
    }
    builder.into_inner().unwrap()
}

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// The job completed and delivered exactly `expected`.
fn assert_delivered(outcome: &Outcome, expected: &[(&str, Vec<u8>)]) {
    assert!(
        outcome.completed(),
        "{}: {:?}",
        outcome.engine,
        outcome.failure
    );
    let mut names: Vec<String> = expected.iter().map(|(name, _)| delivered(name)).collect();
    names.sort();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        names,
        "{}: delivered names",
        outcome.engine
    );
    for (name, bytes) in expected {
        assert!(
            outcome.files[&delivered(name)] == *bytes,
            "{}: {name} bytes",
            outcome.engine
        );
    }
}

// One compressed post, delivered as the file it holds under both engines.
async fn one_stream(name: &str, compressed: Vec<u8>) {
    let scenario = Scenario::new(vec![Post::plain(name, compressed)]);
    let (classic, graph) = run_both(&scenario).await;
    assert_delivered(&graph, &[("ember.bin", ember())]);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn an_xz_stream_delivers_the_file_it_holds_under_both_engines() {
    one_stream("ember.bin.xz", xz(&ember())).await;
}

#[tokio::test]
async fn a_gzip_stream_delivers_the_file_it_holds_under_both_engines() {
    one_stream("ember.bin.gz", gzip(&ember())).await;
}

#[tokio::test]
async fn a_bzip2_stream_delivers_the_file_it_holds_under_both_engines() {
    one_stream("ember.bin.bz2", bzip2(&ember())).await;
}

#[tokio::test]
async fn a_zstd_stream_delivers_the_file_it_holds_under_both_engines() {
    one_stream("ember.bin.zst", zstd(&ember())).await;
}

#[tokio::test]
async fn a_brotli_stream_delivers_the_file_it_holds_under_both_engines() {
    one_stream("ember.bin.br", brotli(&ember())).await;
}

#[tokio::test]
async fn a_raw_deflate_stream_delivers_the_file_it_holds_under_both_engines() {
    one_stream("ember.bin.deflate", deflate(&ember())).await;
}

pub(super) fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![("ember.bin", ember()), ("quill.bin", quill())]
}

#[tokio::test]
async fn a_tar_delivers_its_members_under_both_engines() {
    let scenario = Scenario::new(vec![Post::plain("lantern.tar", tar(&members()))]);
    let (classic, graph) = run_both(&scenario).await;
    assert_delivered(&graph, &members());
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn a_gzipped_tar_delivers_its_members_under_both_engines() {
    let scenario = Scenario::new(vec![Post::plain("lantern.tar.gz", gzip(&tar(&members())))]);
    let (classic, graph) = run_both(&scenario).await;
    assert_delivered(&graph, &members());
    assert_same(&classic, &graph);
}

// The compressed subtitle holds a different file of the plain one's name.
// Neither is written over the other. Classic writes the inner file over the
// plain one; so does graph for now, since each namespace allocates its own
// members' names while every namespace's leaves share the job's folder.
#[tokio::test]
#[ignore = "graph allocates output names per namespace, not across the tree"]
async fn subs_srt_beside_subs_srt_gz_delivers_both_and_overwrites_nothing() {
    let plain = payload(2 * ARTICLE + 5, 63);
    let inner = payload(3 * ARTICLE + 9, 64);
    let scenario = Scenario::new(vec![
        Post::plain("subs.srt", plain.clone()),
        Post::plain("subs.srt.gz", gzip(&inner)),
    ]);
    let classic = run(&scenario, Engine::Classic).await;
    assert_delivered(&classic, &[("subs.srt", inner.clone())]);
    let graph = run(&scenario, Engine::Graph).await;
    assert_delivered(&graph, &[("subs.srt", plain), ("subs (1).srt", inner)]);
}

// A PAR2 set that describes only a file never posted covers nothing here,
// and an xz whose first article arrived damaged has no recovery behind it.
// The set rules as soon as its volumes are in, so nothing waits on it: the
// job ends with the xz short as it stands, its stream header unreadable, and
// with nothing whole it fails for unpacking, carrying the set's repair
// failure. The file the set describes was never rebuilt, so it is no file;
// the graph's claim on the xz is what arrived of it.
#[tokio::test]
async fn an_xz_damaged_at_its_front_beside_a_par2_set_covering_nothing_ends_without_waiting() {
    let other = payload(2 * ARTICLE + 7, 65);
    let mut posts = vec![Post::plain("ember.bin.xz", xz(&ember()))];
    posts.extend(par2_set("tinder", &[("tinder.bin", &other)], 2));
    let graph = run(&Scenario::new(posts).damaged(0, 0), Engine::Graph).await;
    assert!(
        graph.failure.as_deref().is_some_and(
            |error| error.starts_with("PAR2 recovery failed for 1 set(s): tinder.par2")
        ),
        "{:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    // A failed job ships nothing; the graph's own claim keeps the damage.
    assert_eq!(graph.claims.get("ember.bin.xz"), Some(&Claim::Missing));
    assert_eq!(
        graph.graph().claims.get("ember.bin.xz"),
        Some(&Claim::Damaged)
    );
}
