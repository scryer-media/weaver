// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Compressed streams posted whole, every article clean, in the shapes a
// stream breaks or surprises in at its end: cut short by eight bytes, its
// trailer changed, and two streams of one kind concatenated in one file.
// A stream that cannot be read to a checked end is damaged, and a damaged
// stream fails the job, delivered as posted; concatenated streams decode to
// every byte of each.

use super::harness::{Outcome, Post, Scenario, run};
use super::streams::{brotli, bzip2, deflate, gzip, prose, tar, xz, zstd};
use crate::jobs::model::JobStatus;

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// The job failed, the stream posted delivered as it came, not whole.
fn assert_failed_damaged(outcome: &Outcome, name: &str) {
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{name}: {:?} {:?} {:?}",
        outcome.status,
        outcome.failure,
        outcome.files.keys().collect::<Vec<_>>()
    );
    let files = &outcome.graph().delivery.files;
    assert!(
        files
            .iter()
            .any(|file| file.name.ends_with(name) && file.depth == 0 && !file.whole),
        "{name}: {files:?}"
    );
    assert!(
        files.iter().all(|file| file.depth == 0),
        "{name}: nothing comes out of a stream that never ends: {files:?}"
    );
}

async fn cut_short(name: &str, mut bytes: Vec<u8>) {
    bytes.truncate(bytes.len() - 8);
    let graph = run(&Scenario::new(vec![Post::plain(name, bytes)])).await;
    assert_failed_damaged(&graph, name);
}

#[tokio::test]
async fn a_deflate_stream_cut_short_fails() {
    cut_short("lantern.mkv.deflate", deflate(&prose())).await;
}

#[tokio::test]
async fn a_gzip_stream_cut_short_fails() {
    cut_short("lantern.mkv.gz", gzip(&prose())).await;
}

#[tokio::test]
async fn a_bzip2_stream_cut_short_fails() {
    cut_short("lantern.mkv.bz2", bzip2(&prose())).await;
}

#[tokio::test]
async fn an_xz_stream_cut_short_fails() {
    cut_short("lantern.mkv.xz", xz(&prose())).await;
}

#[tokio::test]
async fn a_brotli_stream_cut_short_fails() {
    cut_short("lantern.mkv.br", brotli(&prose())).await;
}

#[tokio::test]
async fn a_zstd_stream_cut_short_fails() {
    cut_short("lantern.mkv.zst", zstd(&prose())).await;
}

// A tarred stream cut short: the stream never reaches its checked end, so
// whatever entry the tar yields before the cut, the job does not complete.
// Whether a whole entry ahead of the cut makes it Partial (a damaged tar
// that delivered a member) or the stream's damage fails it is open; SABnzbd
// fails a tar it cannot read through (sabnzbd/newsunpack.py:1160-1162), so
// the assertion is that it is never Complete.
async fn tarred_cut_short(name: &str, compress: fn(&[u8]) -> Vec<u8>) {
    let mut bytes = compress(&tar(&[("lantern.mkv", prose())]));
    bytes.truncate(bytes.len() - 8);
    let graph = run(&Scenario::new(vec![Post::plain(name, bytes)])).await;
    assert!(
        matches!(
            graph.status,
            Some(JobStatus::Failed { .. } | JobStatus::Partial)
        ),
        "{name}: {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.files.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_gzipped_tar_cut_short_never_completes() {
    tarred_cut_short("lantern.tar.gz", gzip).await;
}

#[tokio::test]
async fn a_bzipped_tar_cut_short_never_completes() {
    tarred_cut_short("lantern.tar.bz2", bzip2).await;
}

#[tokio::test]
async fn an_xzipped_tar_cut_short_never_completes() {
    tarred_cut_short("lantern.tar.xz", xz).await;
}

// The fifth byte from the end changed: in a gzip member that is its CRC32,
// in a bzip2 stream its combined checksum, in an xz stream its footer.
async fn trailer_changed(name: &str, mut bytes: Vec<u8>) {
    let at = bytes.len() - 5;
    bytes[at] ^= 0x7f;
    let graph = run(&Scenario::new(vec![Post::plain(name, bytes)])).await;
    assert_failed_damaged(&graph, name);
}

#[tokio::test]
async fn a_gzip_stream_whose_trailer_changed_fails() {
    trailer_changed("lantern.mkv.gz", gzip(&prose())).await;
}

#[tokio::test]
async fn a_bzip2_stream_whose_trailer_changed_fails() {
    trailer_changed("lantern.mkv.bz2", bzip2(&prose())).await;
}

#[tokio::test]
async fn an_xz_stream_whose_trailer_changed_fails() {
    trailer_changed("lantern.mkv.xz", xz(&prose())).await;
}

// Two streams of one kind back to back in one file decode to the bytes of
// each, in order, as every one of these formats defines it.
async fn concatenated(name: &str, compress: fn(&[u8]) -> Vec<u8>) {
    let mut bytes = compress(&prose());
    bytes.extend_from_within(..);
    let mut expected = prose();
    expected.extend_from_within(..);
    let graph = run(&Scenario::new(vec![Post::plain(name, bytes)])).await;
    assert!(
        graph.completed(),
        "{name}: {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.files.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("lantern.mkv")],
        "{name}: delivered names"
    );
    assert!(
        graph.files[&delivered("lantern.mkv")] == expected,
        "{name}: every byte of both streams, {} of {}",
        graph.files[&delivered("lantern.mkv")].len(),
        expected.len()
    );
}

#[tokio::test]
async fn two_concatenated_gzip_members_decode_to_both() {
    concatenated("lantern.mkv.gz", gzip).await;
}

#[tokio::test]
async fn two_concatenated_bzip2_streams_decode_to_both() {
    concatenated("lantern.mkv.bz2", bzip2).await;
}

#[tokio::test]
async fn two_concatenated_xz_streams_decode_to_both() {
    concatenated("lantern.mkv.xz", xz).await;
}

#[tokio::test]
async fn two_concatenated_zstd_frames_decode_to_both() {
    concatenated("lantern.mkv.zst", zstd).await;
}
