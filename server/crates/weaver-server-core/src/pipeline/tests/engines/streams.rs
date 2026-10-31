// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Compressed streams and tars: one file compressed by
// each stream kind the objects read, a tar of two members, the same tar
// gzipped, and a compressed subtitle posted beside the plain one it holds.

use std::io::Write;

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_fetched_once, kinds, par2_set, payload,
    repair_and_terminal_events, run,
};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::lines::Lines;

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
    std::path::Path::new("lantern beacon")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

// The job completed and delivered exactly `expected`.
fn assert_delivered(outcome: &Outcome, expected: &[(&str, Vec<u8>)]) {
    assert!(outcome.completed(), "{:?}", outcome.failure);
    let mut names: Vec<String> = expected.iter().map(|(name, _)| delivered(name)).collect();
    names.sort();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        names,
        "delivered names",
    );
    for (name, bytes) in expected {
        assert!(outcome.files[&delivered(name)] == *bytes, "{name} bytes",);
    }
}

// One compressed post, delivered as the file it holds.
async fn one_stream(name: &str, compressed: Vec<u8>) {
    let scenario = Scenario::new(vec![Post::plain(name, compressed)]);
    let graph = run(&scenario).await;
    assert_delivered(&graph, &[("ember.bin", ember())]);
    assert_fetched_once(&graph);
}

#[tokio::test]
async fn an_xz_stream_delivers_the_file_it_holds() {
    one_stream("ember.bin.xz", xz(&ember())).await;
}

// A stream posted in numbered pieces is one stream: the first piece carries
// the signature, the others only their names, which place them after it.
#[tokio::test]
async fn an_xz_stream_split_into_numbered_pieces_delivers_the_file_it_holds() {
    let compressed = xz(&ember());
    let chunk = compressed.len().div_ceil(3);
    let pieces = compressed
        .chunks(chunk)
        .zip(1..)
        .map(|(bytes, number)| Post::plain(&format!("ember.bin.xz.{number:03}"), bytes.to_vec()))
        .collect();
    let graph = run(&Scenario::new(pieces)).await;
    assert_delivered(&graph, &[("ember.bin", ember())]);
}

fn unicode_numbered_stream(recovery: bool) -> Scenario {
    let compressed = xz(&ember());
    let chunk = compressed.len().div_ceil(3);
    let mut posts: Vec<_> = compressed
        .chunks(chunk)
        .zip(1..)
        .map(|(bytes, number)| Post::plain(&format!("灯火.bin.xz.{number:03}"), bytes.to_vec()))
        .collect();
    if recovery {
        let files: Vec<_> = posts
            .iter()
            .map(|post| (post.name.as_str(), post.bytes.as_slice()))
            .collect();
        let parity = par2_set("回復", &files, 8);
        posts.extend(parity);
    }
    Scenario::new(posts)
}

#[tokio::test]
async fn a_numbered_stream_with_non_ascii_names_is_repaired_from_a_missing_piece() {
    let mut scenario = unicode_numbered_stream(true);
    let articles = scenario.posts[1].articles(ARTICLE).count();
    for article in 0..articles {
        scenario = scenario.lost(1, u32::try_from(article).unwrap());
    }
    let graph = run(&scenario).await;
    assert_delivered(&graph, &[("灯火.bin", ember())]);
    assert_eq!(
        repair_and_terminal_events(&graph),
        [
            format!("RepairComplete {articles}"),
            "JobCompleted".to_owned()
        ]
    );
}

#[tokio::test]
async fn a_numbered_stream_with_non_ascii_names_is_repaired_from_damaged_bytes() {
    let graph = run(&unicode_numbered_stream(true).damaged(1, 1)).await;
    assert_delivered(&graph, &[("灯火.bin", ember())]);
    assert_eq!(
        repair_and_terminal_events(&graph),
        ["RepairComplete 1", "JobCompleted"]
    );
}

#[tokio::test]
async fn a_numbered_stream_missing_a_piece_without_recovery_fails() {
    let mut scenario = unicode_numbered_stream(false);
    let articles = scenario.posts[1].articles(ARTICLE).count();
    for article in 0..articles {
        scenario = scenario.lost(1, u32::try_from(article).unwrap());
    }
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert!(graph.files.is_empty());
    assert!(graph.graph().delivery.files.iter().all(|file| !file.whole));
}

#[tokio::test]
async fn a_numbered_stream_with_an_unposted_middle_piece_retains_the_original_pieces() {
    let mut scenario = unicode_numbered_stream(false);
    scenario.posts.remove(1);
    assert_unposted_middle_preserves_pieces(scenario).await;
}

#[tokio::test]
async fn an_unposted_middle_piece_cannot_publish_an_accidentally_valid_decoded_join() {
    let compressed = xz(&ember());
    let (first, last) = compressed.split_at(compressed.len() / 2);
    // Concatenating these inputs happens to make a valid complete XZ. The
    // ordinal gap still has no known length and cannot be silently erased.
    let scenario = Scenario::new(vec![
        Post::plain("灯火.bin.xz.001", first.to_vec()),
        Post::plain("灯火.bin.xz.003", last.to_vec()),
    ]);
    assert_unposted_middle_preserves_pieces(scenario).await;
}

#[tokio::test]
async fn a_numbered_stream_with_an_unposted_middle_piece_recovers_with_par2() {
    let mut scenario = unicode_numbered_stream(true);
    scenario.posts.remove(1);
    let graph = run(&scenario).await;
    assert_delivered(&graph, &[("灯火.bin", ember())]);
}

async fn assert_unposted_middle_preserves_pieces(scenario: Scenario) {
    let originals: std::collections::BTreeMap<_, _> = scenario
        .posts
        .iter()
        .map(|post| (post.name.clone(), post.bytes.clone()))
        .collect();
    let graph = run(&scenario).await;
    assert!(matches!(graph.status, Some(JobStatus::Failed { .. })));
    let listed: Vec<_> = graph
        .graph()
        .delivery
        .files
        .iter()
        .map(|file| (file.name.as_str(), file.depth, file.whole))
        .collect();
    assert_eq!(
        listed,
        [("灯火.bin.xz.001", 0, false), ("灯火.bin.xz.003", 0, false)]
    );
    for (name, bytes) in originals {
        let retained = graph
            .retained_files
            .iter()
            .find(|(path, _)| std::path::Path::new(path).file_name().unwrap() == name.as_str())
            .map(|(_, bytes)| bytes);
        assert_eq!(retained, Some(&bytes), "original piece {name} was changed");
    }
    assert_eq!(
        graph
            .retained_files
            .keys()
            .filter(|path| { std::path::Path::new(path).file_name().unwrap() != ".weaver-job-dir" })
            .count(),
        2
    );
    assert!(graph.files.is_empty(), "a decoded descendant was published");
}

// A damaged stream gives back the join's one member, preserving the posted
// compressed bytes. Nothing usable came out, so the job fails.
#[tokio::test]
async fn a_damaged_xz_split_into_numbered_pieces_fails() {
    damaged_numbered_stream("xz", damaged_in_the_middle(xz(&prose()))).await;
}

#[tokio::test]
async fn a_damaged_gzip_split_into_numbered_pieces_fails() {
    damaged_numbered_stream("gz", damaged_in_the_middle(gzip(&prose()))).await;
}

async fn damaged_numbered_stream(extension: &str, compressed: Vec<u8>) {
    let joined = format!("ember.bin.{extension}");
    let chunk = compressed.len().div_ceil(3);
    let pieces = compressed
        .chunks(chunk)
        .zip(1..)
        .map(|(bytes, number)| Post::plain(&format!("{joined}.{number:03}"), bytes.to_vec()))
        .collect();
    let graph = run(&Scenario::new(pieces)).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.files.keys().collect::<Vec<_>>()
    );
    let files = &graph.graph().delivery.files;
    let listed: Vec<(&str, u32, bool)> = files
        .iter()
        .map(|file| (file.name.as_str(), file.depth, file.whole))
        .collect();
    assert_eq!(listed, [(joined.as_str(), 1, false)], "{files:?}");
    assert!(
        graph
            .retained_files
            .values()
            .any(|bytes| *bytes == compressed)
    );
}

#[tokio::test]
async fn a_gzip_stream_delivers_the_file_it_holds() {
    one_stream("ember.bin.gz", gzip(&ember())).await;
}

#[tokio::test]
async fn a_bzip2_stream_delivers_the_file_it_holds() {
    one_stream("ember.bin.bz2", bzip2(&ember())).await;
}

#[tokio::test]
async fn a_zstd_stream_delivers_the_file_it_holds() {
    one_stream("ember.bin.zst", zstd(&ember())).await;
}

#[tokio::test]
async fn a_brotli_stream_delivers_the_file_it_holds() {
    one_stream("ember.bin.br", brotli(&ember())).await;
}

#[tokio::test]
async fn a_raw_deflate_stream_delivers_the_file_it_holds() {
    one_stream("ember.bin.deflate", deflate(&ember())).await;
}

pub(super) fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![("ember.bin", ember()), ("quill.bin", quill())]
}

#[tokio::test]
async fn a_tar_delivers_its_members() {
    let scenario = Scenario::new(vec![Post::plain("lantern.tar", tar(&members()))]);
    let graph = run(&scenario).await;
    assert_delivered(&graph, &members());
    assert_fetched_once(&graph);
}

#[tokio::test]
async fn a_gzipped_tar_delivers_its_members() {
    let scenario = Scenario::new(vec![Post::plain("lantern.tar.gz", gzip(&tar(&members())))]);
    let graph = run(&scenario).await;
    assert_delivered(&graph, &members());
    assert_fetched_once(&graph);
}

// One post cut into articles of every size from the whole file down to a
// byte (4 KiB is the whole file again): one outcome and one set of files,
// nothing is refetched, and the graph reads it through `kinds`.
async fn delivers_alike_in_every_frame_size(
    name: &str,
    bytes: Vec<u8>,
    expected: &[(&str, Vec<u8>)],
    expected_kinds: &[&str],
) {
    let mut seen = None;
    for size in [usize::MAX, 4096, 64, 1] {
        let scenario = Scenario::new(vec![Post::plain(name, bytes.clone())]).framed(size);
        let graph = run(&scenario).await;
        assert_delivered(&graph, expected);
        assert_eq!(kinds(&graph), expected_kinds, "frames of {size} bytes");
        assert_fetched_once(&graph);
        let files = seen.get_or_insert_with(|| graph.files.clone());
        assert_eq!(*files, graph.files, "frames of {size} bytes");
    }
}

async fn one_stream_in_every_frame_size(name: &str, compressed: Vec<u8>, kind: &str) {
    delivers_alike_in_every_frame_size(name, compressed, &[("ember.bin", ember())], &[kind]).await;
}

#[tokio::test]
async fn an_xz_stream_delivers_alike_in_every_frame_size() {
    one_stream_in_every_frame_size("ember.bin.xz", xz(&ember()), "stream_xz").await;
}

#[tokio::test]
async fn a_gzip_stream_delivers_alike_in_every_frame_size() {
    one_stream_in_every_frame_size("ember.bin.gz", gzip(&ember()), "stream_gz").await;
}

#[tokio::test]
async fn a_bzip2_stream_delivers_alike_in_every_frame_size() {
    one_stream_in_every_frame_size("ember.bin.bz2", bzip2(&ember()), "stream_bzip2").await;
}

#[tokio::test]
async fn a_zstd_stream_delivers_alike_in_every_frame_size() {
    one_stream_in_every_frame_size("ember.bin.zst", zstd(&ember()), "stream_zstd").await;
}

#[tokio::test]
async fn a_brotli_stream_delivers_alike_in_every_frame_size() {
    one_stream_in_every_frame_size("ember.bin.br", brotli(&ember()), "stream_brotli").await;
}

#[tokio::test]
async fn a_raw_deflate_stream_delivers_alike_in_every_frame_size() {
    one_stream_in_every_frame_size("ember.bin.deflate", deflate(&ember()), "stream_deflate").await;
}

#[tokio::test]
async fn a_tar_delivers_alike_in_every_frame_size() {
    delivers_alike_in_every_frame_size("lantern.tar", tar(&members()), &members(), &["tar"]).await;
}

#[tokio::test]
async fn a_gzipped_tar_delivers_alike_in_every_frame_size() {
    delivers_alike_in_every_frame_size(
        "lantern.tar.gz",
        gzip(&tar(&members())),
        &members(),
        &["stream_gz", "tar"],
    )
    .await;
}

// The compressed subtitle holds a different file of the plain one's name.
// Neither is written over the other.
#[tokio::test]
async fn subs_srt_beside_subs_srt_gz_delivers_both_and_overwrites_nothing() {
    let plain = payload(2 * ARTICLE + 5, 63);
    let inner = payload(3 * ARTICLE + 9, 64);
    let scenario = Scenario::new(vec![
        Post::plain("subs.srt", plain.clone()),
        Post::plain("subs.srt.gz", gzip(&inner)),
    ]);
    let graph = run(&scenario).await;
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
    let graph = run(&Scenario::new(posts).damaged(0, 0)).await;
    assert!(
        graph.failure.as_deref().is_some_and(
            |error| error.starts_with("PAR2 recovery failed for 1 set(s): tinder.par2")
        ),
        "{:?}",
        graph.status
    );
    // The set's own file is short because the set could not rebuild it:
    // the repair is the stage that left the job short.
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    // A failed job ships nothing; the graph's own claim keeps the damage.
    assert_eq!(graph.claims.get("ember.bin.xz"), Some(&Claim::Missing));
    assert_eq!(
        graph.graph().claims.get("ember.bin.xz"),
        Some(&Claim::Damaged)
    );
}

// A tar holding an empty directory ahead of a member.
fn tar_with_empty_directory(member: &[u8]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    let mut directory = tar::Header::new_gnu();
    directory.set_entry_type(tar::EntryType::Directory);
    directory.set_size(0);
    directory.set_mode(0o755);
    directory.set_cksum();
    builder
        .append_data(&mut directory, "hollow/", std::io::empty())
        .unwrap();
    let mut header = tar::Header::new_gnu();
    header.set_size(member.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder
        .append_data(&mut header, "ember.bin", member)
        .unwrap();
    builder.into_inner().unwrap()
}

#[tokio::test]
async fn a_tar_holding_an_empty_directory_delivers_it() {
    let member = members()[0].1.clone();
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.tar",
        tar_with_empty_directory(&member),
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

// A compressed stream states no checksum of the file it holds, so the
// naming plan has none for it.
#[tokio::test]
async fn a_stream_member_gives_the_naming_plan_no_checksum() {
    let graph = run(&Scenario::new(vec![Post::plain(
        "ember.bin.xz",
        xz(&ember()),
    )]))
    .await;
    assert_delivered(&graph, &[("ember.bin", ember())]);
    let files = &graph.graph().delivery.files;
    assert!(
        files.iter().any(|file| file.name.ends_with("ember.bin")),
        "{files:?}"
    );
    assert!(files.iter().all(|file| file.crc32.is_none()), "{files:?}");
    assert!(crate::pipeline::completion::finalize::member_crc32_by_path(files).is_empty());
}

// A single file posted under `name` with a compressed stream's or a tar's
// name and no recovery, whose bytes all arrived and which nothing opened: it
// is no stream, so it is a file as posted, the delivery, whole and
// unmarked, and the job completes with it and with nothing unpacked from it.
async fn assert_a_single_stream_completes_as_posted(name: &str, bytes: Vec<u8>) {
    let graph = run(&Scenario::new(vec![Post::plain(name, bytes.clone())])).await;
    assert!(
        graph.completed(),
        "{name}: {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.files.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        [delivered(name)],
        "{name}: delivered names"
    );
    assert!(graph.files[&delivered(name)] == bytes, "{name}: bytes");
    let files = &graph.graph().delivery.files;
    assert!(
        files.len() == 1 && files[0].depth == 0 && files[0].whole,
        "{name}: {files:?}"
    );
    assert_eq!(
        graph.graph().claims.get(name),
        Some(&Claim::Complete),
        "{name}: the claim"
    );
}

// A single compressed stream or tar posted under `name` with no recovery,
// whose bytes all arrived and which opened and came out damaged: the
// object refuses it for its own reason, so the posted file is listed
// damaged, and with nothing usable the job fails.
async fn assert_a_single_damaged_stream_fails(name: &str, bytes: Vec<u8>) {
    let graph = run(&Scenario::new(vec![Post::plain(name, bytes)])).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{name}: {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.files.keys().collect::<Vec<_>>()
    );
    let files = &graph.graph().delivery.files;
    assert!(
        files.len() == 1 && files[0].name.ends_with(name) && files[0].depth == 0 && !files[0].whole,
        "{name}: {files:?}"
    );
}

fn unopened() -> Vec<u8> {
    payload(6 * ARTICLE + 7, 63)
}

// Text a compressor codes rather than stores, so damage to it lands in
// coded data the decoder reads.
pub(super) fn prose() -> Vec<u8> {
    b"a lantern by the quill, an ember in the beacon; "
        .iter()
        .copied()
        .cycle()
        .take(12 * ARTICLE + 9)
        .collect()
}

// The bytes with a run in their middle arrived wrong under a good article
// checksum: the signature still opens them and the damage lies past it.
pub(super) fn damaged_in_the_middle(mut bytes: Vec<u8>) -> Vec<u8> {
    let at = bytes.len() / 2;
    for byte in &mut bytes[at..at + 16] {
        *byte ^= 0x5a;
    }
    bytes
}

// A tar whose one header arrived wrong under a good article checksum: the
// ustar magic still opens it, and the header's own checksum fails.
fn tar_with_a_damaged_header() -> Vec<u8> {
    let mut bytes = tar(&[("lantern.mkv", prose())]);
    bytes[0] ^= 0x5a;
    bytes
}

// The tar's own header checksum marks nothing in the posted file: the walk
// is refused, the tar listed damaged, and with nothing usable the job fails.
#[tokio::test]
async fn a_single_tar_whose_header_is_damaged_fails() {
    assert_a_single_damaged_stream_fails("lantern.tar", tar_with_a_damaged_header()).await;
}

// An xz whose one block arrived with its check wrong under a good article
// checksum: the block decodes and its check fails. The check speaks of what
// the stream reads out and marks nothing in the posted file: the stream is
// refused, listed damaged, and with nothing usable the job fails.
#[tokio::test]
async fn a_single_xz_whose_block_check_fails_fails() {
    let mut bytes = xz(&prose());
    assert_ne!(bytes[7] & 0x0f, 0, "the stream carries a block check");
    let backward = u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
    let index = bytes.len() - 12 - (backward as usize + 1) * 4;
    bytes[index - 1] ^= 0x01;
    assert_a_single_damaged_stream_fails("lantern.mkv.xz", bytes).await;
}

// A tar of two entries whose second header arrived wrong under a good
// article checksum, with no recovery: the walk reads the first entry whole,
// holds at the second header, and refuses the tar once nothing more will
// arrive. What came out whole stays delivered, beside the tar listed
// damaged, and the job ends Partial.
#[tokio::test]
async fn a_tar_whose_second_header_is_damaged_is_partial_with_its_first_entry() {
    let first = prose();
    let mut bytes = tar(&[("lantern.mkv", first.clone()), ("ember.bin", ember())]);
    let second = 512 + first.len().div_ceil(512) * 512;
    bytes[second] ^= 0x5a;
    let (lines, guard) = Lines::capture();
    let graph = run(&Scenario::new(vec![Post::plain(
        "lantern.tar",
        bytes.clone(),
    )]))
    .await;
    drop(guard);
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?} {:?}",
        graph.failure,
        graph.files.keys().collect::<Vec<_>>()
    );
    let refused = lines.of("object refused");
    assert!(
        refused.len() == 1 && refused[0].contains("kind=tar member=lantern.tar why=unrepairable"),
        "{refused:?}"
    );
    assert!(
        graph.files[&delivered("lantern.mkv")] == first,
        "lantern.mkv bytes"
    );
    let files = &graph.graph().delivery.files;
    let depth_of = |name: &str| {
        files
            .iter()
            .find(|file| file.name.ends_with(name))
            .map(|file| (file.depth, file.whole))
    };
    assert_eq!(depth_of("lantern.mkv"), Some((1, true)), "{files:?}");
    assert_eq!(depth_of("lantern.tar"), Some((0, false)), "{files:?}");
}

// The same damaged tar under a PAR2 set made from the tar as it should be:
// the set's own verification finds the posted file wrong and repairs it to
// the original, with no mark of the tar's to drive it, and the tar then
// walks whole.
#[tokio::test]
async fn a_tar_whose_header_is_damaged_under_par2_is_repaired_to_the_original() {
    let original = tar(&[("lantern.mkv", prose())]);
    let mut posts = vec![Post::plain("lantern.tar", tar_with_a_damaged_header())];
    posts.extend(par2_set("lantern", &[("lantern.tar", &original)], 2));
    let graph = run(&Scenario::new(posts)).await;
    assert!(
        graph.completed(),
        "{:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.files.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        repair_and_terminal_events(&graph),
        ["RepairComplete 1", "JobCompleted"]
    );
    assert_eq!(
        graph.graph().claims.get("lantern.tar"),
        Some(&Claim::Complete)
    );
    assert_delivered(&graph, &[("lantern.mkv", prose())]);
}

#[tokio::test]
async fn a_single_damaged_tar_gz_fails() {
    let bytes = damaged_in_the_middle(gzip(&tar(&[("lantern.mkv", prose())])));
    assert_a_single_damaged_stream_fails("lantern.tar.gz", bytes).await;
}

#[tokio::test]
async fn a_single_damaged_tar_bz2_fails() {
    let bytes = damaged_in_the_middle(bzip2(&tar(&[("lantern.mkv", prose())])));
    assert_a_single_damaged_stream_fails("lantern.tar.bz2", bytes).await;
}

#[tokio::test]
async fn a_single_damaged_tar_xz_fails() {
    let bytes = damaged_in_the_middle(xz(&tar(&[("lantern.mkv", prose())])));
    assert_a_single_damaged_stream_fails("lantern.tar.xz", bytes).await;
}

#[tokio::test]
async fn a_single_damaged_gz_fails() {
    let bytes = damaged_in_the_middle(gzip(&prose()));
    assert_a_single_damaged_stream_fails("lantern.mkv.gz", bytes).await;
}

#[tokio::test]
async fn a_single_damaged_deflate_fails() {
    let bytes = damaged_in_the_middle(deflate(&prose()));
    assert_a_single_damaged_stream_fails("lantern.mkv.deflate", bytes).await;
}

#[tokio::test]
async fn a_single_damaged_brotli_fails() {
    let bytes = damaged_in_the_middle(brotli(&prose()));
    assert_a_single_damaged_stream_fails("lantern.mkv.br", bytes).await;
}

// A zstd frame carrying its content checksum, as the command-line tool
// writes it: a frame without one decodes damaged bytes as if whole.
fn zstd_checked(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = zstd::stream::Encoder::new(Vec::new(), 1).unwrap();
    encoder.include_checksum(true).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn a_single_damaged_zstd_fails() {
    let bytes = damaged_in_the_middle(zstd_checked(&prose()));
    assert_a_single_damaged_stream_fails("lantern.mkv.zst", bytes).await;
}

#[tokio::test]
async fn a_single_damaged_bzip2_fails() {
    let bytes = damaged_in_the_middle(bzip2(&prose()));
    assert_a_single_damaged_stream_fails("lantern.mkv.bz2", bytes).await;
}

#[tokio::test]
async fn a_single_damaged_xz_fails() {
    let bytes = damaged_in_the_middle(xz(&prose()));
    assert_a_single_damaged_stream_fails("lantern.mkv.xz", bytes).await;
}

#[tokio::test]
async fn an_unopened_single_tar_gz_completes_as_posted() {
    assert_a_single_stream_completes_as_posted("lantern.tar.gz", unopened()).await;
}

#[tokio::test]
async fn an_unopened_single_tar_xz_completes_as_posted() {
    assert_a_single_stream_completes_as_posted("lantern.tar.xz", unopened()).await;
}

#[tokio::test]
async fn an_unopened_single_gz_completes_as_posted() {
    assert_a_single_stream_completes_as_posted("lantern.mkv.gz", unopened()).await;
}

#[tokio::test]
async fn an_unopened_single_zstd_completes_as_posted() {
    assert_a_single_stream_completes_as_posted("lantern.mkv.zst", unopened()).await;
}

#[tokio::test]
async fn an_unopened_single_xz_completes_as_posted() {
    assert_a_single_stream_completes_as_posted("lantern.mkv.xz", unopened()).await;
}
