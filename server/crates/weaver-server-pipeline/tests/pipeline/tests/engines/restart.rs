// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeSet;

use super::harness::{ARTICLE, Post, Scenario, payload, run};
use super::sfv::{crc, listing};
use super::streams::{brotli, bzip2, gzip, members, tar, zstd};
use super::zip;
pub(super) use weaver_server_pipeline::pipeline::tests::engines::restart_fixtures::{
    Down, assert_restarted, restart_row, run_restarting, run_restarting_at,
};
use weaver_server_pipeline::pipeline::tests::engines::restart_fixtures::{
    SECRET, with_par2, with_par3,
};

#[tokio::test]
async fn a_plain_file_under_par2_restarted_after_every_article_is_repaired_with_no_refetch() {
    let posts = with_par2(
        vec![Post::plain("ember.bin", payload(9 * ARTICLE + 21, 71))],
        2,
    );
    restart_row(Scenario::new(posts).damaged(0, 3)).await;
}

// A uuencoded article says nothing of where its bytes go: each is laid
// after the one before it. A restart must leave the restored job placing
// every article still to come where the uninterrupted run placed it.
#[tokio::test]
async fn a_uuencoded_file_restarted_after_every_article_is_laid_as_it_was() {
    let posts = vec![Post::plain("ember.bin", payload(6 * ARTICLE + 21, 73))];
    restart_row(Scenario::new(posts).uuencoded()).await;
}

#[tokio::test]
async fn a_uuencoded_file_under_par2_restarted_after_every_article_is_repaired_with_no_refetch() {
    let posts = with_par2(
        vec![Post::plain("ember.bin", payload(6 * ARTICLE + 21, 73))],
        2,
    );
    restart_row(Scenario::new(posts).uuencoded().damaged(0, 3)).await;
}

// An article every server failed before a restart is passed by the restored
// cursor as the uninterrupted run passed it. Every part behind the hole is
// laid one article early, so the set carries enough blocks to mend them all.
#[tokio::test]
async fn a_uuencoded_file_under_par2_with_a_lost_article_restarted_after_every_article_is_repaired()
{
    let posts = with_par2(
        vec![Post::plain("ember.bin", payload(6 * ARTICLE + 21, 73))],
        8,
    );
    restart_row(Scenario::new(posts).uuencoded().lost(0, 2)).await;
}

// A plain file cut shorter than its checkpoint says while the pipeline is
// down: the restore finds that from the file's length alone, the articles
// past the cut and no other come again, and every file is delivered whole.
#[tokio::test]
async fn a_plain_file_cut_short_at_a_restart_fetches_again_only_what_it_lost() {
    let posts = vec![
        Post::plain("ember.bin", payload(9 * ARTICLE + 21, 71)),
        Post::plain("quill.bin", payload(7 * ARTICLE + 5, 72)),
    ];
    let order: Vec<(u32, u32)> = (0..10).map(|ordinal| (0, ordinal)).collect();
    let scenario = Scenario::new(posts).first(&order);
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let cut = 4 * ARTICLE as u64 + 10;
    let down = order.len();
    let restarted = run_restarting_at(
        &scenario,
        &mut |_| {},
        Down::Cutting("ember.bin", cut),
        &|nth| nth == down,
    )
    .await;
    assert_eq!(restarted.restarts, 1);
    assert_eq!(restarted.status, whole.status, "status");
    assert_eq!(
        restarted.files.keys().collect::<Vec<_>>(),
        whole.files.keys().collect::<Vec<_>>()
    );
    for (name, bytes) in &whole.files {
        assert!(restarted.files[name] == *bytes, "{name}: delivered bytes");
    }
    let lost: BTreeSet<(u32, u32)> = (4..10)
        .map(|ordinal| (0, ordinal))
        .filter(|article| !restarted.in_memory.contains(article))
        .collect();
    assert!(lost.contains(&(0, 4)), "the cut article was on disk");
    assert_eq!(
        restarted.from_disk, lost,
        "the articles past the cut, and no other, come again"
    );
}

#[tokio::test]
async fn a_tar_restarted_after_every_article_delivers_its_members() {
    restart_row(Scenario::new(vec![Post::plain(
        "lantern.tar",
        tar(&members()),
    )]))
    .await;
}

#[tokio::test]
async fn a_gzipped_tar_restarted_after_every_article_replays_from_its_start() {
    restart_row(Scenario::new(vec![Post::plain(
        "lantern.tar.gz",
        gzip(&tar(&members())),
    )]))
    .await;
}

// A stream object keeps how far its input was consumed; its input stays in
// its partial, so a restart replays the stream from its start off disk.
async fn stream_row(name: &str, compress: fn(&[u8]) -> Vec<u8>) {
    let body = compress(&payload(30 * ARTICLE + 7, 80));
    restart_row(Scenario::new(vec![Post::plain(name, body)])).await;
}

#[tokio::test]
async fn a_gzip_stream_restarted_after_every_article_replays_from_its_start() {
    stream_row("ember.bin.gz", gzip).await;
}

// A stream restored mid-download decodes again from its start on its run's
// thread, not in the restore: the restored job hands out the articles still
// to come while that run is out.
#[tokio::test]
async fn a_gzip_stream_restored_mid_download_fetches_while_it_decodes_again() {
    let body = gzip(&payload(30 * ARTICLE + 7, 80));
    let scenario = Scenario::new(vec![Post::plain("ember.bin.gz", body)]);
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let restarted = run_restarting(&scenario).await;
    assert!(
        restarted.graph.fetching_while_running > 0,
        "a restored job fetches while its run decodes again"
    );
    assert_restarted(&restarted, &whole);
}

#[tokio::test]
async fn a_bzip2_stream_restarted_after_every_article_replays_from_its_start() {
    stream_row("ember.bin.bz2", bzip2).await;
}

#[tokio::test]
async fn a_zstd_stream_restarted_after_every_article_replays_from_its_start() {
    stream_row("ember.bin.zst", zstd).await;
}

#[tokio::test]
async fn a_brotli_stream_restarted_after_every_article_replays_from_its_start() {
    stream_row("ember.bin.br", brotli).await;
}

#[tokio::test]
async fn plain_files_beside_an_sfv_restarted_after_every_article_verify_with_no_refetch() {
    let lantern = payload(5 * ARTICLE, 81);
    let beacon = payload(3 * ARTICLE - 5, 82);
    restart_row(Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
        listing(
            "lantern.sfv",
            &[("lantern.bin", crc(&lantern)), ("beacon.bin", crc(&beacon))],
        ),
    ]))
    .await;
}

#[tokio::test]
async fn plain_files_under_par3_restarted_after_every_article_are_repaired_with_no_refetch() {
    let files = [
        ("lantern.bin", payload(6 * ARTICLE, 83)),
        ("beacon.bin", payload(3 * ARTICLE + 9, 84)),
    ];
    let posts = with_par3(&files, &["lantern.bin", "beacon.bin"], "lantern");
    restart_row(Scenario::new(posts).damaged(0, 2)).await;
}

// Every file is given its declared length when it is made, on whatever
// filesystem the test runs: a file's length then says nothing of how far its
// bytes reach. Articles a restored member holds in memory, undecided under
// its set, are in no file, and each restart fetches them again from what the
// checkpoint says alone. One that took the file's length for them would
// leave holes of zeros where they lay, more than the set's blocks mend.
#[tokio::test]
async fn articles_held_in_memory_over_a_sized_file_are_fetched_again_after_every_restart() {
    let _sized = weaver_server_disk::pipeline::graph::sparse::sizing::always();
    let files = [
        ("lantern.bin", payload(6 * ARTICLE, 83)),
        ("beacon.bin", payload(3 * ARTICLE + 9, 84)),
    ];
    let posts = with_par3(&files, &["lantern.bin", "beacon.bin"], "lantern");
    restart_row(Scenario::new(posts).damaged(0, 2)).await;
}

// No file is given a length before its bytes, so each is as long as its
// last byte written. The second file is small enough to be decided at its
// first article: its first and last articles come first and go to its
// file, which then reaches its end. Every article between them comes after
// a restart, while the member is undecided under its set, and is held in
// memory below the file's length. Each restart fetches those again: the
// file's length covers them, and the checkpoint does not.
#[tokio::test]
async fn articles_held_in_memory_below_a_files_length_are_fetched_again_after_every_restart() {
    let _grown = weaver_server_disk::pipeline::graph::sparse::sizing::never();
    let files = [
        ("lantern.bin", payload(6 * ARTICLE, 83)),
        ("beacon.bin", payload(3 * ARTICLE + 9, 84)),
    ];
    let posts = with_par3(&files, &["lantern.bin", "beacon.bin"], "lantern");
    // Sixteen bytes an article: the second file is thirteen of them.
    let scenario = Scenario::new(posts).framed(16).first(&[(1, 0), (1, 12)]);
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let restarted = run_restarting(&scenario).await;
    assert_restarted(&restarted, &whole);
    assert!(
        (1..12).any(|ordinal| restarted.in_memory.contains(&(1, ordinal))),
        "some article between the file's ends was held in memory at a restart: {:?}",
        restarted.in_memory
    );
}

// The members are bound by their bytes, so the restored set must still
// know what it identified: a lost match would read them back again, and a
// lost rename would deliver them under their hex names.
#[tokio::test]
async fn obfuscated_members_under_par3_restarted_after_every_article_are_renamed_with_no_refetch() {
    let files = [
        ("lantern.bin", payload(5 * ARTICLE, 85)),
        ("beacon.bin", payload(2 * ARTICLE + 7, 86)),
    ];
    let posts = with_par3(
        &files,
        &["3f9a0c71e25b84d6", "a07d5e19c4b26f83"],
        "e81b6d3f0a4c9725",
    );
    restart_row(Scenario::new(posts).damaged(0, 1)).await;
}

// One stored member in a zip of its own.
fn one_member_zip() -> Vec<u8> {
    let mut writer = ::zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer.start_file("ember.bin", zip::stored()).unwrap();
    std::io::Write::write_all(&mut writer, &payload(9 * ARTICLE + 5, 87)).unwrap();
    writer.finish().unwrap().into_inner()
}

#[tokio::test]
async fn a_stored_zip_of_one_member_restarted_after_every_article_delivers_it() {
    restart_row(Scenario::new(vec![Post::plain(
        "lantern.zip",
        one_member_zip(),
    )]))
    .await;
}

// A ZipCrypto member's ciphertext is deciphered on the pool while its zip is
// lent out; a restart while it is out, or before, delivers it all the same.
fn zipcrypto() -> ::zip::write::SimpleFileOptions {
    use ::zip::unstable::write::FileOptionsExt;
    zip::stored()
        .with_deprecated_encryption(SECRET.as_bytes())
        .unwrap()
}

#[tokio::test]
async fn a_zipcrypto_zip_of_one_member_restarted_after_every_article_delivers_it() {
    let mut writer = ::zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer.start_file("ember.bin", zipcrypto()).unwrap();
    std::io::Write::write_all(&mut writer, &payload(9 * ARTICLE + 5, 88)).unwrap();
    let bytes = writer.finish().unwrap().into_inner();
    restart_row(Scenario::new(vec![Post::plain("lantern.zip", bytes)]).password(SECRET)).await;
}

#[tokio::test]
async fn a_zipcrypto_zip_under_par2_restarted_after_every_article_is_repaired_with_no_refetch() {
    let posts = with_par2(
        vec![Post::plain("lantern.zip", zip::archive(zipcrypto()))],
        2,
    );
    restart_row(Scenario::new(posts).damaged(0, 2).password(SECRET)).await;
}

#[tokio::test]
async fn a_zipcrypto_zip_spanned_restarted_after_every_article_delivers_its_members() {
    restart_row(Scenario::new(zip::spanned(&zip::archive(zipcrypto()), 3)).password(SECRET)).await;
}

// The engine's inflate run replays the member's stream from its start over
// the partial on disk, so a restart inside the run re-reads and never fetches.
#[tokio::test]
async fn a_deflate_zip_restarted_after_every_article_replays_the_engine_from_disk() {
    restart_row(Scenario::new(vec![Post::plain(
        "lantern.zip",
        zip::deflated(),
    )]))
    .await;
}

// The store knows the set from the names it bound and each volume's length;
// a restored store that lost either would place no byte past the first volume.
#[tokio::test]
async fn a_stored_zip_spanned_over_z01_z02_zip_restarted_after_every_article_delivers_its_members()
{
    restart_row(Scenario::new(zip::spanned(&zip::archive(zip::stored()), 3))).await;
}

#[tokio::test]
async fn a_stored_zip_under_par2_restarted_after_every_article_is_repaired_with_no_refetch() {
    let posts = with_par2(
        vec![Post::plain("lantern.zip", zip::archive(zip::stored()))],
        2,
    );
    restart_row(Scenario::new(posts).damaged(0, 2)).await;
}
