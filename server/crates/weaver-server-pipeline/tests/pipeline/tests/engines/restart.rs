// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeSet;

use super::harness::{ARTICLE, Post, Scenario, par2_set, payload, run};
use super::rar::posts as volumes;
use super::sfv::{crc, listing};
use super::streams::xz;
use super::streams::{brotli, bzip2, gzip, members, tar, zstd};
use super::zip;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
    Entry, build_7z_shaped, split_volumes,
};
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_store_set, header_encrypted_store_set,
};
use crate::pipeline::tests::direct_store::{
    obfuscate_volumes, protected_store_set, single_member_store_set,
};
use sevenz_turbo::EncoderMethod;
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

#[tokio::test]
async fn an_xz_stream_under_par2_restarted_after_every_article_replays_from_its_start() {
    let posts = with_par2(
        vec![Post::plain(
            "ember.bin.xz",
            xz(&payload(40 * ARTICLE + 5, 72)),
        )],
        2,
    );
    restart_row(Scenario::new(posts)).await;
}

#[tokio::test]
async fn a_three_volume_stored_rar_restarted_after_every_article_delivers_its_member() {
    let body = payload(11 * ARTICLE + 9, 73);
    restart_row(Scenario::new(volumes(single_member_store_set(
        "beacon.bin",
        &body,
        3,
    ))))
    .await;
}

#[tokio::test]
async fn a_stored_rar_under_par2_restarted_after_every_article_is_repaired_with_no_refetch() {
    let body = payload(8 * ARTICLE + 17, 74);
    let posts = with_par2(volumes(single_member_store_set("beacon.bin", &body, 2)), 2);
    restart_row(Scenario::new(posts).damaged(0, 2)).await;
}

#[tokio::test]
async fn a_rar5_volume_repaired_from_its_recovery_record_restarted_after_every_article_is_repaired_with_no_refetch()
 {
    let body = payload(8 * ARTICLE + 17, 76);
    let posts = volumes(protected_store_set("beacon.bin", &body, 2, 1));
    restart_row(Scenario::new(posts).damaged(0, 2)).await;
}

#[tokio::test]
async fn an_xz_stream_inside_a_rar_volume_restarted_after_every_article_delivers_the_file() {
    let compressed = xz(&payload(30 * ARTICLE + 3, 75));
    restart_row(Scenario::new(volumes(single_member_store_set(
        "beacon.bin.xz",
        &compressed,
        2,
    ))))
    .await;
}

// The RARLAB archive with a compressed member, cut fine enough to restart
// between its articles: the engine's run replays from the volume's start
// over the partials on disk, so a restart re-reads and never fetches.
#[tokio::test]
async fn a_compressed_rar5_volume_restarted_after_every_article_replays_the_engine_from_disk() {
    let bytes =
        weaver_server_pipeline::pipeline::tests::rar5_fixture_bytes("rar5_multifile_lz.rar");
    restart_row(Scenario::new(vec![Post::plain("mixed.rar", bytes)]).framed(64)).await;
}

// The same archive under PAR2 with its first article lost: whenever the
// restart lands, the restored job repairs the front it never had and the
// engine delivers every member.
#[tokio::test]
async fn a_compressed_rar5_volume_under_par2_with_its_front_lost_restarted_after_every_article_is_repaired()
 {
    let bytes =
        weaver_server_pipeline::pipeline::tests::rar5_fixture_bytes("rar5_multifile_lz.rar");
    let posts = with_par2(vec![Post::plain("mixed.rar", bytes)], 8);
    restart_row(Scenario::new(posts).framed(64).lost(0, 0)).await;
}

// A file posted under a name its PAR2 set does not give, damaged: the set
// knows it by its contents. No checkpoint keeps that, so a restored set
// binds over the restored members before it rules, and never takes the file
// it describes for absent while the member that is it sits there complete.
// Whenever the restart lands, the file is repaired and delivered once,
// under the name the set describes.
#[tokio::test]
async fn a_damaged_file_under_another_name_restarted_after_every_article_is_repaired_under_its_own()
{
    let body = payload(5 * ARTICLE, 121);
    let mut posts = with_par2(vec![Post::plain("lantern.bin", body.clone())], 2);
    posts[0] = Post::plain("a1b2c3d4e5", body.clone());
    let whole = restart_row(Scenario::new(posts).damaged(0, 1)).await;
    assert_eq!(
        whole.files.values().collect::<Vec<_>>(),
        vec![&body],
        "{:?}",
        whole.files.keys().collect::<Vec<_>>()
    );
    assert!(
        whole
            .files
            .keys()
            .all(|path| path.ends_with("/lantern.bin")),
        "{:?}",
        whole.files.keys().collect::<Vec<_>>()
    );
}

// The five encrypted RARLAB volumes, offered their password again at every
// restore, in 64 KiB articles so a restart lands inside every volume.
#[tokio::test]
async fn the_encrypted_five_volume_rar5_set_restarted_after_every_article_delivers_its_member() {
    let set = (1..=5)
        .map(|part| {
            (
                format!("clip.part{part}.rar"),
                weaver_server_pipeline::pipeline::tests::rar5_fixture_bytes(&format!(
                    "rar5_enc_mv_video.part{part}.rar"
                )),
            )
        })
        .collect();
    restart_row(
        Scenario::new(volumes(set))
            .framed(64 * 1024)
            .password("testpass123"),
    )
    .await;
}

// Two members in one 7z coded by `method`, cut into `count` volumes.
fn sevenz(method: EncoderMethod, count: usize) -> Vec<Post> {
    let entries = [
        Entry::file("ember.bin", payload(7 * ARTICLE + 11, 78)),
        Entry::file("quill.bin", payload(4 * ARTICLE - 3, 79)),
    ];
    volumes(split_volumes(
        &build_7z_shaped(&entries, method, None, false),
        count,
    ))
}

#[tokio::test]
async fn a_stored_7z_restarted_after_every_article_delivers_its_members() {
    restart_row(Scenario::new(sevenz(EncoderMethod::COPY, 1))).await;
}

// The two members of `sevenz` stored in two volumes, enciphered under
// `password` when one is given.
fn quill_parts(password: Option<&str>) -> Vec<(String, Vec<u8>)> {
    let entries = [
        Entry::file("ember.bin", payload(7 * ARTICLE + 11, 78)),
        Entry::file("quill.bin", payload(4 * ARTICLE - 3, 79)),
    ];
    split_volumes(
        &build_7z_shaped(&entries, EncoderMethod::COPY, password, false),
        2,
    )
}

// The article holding the first byte of `quill.bin` starts inside it and
// holds the end of `ember.bin` too; enciphered, it starts inside a cipher
// block whose first bytes are the article before's. The second volume
// backwards from its end header, then the first volume, and the cut last:
// both members are whole and on disk when the pipeline goes down with
// `quill.bin` gone. The article before reads back and comes no more, and
// the one holding the cut is fetched again. The job finishes as the run
// that never restarted does, fetching again only the articles holding
// `quill.bin`.
async fn lost_quill(password: Option<&str>) {
    let parts = quill_parts(password);
    let first = parts[0].1.len();
    let counts: Vec<u32> = parts
        .iter()
        .map(|(_, bytes)| bytes.len().div_ceil(ARTICLE) as u32)
        .collect();
    let article_at = |offset: usize| match offset.checked_sub(first) {
        None => (0, (offset / ARTICLE) as u32),
        Some(into) => (1, (into / ARTICLE) as u32),
    };
    let start_of =
        |(volume, ordinal): (u32, u32)| volume as usize * first + ordinal as usize * ARTICLE;
    // `quill.bin` follows `ember.bin` in the folder's plaintext.
    let quill = 32 + 7 * ARTICLE + 11..32 + 7 * ARTICLE + 11 + 4 * ARTICLE - 3;
    let cut = article_at(quill.start);
    let cut_start = start_of(cut);
    assert!(
        cut_start < quill.start,
        "the cut article holds ember.bin too"
    );
    if password.is_some() {
        assert_ne!((cut_start - 32) % 16, 0, "the cut is inside a cipher block");
    }
    assert_eq!(cut.0, 1, "the cut is the second volume's");
    let before = (0, counts[0] - 1);
    let order: Vec<(u32, u32)> = std::iter::once((0, 0))
        .chain((1..counts[1]).rev().map(|ordinal| (1, ordinal)))
        .chain((1..counts[0]).map(|ordinal| (0, ordinal)))
        .chain(std::iter::once(cut))
        .collect();
    let mut scenario = Scenario::new(volumes(parts)).first(&order);
    if let Some(password) = password {
        scenario = scenario.password(password);
    }
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let down = order.len();
    let restarted = run_restarting_at(&scenario, &mut |_| {}, Down::Losing("quill.bin"), &|nth| {
        nth == down
    })
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
    let holding: BTreeSet<(u32, u32)> = (cut.1..=article_at(quill.end - 1).1)
        .map(|ordinal| (1, ordinal))
        .filter(|article| !restarted.in_memory.contains(article))
        .collect();
    assert!(holding.contains(&cut), "the cut article was on disk");
    assert!(
        !restarted.in_memory.contains(&before),
        "the article before was on disk"
    );
    assert_eq!(
        restarted.from_disk, holding,
        "the articles holding quill.bin on disk, and no other, come again"
    );
}

#[tokio::test]
async fn an_enciphered_7z_member_lost_from_inside_a_cipher_block_comes_out_again_whole() {
    lost_quill(Some(SECRET)).await;
}

#[tokio::test]
async fn a_stored_7z_member_lost_at_a_restart_comes_out_again_whole() {
    lost_quill(None).await;
}

#[tokio::test]
async fn a_three_volume_stored_7z_restarted_after_every_article_delivers_its_members() {
    restart_row(Scenario::new(sevenz(EncoderMethod::COPY, 3))).await;
}

// The engine's run replays its folder from the start over the partials on
// disk, so a restart re-reads and never fetches.
#[tokio::test]
async fn an_lzma2_7z_restarted_after_every_article_replays_the_engine_from_disk() {
    restart_row(Scenario::new(sevenz(EncoderMethod::LZMA2, 1))).await;
}

// The two members of `sevenz` in one AES folder, cut into two volumes: the
// second starts inside a cipher block, so an article of it starts inside one
// whose first bytes are the article before's.
fn enciphered_7z_parts() -> Vec<(String, Vec<u8>)> {
    let entries = [
        Entry::file("ember.bin", payload(7 * ARTICLE + 11, 78)),
        Entry::file("quill.bin", payload(4 * ARTICLE - 3, 79)),
    ];
    let parts = split_volumes(
        &build_7z_shaped(&entries, EncoderMethod::COPY, Some(SECRET), false),
        2,
    );
    // The folder's cipher starts past the 32-byte signature header.
    assert_ne!(
        (parts[0].1.len() - 32) % 16,
        0,
        "the second volume starts inside a cipher block"
    );
    parts
}

// The signature, the second volume backwards from its end header, the
// first volume's last article, then the rest of the first in order: the
// article that starts the second volume comes before the one that brings
// the first bytes of its cipher block.
fn header_first(parts: &[(String, Vec<u8>)]) -> Vec<(u32, u32)> {
    let counts: Vec<u32> = parts
        .iter()
        .map(|(_, bytes)| bytes.len().div_ceil(ARTICLE) as u32)
        .collect();
    std::iter::once((0, 0))
        .chain((0..counts[1]).rev().map(|ordinal| (1, ordinal)))
        .chain(std::iter::once((0, counts[0] - 1)))
        .chain((1..counts[0] - 1).map(|ordinal| (0, ordinal)))
        .collect()
}

// In order, backwards, and with the second volume ahead of the first.
#[tokio::test]
async fn an_enciphered_split_7z_restarted_after_every_article_delivers_its_members() {
    let scenario = || Scenario::new(volumes(enciphered_7z_parts())).password(SECRET);
    restart_row(scenario()).await;
    restart_row(scenario().backwards()).await;
    let order = header_first(&enciphered_7z_parts());
    restart_row(scenario().first(&order)).await;
}

// A stored 7z in hex-named parts that say nothing of which part they are,
// under a PAR2 set that names them, its index posted first. Its verify run
// is out from the index on, so no part is bound by its bytes before a
// restart; the restore binds each one as it reads the part back, and the
// articles of that part it has not read yet are on disk all the same.
#[tokio::test]
async fn obfuscated_7z_parts_under_par2_restarted_after_every_article_are_bound_with_no_refetch() {
    let entries = [Entry::file("ember.bin", payload(70_001, 88))];
    let parts = split_volumes(
        &build_7z_shaped(&entries, EncoderMethod::COPY, None, false),
        2,
    );
    let named: Vec<(&str, &[u8])> = parts
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut set = par2_set("lantern", &named, 2);
    let mut posts = vec![set.remove(0)];
    posts.extend(volumes(obfuscate_volumes(&parts)));
    posts.extend(set);
    let scenario = Scenario::new(posts)
        .framed(20 * 1024)
        .first(&[(0, 0), (0, 1), (1, 1), (1, 0)]);
    restart_row(scenario).await;
}

// Nameless header-encrypted RAR5 volumes under a PAR2 set that names them,
// its index posted first, then the second volume ahead of the first. The
// second is bound to a store of its own, which gives it back once the set
// renames it, and the store it founds opens the headers before the first
// volume arrives.
fn nameless_header_encrypted_second_first() -> Scenario {
    let body = payload(70_001, 51);
    let parts = header_encrypted_store_set(
        "nested/feature.mkv",
        &body,
        4,
        SECRET,
        HeaderCheck::For(SECRET),
    );
    let named: Vec<(&str, &[u8])> = parts
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut set = par2_set("silver.horizon", &named, 2);
    let mut posts = vec![set.remove(0)];
    posts.extend(volumes(obfuscate_volumes(&parts)));
    posts.extend(set);
    Scenario::new(posts)
        .framed(64 * 1024)
        .password(SECRET)
        .first(&[(0, 0), (2, 0), (1, 0), (3, 0), (4, 0)])
}

// The run shut down and restored after each article `at` picks finishes as
// the run that never restarted does, and fetches nothing it had on disk.
async fn shut_down_after(scenario: Scenario, at: &dyn Fn(usize) -> bool) {
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let restarted = run_restarting_at(&scenario, &mut |_| {}, Down::Shutdown, at).await;
    assert_restarted(&restarted, &whole);
}

// A solid RAR5 set in hex-named volumes under a PAR2 set that names them,
// its index posted first. The engine's run is out over the first volume
// whenever the pipeline goes down, waiting for bytes of a volume still to
// come or for the owner of a member still undecided, which nothing brings
// once the pipeline is down: the drain stops it where it stands, and the
// restored job reads the volumes back and fetches none again.
#[tokio::test]
async fn a_solid_rar5_set_in_hex_names_shut_down_while_its_run_waits_is_read_back() {
    let parts: Vec<(String, Vec<u8>)> = (1..=4)
        .map(|part| {
            let name = format!("rar5_solid_four.part0{part}.rar");
            let path = format!(
                "{}/../weaver-server-core/tests/fixtures/extraction_profiles/{name}",
                env!("CARGO_MANIFEST_DIR")
            );
            (name, std::fs::read(path).unwrap())
        })
        .collect();
    let named: Vec<(&str, &[u8])> = parts
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut set = par2_set("harbour", &named, 2);
    let mut posts = vec![set.remove(0)];
    posts.extend(volumes(obfuscate_volumes(&parts)));
    posts.extend(set);
    shut_down_after(Scenario::new(posts).framed(4096), &|_| true).await;
}

// Restarted before the first volume arrives, the store the given-back
// volume founded still opens the first volume's headers.
#[tokio::test]
async fn nameless_header_encrypted_volumes_restarted_before_the_first_arrives_are_bound_whole() {
    shut_down_after(nameless_header_encrypted_second_first(), &|nth| nth == 2).await;
}

// Restarted once the first volume is in, the store reads both volumes back
// from disk and fetches neither again.
#[tokio::test]
async fn nameless_header_encrypted_volumes_restarted_after_the_first_arrives_are_read_back() {
    shut_down_after(nameless_header_encrypted_second_first(), &|nth| nth == 3).await;
}

// Restarted after every article, each store the restore rebuilds opens
// what it opened before.
#[tokio::test]
async fn nameless_header_encrypted_volumes_restarted_after_every_article_deliver() {
    shut_down_after(nameless_header_encrypted_second_first(), &|_| true).await;
}

// A three-volume set the RAR engine reads, posted without `.r00`. The engine
// announces the member and never closes it, so its refusal leaves it buried
// under the engine's node, and that buried member leaves nothing whole, so
// the job fails. A refusal of that kind waits for the payload to be in, so it
// lands in the last settle, after the last checkpoint: restored after every
// article before it, the job still fails as the run that never restarted
// does. That a checkpoint carries a buried member back is the tree record's
// round trip.
#[tokio::test]
async fn a_set_missing_a_volume_restarted_after_every_article_still_fails() {
    let body = payload(7 * ARTICLE + 13, 51);
    let posts: Vec<Post> =
        header_encrypted_store_set("beacon.bin", &body, 3, SECRET, HeaderCheck::Absent)
            .into_iter()
            .zip(["ember.rar", "ember.r00", "ember.r01"])
            .filter(|(_, name)| *name != "ember.r00")
            .map(|((_, bytes), name)| Post::plain(name, bytes))
            .collect();
    let scenario = Scenario::new(posts).password(SECRET);
    let whole = run(&scenario).await;
    assert_eq!(
        whole.failure.as_deref(),
        Some("no file was delivered whole; 2 short: ember.rar, ember.r01")
    );
    let restarted = run_restarting(&scenario).await;
    assert_restarted(&restarted, &whole);
}

// One enciphered RAR5 member whose checksum the password keys, with 16 bytes
// in the middle of the volume posted wrong when `spoiled`, though every
// article passes its own check.
fn keyed_member(body: &[u8], spoiled: bool) -> Scenario {
    let mut set = encrypted_store_set("beacon.bin", body, 1, SECRET, Some(SECRET), true);
    if spoiled {
        let middle = set[0].1.len() / 2;
        set[0].1[middle..middle + 16]
            .iter_mut()
            .for_each(|byte| *byte ^= 0x5a);
    }
    Scenario::new(volumes(set)).password(SECRET)
}

// The article after which a keyed member restarts while it is still open:
// past the wrong span in its middle, short of its end.
const KEYED_OPEN_AT: usize = 6;

// Run straight through, a keyed member posted wrong fails its checksum.
#[tokio::test]
async fn a_keyed_rar_member_posted_wrong_fails_its_checksum() {
    let body = payload(6 * ARTICLE + 11, 77);
    let whole = run(&keyed_member(&body, true)).await;
    assert!(!whole.completed(), "{:?} {:?}", whole.status, whole.failure);
}

// Restarted after every article, it fails it too: a restore does not leave
// the member unchecked.
#[tokio::test]
async fn a_keyed_rar_member_posted_wrong_fails_its_checksum_across_restarts() {
    let body = payload(6 * ARTICLE + 11, 77);
    let restarted = run_restarting(&keyed_member(&body, true)).await;
    assert!(restarted.restarts > 0);
    assert!(
        !matches!(restarted.status, Some(JobStatus::Complete)),
        "{:?} {:?}",
        restarted.status,
        restarted.files.keys()
    );
}

// A keyed member restarted while it is open takes its last articles after the
// restore and comes out whole, as it does run straight through.
#[tokio::test]
async fn a_keyed_rar_member_restarted_open_comes_out_whole() {
    let body = payload(6 * ARTICLE + 11, 77);
    let scenario = keyed_member(&body, false);
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    assert!(
        whole.files.values().any(|bytes| *bytes == body),
        "member bytes"
    );
    let restarted =
        run_restarting_at(&scenario, &mut |_| {}, Down::Crash, &|n| n == KEYED_OPEN_AT).await;
    assert_restarted(&restarted, &whole);
}

// A keyed member restarted while it is open, its wrong span read before the
// restart, fails its checksum once its last articles come.
#[tokio::test]
async fn a_keyed_rar_member_restarted_open_over_a_wrong_span_fails_its_checksum() {
    let body = payload(6 * ARTICLE + 11, 77);
    let scenario = keyed_member(&body, true);
    let restarted =
        run_restarting_at(&scenario, &mut |_| {}, Down::Crash, &|n| n == KEYED_OPEN_AT).await;
    assert_eq!(restarted.restarts, 1);
    assert!(
        !matches!(restarted.status, Some(JobStatus::Complete)),
        "{:?} {:?}",
        restarted.status,
        restarted.files.keys()
    );
    assert!(
        !restarted.files.values().any(|bytes| *bytes == body),
        "member whole"
    );
}
