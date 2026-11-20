// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Rows past size boundaries the small rows never cross.

use super::harness::{Post, Scenario, par2_volumes_sliced, payload, run};

const MIB: usize = 1 << 20;
const FRAME: usize = 256 * 1024;

// A PAR2 recovery volume of ten 1 MiB blocks, past the 8 MiB a volume may
// hold ahead of its front, needed whole: ten slices of the file each lose
// an article. Every article of the volume arrives last first, so its tail
// lands far past its front and the walk must read what it could not hold
// back from the volume's file once the front comes.
#[tokio::test]
async fn a_par2_volume_past_the_hold_cap_arriving_last_first_repairs() {
    let data = payload(10 * MIB + 13, 171);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_volumes_sliced(
        "lantern",
        &[("lantern.bin", &data)],
        MIB as u64,
        10,
        1,
    ));
    assert_eq!(posts.len(), 3);
    assert!(posts[2].bytes.len() > 8 * MIB, "{}", posts[2].bytes.len());
    let mut scenario = Scenario::new(posts).framed(FRAME).reversed(&[2]);
    for slice in 0..10u32 {
        scenario = scenario.lost(0, slice * 4 + 1);
    }
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert!(graph.files["lantern beacon/lantern.bin"] == data, "bytes");
}

// A stored RAR set of four volumes of about 3 MiB, nameless and posted last
// volume first, every handout arriving last article first: the later
// volumes are written to their own files before the first builds the set,
// and each is read back to the store from there in more than one piece.
async fn rar_set_posted_backwards(nameless: bool, len: usize, count: usize, least: usize) {
    use crate::pipeline::tests::direct_store::{obfuscate_volumes, single_member_store_set};
    let data = payload(len, 173);
    let mut volumes = single_member_store_set("ember.bin", &data, count);
    if nameless {
        volumes = obfuscate_volumes(&volumes);
    }
    volumes.reverse();
    assert!(volumes.iter().all(|(_, bytes)| bytes.len() > least));
    let scenario = Scenario::new(super::rar::posts(volumes))
        .framed(FRAME)
        .backwards();
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert!(graph.files["lantern beacon/ember.bin"] == data, "bytes");
}

#[tokio::test]
async fn a_large_rar_set_posted_last_volume_first_delivers() {
    rar_set_posted_backwards(false, 12 * MIB + 29, 4, 2 * MIB).await;
}

#[tokio::test]
async fn a_large_nameless_rar_set_posted_last_volume_first_delivers() {
    rar_set_posted_backwards(true, 12 * MIB + 29, 4, 2 * MIB).await;
}

// A PAR3 set of 1 MiB blocks over a file past three of them, two slices
// lost, its recovery volumes posted last article first: the rebuild reads
// the file and the volumes past the 1 MiB it reads at once.
#[tokio::test]
async fn a_par3_set_of_mib_blocks_repairs_two_slices_posted_backwards() {
    let data = payload(3 * MIB + 4099, 179);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(super::harness::par3_set_sized(
        "lantern",
        &[("lantern.bin", &data)],
        MIB as u64,
        2,
    ));
    assert!(posts.len() >= 3, "{}", posts.len());
    let volumes: Vec<u32> = (2..posts.len() as u32).collect();
    assert!(
        volumes
            .iter()
            .map(|at| posts[*at as usize].bytes.len())
            .sum::<usize>()
            > 2 * MIB,
        "volumes hold the blocks"
    );
    let scenario = Scenario::new(posts)
        .framed(FRAME)
        .reversed(&volumes)
        .lost(0, 1)
        .lost(0, 9);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert!(graph.files["lantern beacon/lantern.bin"] == data, "bytes");
}

// The nameless set again with volumes past the 4 MiB a volume's parse may
// carry to the set's object once the first volume founds it: the volumes
// that came first, each founding an object of its own, are given up to it
// some other way, or read again.
#[tokio::test]
async fn a_nameless_rar_set_of_volumes_past_the_carry_cap_posted_backwards_delivers() {
    rar_set_posted_backwards(true, 18 * MIB + 31, 3, 4 * MIB + 1).await;
}

#[tokio::test]
async fn a_named_rar_set_of_volumes_past_the_carry_cap_posted_backwards_delivers() {
    rar_set_posted_backwards(false, 18 * MIB + 31, 3, 4 * MIB + 1).await;
}

// A stored 7z of two members past READ_BACK, split into three numbered
// volumes of about 2 MiB: its map is in the last volume, so the body comes
// before it and is kept or held, then placed. Posted in order, header
// volume first, and with every handout last article first.
async fn large_split_7z(rotate: bool, backwards: bool) {
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
    use std::io::Cursor;
    let members = [
        ("ember.mkv", payload(3 * MIB + 11, 181)),
        ("quill.mkv", payload(2 * MIB + 4093, 182)),
    ];
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    for (name, bytes) in &members {
        let mut entry = ArchiveEntry::new_file(name);
        entry.size = bytes.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(bytes.clone())))
            .unwrap();
    }
    let archive = writer.finish().unwrap().into_inner();
    let mut volumes = super::sevenz::split(&archive, 3);
    assert!(volumes.iter().all(|post| post.bytes.len() > MIB + 1));
    if rotate {
        volumes.rotate_right(1);
    }
    let mut scenario = Scenario::new(volumes).framed(FRAME);
    if backwards {
        scenario = scenario.backwards();
    }
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    for (name, bytes) in &members {
        let path = super::sevenz::delivered(name);
        assert!(graph.files[&path] == *bytes, "{name} bytes");
    }
}

#[tokio::test]
async fn a_large_split_7z_in_order_delivers() {
    large_split_7z(false, false).await;
}

#[tokio::test]
async fn a_large_split_7z_header_volume_first_delivers() {
    large_split_7z(true, false).await;
}

#[tokio::test]
async fn a_large_split_7z_posted_backwards_delivers() {
    large_split_7z(false, true).await;
}

// One stored 7z volume, its members past READ_BACK, every handout last
// article first: the map at its tail comes before the body it places.
#[tokio::test]
async fn a_large_single_7z_posted_backwards_delivers() {
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
    use std::io::Cursor;
    let file = payload(5 * MIB + 7, 183);
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    let mut entry = ArchiveEntry::new_file("lantern.bin");
    entry.size = file.len() as u64;
    writer
        .push_archive_entry(entry, Some(Cursor::new(file.clone())))
        .unwrap();
    let archive = writer.finish().unwrap().into_inner();
    for backwards in [false, true] {
        let mut scenario =
            Scenario::new(vec![Post::plain("lantern.7z", archive.clone())]).framed(FRAME);
        if backwards {
            scenario = scenario.backwards();
        }
        let graph = run(&scenario).await;
        assert!(graph.completed(), "{backwards} {:?}", graph.failure);
        assert!(
            graph.files[&super::sevenz::delivered("lantern.bin")] == file,
            "{backwards} bytes"
        );
    }
}

// A stored RAR of one member under PAR2 sliced at 384 KiB while articles
// are 256 KiB: one lost article costs two slices, and each repaired slice
// cuts the runs of the articles either side of it, so the store reads what
// it cut back from its member. Once the member is a plain file; once it is
// an xz the store's member binds, whose bytes went to the stream, not to a
// file of their own.
async fn rar_repair_reads_back(name: &str, member: Vec<u8>, delivered: &str, expected: &[u8]) {
    use crate::pipeline::tests::direct_store::single_member_store_set;
    let volumes = single_member_store_set(name, &member, 1);
    assert_eq!(volumes.len(), 1);
    let (rar_name, rar_bytes) = volumes[0].clone();
    let mut posts = super::rar::posts(volumes);
    posts.extend(par2_volumes_sliced(
        "lantern",
        &[(rar_name.as_str(), &rar_bytes)],
        384 * 1024,
        4,
        1,
    ));
    let scenario = Scenario::new(posts).framed(FRAME).lost(0, 2);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    let path = format!("lantern beacon/{delivered}");
    assert!(graph.files.contains_key(&path), "{:?}", graph.files.keys());
    let got = &graph.files[&path];
    let first_wrong = got.iter().zip(expected).position(|(a, b)| a != b);
    assert!(
        got.len() == expected.len() && first_wrong.is_none(),
        "len {} of {}, first wrong byte at {first_wrong:?}",
        got.len(),
        expected.len()
    );
}

#[tokio::test]
async fn a_rar_repair_off_article_bounds_reads_back_a_plain_member() {
    let data = payload(3 * MIB + 17, 193);
    rar_repair_reads_back("ember.bin", data.clone(), "ember.bin", &data).await;
}

#[tokio::test]
async fn a_rar_repair_off_article_bounds_reads_back_an_xz_member() {
    let data = super::nesting::noise(3 * MIB + 17, 194);
    let packed = super::streams::xz(&data);
    rar_repair_reads_back("ember.bin.xz", packed, "ember.bin", &data).await;
}
