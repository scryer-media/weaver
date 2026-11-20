// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Lost articles over members stored as they lie, with no recovery that can
// bring them back. A member whose lost bytes were zeros is whole when its
// own stated checksum passes with zeros there; one whose lost bytes were
// anything else fails that checksum and is not delivered. The rule is the
// same for a stored zip, a stored RAR4 or RAR5 set and a 7z copy, and at
// every depth below stored layers. Every post is cut into 4 KiB articles.

use std::io::{Cursor, Write};

use ::zip::ZipWriter;
use weaver_server_pipeline::pipeline::graph::lines::Lines;
use weaver_server_pipeline::pipeline::tests::direct_store::{
    multi_member_store_set, single_member_rar4_store_set,
};
use weaver_server_pipeline::pipeline::tests::engines::depth::stored as sevenz_of;
use weaver_server_pipeline::pipeline::tests::engines::zip::{delivered, stored};

use crate::jobs::model::JobStatus;

use super::harness::{Outcome, Post, Scenario, kinds, par2_volumes_sliced, payload, run};

const FRAME: usize = 4096;

// What the line says when lost bytes are read as zeros for a member.
const TRIED: &str = "lost bytes tried as zeros";

// `head` bytes of noise, then zeros to `len`: a member padded with zeros.
fn zero_tail(head: usize, len: usize, seed: u8) -> Vec<u8> {
    let mut bytes = payload(head, seed);
    bytes.resize(len, 0);
    bytes
}

fn zip_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, bytes) in entries {
        writer.start_file(*name, stored()).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

// A stored RAR5 volume of `entries`.
fn rar5_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let owned: Vec<(&str, Vec<u8>)> = entries
        .iter()
        .map(|(name, bytes)| (*name, bytes.to_vec()))
        .collect();
    multi_member_store_set(&owned, 1).remove(0).1
}

// Where `member[from..]` lies in `posted`: a stored layer holds its
// member's bytes as they are, at every depth.
fn at(posted: &[u8], member: &[u8], from: usize) -> usize {
    let probe = &member[..64.min(member.len())];
    let found = posted
        .windows(probe.len())
        .position(|window| window == probe)
        .expect("a stored layer holds its member's bytes as they lie");
    found + from
}

// The articles of `posted` lying wholly inside `member[from..to]`.
fn inside(posted: &[u8], member: &[u8], (from, to): (usize, usize)) -> Vec<u32> {
    let start = at(posted, member, from);
    let end = at(posted, member, to);
    ((start.div_ceil(FRAME) as u32)..((end / FRAME) as u32)).collect()
}

// `posts`, framed, with `lost` articles of post `file` lost.
fn losing(posts: Vec<Post>, file: u32, lost: &[u32]) -> Scenario {
    lost.iter()
        .fold(Scenario::new(posts).framed(FRAME), |scenario, &ordinal| {
            scenario.lost(file, ordinal)
        })
}

fn one(name: &str, bytes: Vec<u8>, lost: &[u32]) -> Scenario {
    losing(vec![Post::plain(name, bytes)], 0, lost)
}

fn said(outcome: &Outcome) -> String {
    format!(
        "{:?} {:?} {:?} files {:?}",
        outcome.status,
        outcome.failure,
        kinds(outcome),
        outcome.files.keys().collect::<Vec<_>>()
    )
}

fn assert_complete_with(outcome: &Outcome, name: &str, bytes: &[u8]) {
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(outcome)
    );
    assert!(
        outcome.files.get(&delivered(name)).map(Vec::as_slice) == Some(bytes),
        "{name}: {}",
        said(outcome)
    );
}

fn assert_failed_without(outcome: &Outcome, name: &str) {
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{}",
        said(outcome)
    );
    assert!(
        !outcome.files.contains_key(&delivered(name)),
        "{}",
        said(outcome)
    );
}

// The first two articles wholly inside `member[from..]`.
fn two_inside(posted: &[u8], member: &[u8], from: usize) -> Vec<u32> {
    let lost = inside(posted, member, (from, member.len()));
    assert!(lost.len() >= 2, "{lost:?}");
    lost[..2].to_vec()
}

// A member of 30 frames, its last 20 zeros, two articles lost in them.
fn padded() -> Vec<u8> {
    zero_tail(10 * FRAME, 30 * FRAME, 81)
}

fn unpadded() -> Vec<u8> {
    payload(30 * FRAME, 82)
}

#[tokio::test]
async fn a_rar5_stored_member_whose_lost_bytes_were_zeros_is_delivered() {
    let member = padded();
    let posted = rar5_of(&[("ember.mkv", &member)]);
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let outcome = run(&one("lantern.rar", posted, &lost)).await;
    assert_complete_with(&outcome, "ember.mkv", &member);
}

#[tokio::test]
async fn a_rar5_stored_member_whose_lost_bytes_were_not_zeros_is_not_delivered() {
    let member = unpadded();
    let posted = rar5_of(&[("ember.mkv", &member)]);
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let outcome = run(&one("lantern.rar", posted, &lost)).await;
    assert_failed_without(&outcome, "ember.mkv");
}

#[tokio::test]
async fn a_rar4_stored_member_whose_lost_bytes_were_zeros_is_delivered() {
    let member = padded();
    let posted = single_member_rar4_store_set("ember.mkv", &member, 1)
        .remove(0)
        .1;
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let outcome = run(&one("lantern.rar", posted, &lost)).await;
    assert_complete_with(&outcome, "ember.mkv", &member);
}

#[tokio::test]
async fn a_rar4_stored_member_whose_lost_bytes_were_not_zeros_is_not_delivered() {
    let member = unpadded();
    let posted = single_member_rar4_store_set("ember.mkv", &member, 1)
        .remove(0)
        .1;
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let outcome = run(&one("lantern.rar", posted, &lost)).await;
    assert_failed_without(&outcome, "ember.mkv");
}

// A stored RAR5 set of three volumes, the member split across all three:
// the lost articles lie in the second volume, in the member's zeros.
#[tokio::test]
async fn a_rar5_stored_set_whose_lost_bytes_in_one_volume_were_zeros_is_delivered() {
    let member = padded();
    let set = multi_member_store_set(&[("ember.mkv", member.clone())], 3);
    let second = &set[1].1;
    // The second volume holds the member's frames 10 to 20, all zeros.
    let data = second
        .windows(FRAME)
        .position(|window| window.iter().all(|&byte| byte == 0))
        .expect("the second volume holds the zeros");
    let lost: Vec<u32> = ((data.div_ceil(FRAME) as u32)..((data + 10 * FRAME) / FRAME) as u32)
        .take(2)
        .collect();
    assert_eq!(lost.len(), 2, "{lost:?}");
    let posts = set
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    let outcome = run(&losing(posts, 1, &lost)).await;
    assert_complete_with(&outcome, "ember.mkv", &member);
}

#[tokio::test]
async fn a_7z_copied_member_whose_lost_bytes_were_zeros_is_delivered() {
    let member = padded();
    let posted = sevenz_of(&[("ember.mkv", &member)]);
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let outcome = run(&one("lantern.7z", posted, &lost)).await;
    assert_complete_with(&outcome, "ember.mkv", &member);
}

#[tokio::test]
async fn a_7z_copied_member_whose_lost_bytes_were_not_zeros_is_not_delivered() {
    let member = unpadded();
    let posted = sevenz_of(&[("ember.mkv", &member)]);
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let outcome = run(&one("lantern.7z", posted, &lost)).await;
    assert_failed_without(&outcome, "ember.mkv");
}

// A 7z that states no CRC for its member: nothing can say whether the lost
// bytes were zeros, so no zeros are tried, and the member is not delivered.
#[tokio::test]
async fn a_7z_copied_member_with_no_crc_is_not_delivered_over_lost_zeros() {
    let member = padded();
    let posted = strip_member_crcs(&sevenz_of(&[("ember.mkv", &member)]), &member);
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let (lines, guard) = Lines::capture();
    let outcome = run(&one("lantern.7z", posted, &lost)).await;
    drop(guard);
    assert!(
        !outcome.files.contains_key(&delivered("ember.mkv")),
        "{}",
        said(&outcome)
    );
    assert!(lines.of(TRIED).is_empty(), "{:?}", lines.of(TRIED));
}

// A stored zip inside a stored RAR5: the lost articles at the root fall in
// the zip's member, in its zeros. The zip rules on its member, which is
// whole by its CRC; the RAR's member, the zip, is not delivered as a file.
#[tokio::test]
async fn a_stored_zip_in_a_stored_rar_delivers_a_member_whose_lost_bytes_were_zeros() {
    let member = padded();
    let inner = zip_of(&[("ember.mkv", &member)]);
    let posted = rar5_of(&[("level.zip", &inner)]);
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let (lines, guard) = Lines::capture();
    let outcome = run(&one("lantern.rar", posted, &lost)).await;
    drop(guard);
    assert_complete_with(&outcome, "ember.mkv", &member);
    // The try is heard, so a test that hears none means none was made.
    assert_eq!(lines.of(TRIED).len(), 1, "{:?}", lines.of(TRIED));
}

// A stored RAR5 inside a stored 7z, the same way.
#[tokio::test]
async fn a_stored_rar_in_a_stored_7z_delivers_a_member_whose_lost_bytes_were_zeros() {
    let member = padded();
    let inner = rar5_of(&[("ember.mkv", &member)]);
    let posted = sevenz_of(&[("level.rar", &inner)]);
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let outcome = run(&one("lantern.7z", posted, &lost)).await;
    assert_complete_with(&outcome, "ember.mkv", &member);
}

// Three deep: a stored zip in a stored RAR5 in a stored 7z.
#[tokio::test]
async fn three_stored_layers_deep_a_member_whose_lost_bytes_were_zeros_is_delivered() {
    let member = padded();
    let zip = zip_of(&[("ember.mkv", &member)]);
    let rar = rar5_of(&[("level.zip", &zip)]);
    let posted = sevenz_of(&[("level.rar", &rar)]);
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let outcome = run(&one("lantern.7z", posted, &lost)).await;
    assert_complete_with(&outcome, "ember.mkv", &member);
}

// A stored zip of two members inside a stored RAR5, the lost articles in
// the first member's bytes, which were not zeros: that member fails its
// CRC and is not delivered; the second is, whole, and the job is Partial.
// The RAR is not failed whole for what its member lost.
#[tokio::test]
async fn a_nested_member_whose_lost_bytes_were_not_zeros_fails_alone() {
    let (ember, quill) = (unpadded(), payload(12 * FRAME + 5, 83));
    let inner = zip_of(&[("ember.mkv", &ember), ("quill.mkv", &quill)]);
    let posted = rar5_of(&[("level.zip", &inner)]);
    let lost = two_inside(&posted, &ember, 12 * FRAME);
    let outcome = run(&one("lantern.rar", posted, &lost)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Partial),
        "{}",
        said(&outcome)
    );
    assert!(
        !outcome.files.contains_key(&delivered("ember.mkv")),
        "{}",
        said(&outcome)
    );
    assert!(
        outcome.files.get(&delivered("quill.mkv")) == Some(&quill),
        "{}",
        said(&outcome)
    );
}

// A stored RAR5 and a 7z copy of two members each, the lost articles in
// the first member's bytes, which were not zeros: the first is not
// delivered, the second is, and the job is Partial.
#[tokio::test]
async fn a_failed_stored_member_leaves_the_next_delivered_in_rar_and_7z() {
    let (ember, quill) = (unpadded(), payload(12 * FRAME + 5, 84));
    let entries: [(&str, &[u8]); 2] = [("ember.mkv", &ember), ("quill.mkv", &quill)];
    for (name, posted) in [
        ("lantern.rar", rar5_of(&entries)),
        ("lantern.7z", sevenz_of(&entries)),
    ] {
        let lost = two_inside(&posted, &ember, 12 * FRAME);
        let outcome = run(&one(name, posted, &lost)).await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Partial),
            "{name}: {}",
            said(&outcome)
        );
        assert!(
            !outcome.files.contains_key(&delivered("ember.mkv")),
            "{name}: {}",
            said(&outcome)
        );
        assert!(
            outcome.files.get(&delivered("quill.mkv")) == Some(&quill),
            "{name}: {}",
            said(&outcome)
        );
    }
}

// A PAR2 set over the zip that can rebuild what was lost: the repair comes
// first, and no zeros are tried.
#[tokio::test]
async fn a_repair_that_can_rebuild_the_lost_bytes_comes_before_zeros() {
    let member = padded();
    let posted = zip_of(&[("ember.mkv", &member)]);
    let lost = two_inside(&posted, &member, 12 * FRAME);
    let mut posts = vec![Post::plain("lantern.zip", posted.clone())];
    posts.extend(par2_volumes_sliced(
        "lantern",
        &[("lantern.zip", &posted)],
        FRAME as u64,
        4,
        1,
    ));
    let (lines, guard) = Lines::capture();
    let outcome = run(&losing(posts, 0, &lost)).await;
    drop(guard);
    assert_complete_with(&outcome, "ember.mkv", &member);
    assert!(lines.of(TRIED).is_empty(), "{:?}", lines.of(TRIED));
}

// The same container with every stored CRC32 taken out of its end header:
// `sevenz_turbo`'s writer always records one. A single-block archive's
// digests live in its SubStreamsInfo's `kCRC` property; removing it leaves
// an ordinary archive, and the end header's own checksums are computed
// again.
fn strip_member_crcs(archive: &[u8], member: &[u8]) -> Vec<u8> {
    const K_END: u8 = 0x00;
    const K_HEADER: u8 = 0x01;
    const K_SUB_STREAMS_INFO: u8 = 0x08;
    const K_CRC: u8 = 0x0A;
    let crc32 =
        |bytes: &[u8]| crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32;
    let next_header_offset = u64::from_le_bytes(archive[12..20].try_into().unwrap());
    let next_header_size = u64::from_le_bytes(archive[20..28].try_into().unwrap());
    let header_start = (32 + next_header_offset) as usize;
    let header = &archive[header_start..header_start + next_header_size as usize];
    assert_eq!(header.first().copied(), Some(K_HEADER));
    let mut wanted = vec![K_SUB_STREAMS_INFO, K_CRC, 1];
    wanted.extend_from_slice(&crc32(member).to_le_bytes());
    wanted.push(K_END);
    let found = header
        .windows(wanted.len())
        .position(|window| window == wanted.as_slice())
        .expect("the member's recorded digest is in the end header");
    let mut rewritten = header[..found].to_vec();
    rewritten.extend_from_slice(&[K_SUB_STREAMS_INFO, K_END]);
    rewritten.extend_from_slice(&header[found + wanted.len()..]);
    let mut out = archive[..header_start].to_vec();
    out.extend_from_slice(&rewritten);
    out[20..28].copy_from_slice(&(rewritten.len() as u64).to_le_bytes());
    out[28..32].copy_from_slice(&crc32(&rewritten).to_le_bytes());
    let start_crc = crc32(&out[12..32]);
    out[8..12].copy_from_slice(&start_crc.to_le_bytes());
    out
}
