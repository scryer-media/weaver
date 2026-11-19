// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A stored old-numbering RAR4 set shaped as a film's post was. The NZB
// names its first volume `<Release>.part01.rar` and the rest
// `65606-1.rNN`, under two stems; a PAR2 set over the true names,
// `<release>.rar` and `<release>.rNN`, renames each volume by its content.
// Every main header marks its volume the set's first and no volume has an
// end record, so no header states a number. Every part names the member
// alike; the first and last parts state its true size, every part between
// them a larger one. Two articles are lost and the PAR2 set mends them.
//
// unrar reads such a set whole: it extracts a stored member to the size the
// first part states (CmdExtract::ExtractCurrentFile hands
// Arc.FileHead.UnpSize to CmdExtract::UnstoreFile, extract.cpp), reads the
// later parts by place without looking at their sizes (MergeArchive,
// volume.cpp), and checks the CRC the last part states against the bytes
// written (extract.cpp, ValidCRC). So the member is delivered whole when
// its bytes give that CRC, and the job fails when they do not.

use super::harness::{Outcome, Post, Scenario, kinds, par2_volumes_sliced, payload, run};
use super::rar::delivered;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::{rar4_misstated_set, rar5_misstated_set};
use par2_rs::checksum;

const MEMBER: &str = "final.girl.x264-ulshd.mkv";
const TRUE_BASE: &str = "final.girl.x264-ulshd";
// Articles, and PAR2 slices, of 128 KiB: each volume is four of them.
const FRAME: usize = 128 * 1024;
const VOLUMES: usize = 6;
// What the middle parts state over the true size, as the post's did.
const OVERSTATED: u32 = 62_609_861;

fn body() -> Vec<u8> {
    payload(VOLUMES * 4 * FRAME - 777, 83)
}

// The set, each part stating the true size at either end and a larger one
// between, its last part stating `crc` as the member's.
fn set(body: &[u8], crc: u32) -> Vec<(String, Vec<u8>)> {
    let size = body.len() as u32;
    rar4_misstated_set(
        TRUE_BASE,
        MEMBER,
        body,
        VOLUMES,
        |volume| match volume {
            0 => size,
            last if last + 1 == VOLUMES => size,
            _ => size + OVERSTATED,
        },
        crc,
    )
}

// The posts: the PAR2 index and its volume over the true names, then the
// first volume under the release's stem and the rest under the poster's,
// with an article lost in two of the middle volumes.
fn posted(set: Vec<(String, Vec<u8>)>) -> Scenario {
    placed(set).0
}

// [`posted`], and the post the set's first volume is.
fn placed(set: Vec<(String, Vec<u8>)>) -> (Scenario, u32) {
    let files: Vec<(&str, &[u8])> = set
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = par2_volumes_sliced(TRUE_BASE, &files, FRAME as u64, 4, 1);
    let first = posts.len() as u32;
    posts.extend(set.iter().enumerate().map(|(index, (_, bytes))| {
        let name = match index {
            0 => "Final.Girl.x264-ULSHD.part01.rar".to_owned(),
            _ => format!("65606-1.r{:02}", index - 1),
        };
        Post::plain(&name, bytes.clone())
    }));
    let scenario = Scenario::new(posts)
        .framed(FRAME)
        .lost(first + 2, 1)
        .lost(first + 4, 2);
    (scenario, first)
}

fn describe(outcome: &Outcome) -> String {
    format!(
        "{:?} {:?} kinds {:?} delivery {:?}",
        outcome.status,
        outcome.failure,
        kinds(outcome),
        outcome.graph().delivery
    )
}

#[tokio::test]
async fn a_set_whose_middle_parts_overstate_its_size_delivers_the_member_its_crc_proves() {
    let body = body();
    let outcome = run(&posted(set(&body, checksum::crc32(&body)))).await;
    assert!(outcome.completed(), "{}", describe(&outcome));
    assert_eq!(
        outcome.files.get(&delivered(MEMBER)).map(Vec::as_slice),
        Some(body.as_slice()),
        "delivered {:?}",
        outcome.files.keys().collect::<Vec<_>>()
    );
    assert!(
        outcome.graph().delivery.withheld.is_empty(),
        "{}",
        describe(&outcome)
    );
}

// The 548b set with its volumes' fronts arriving in `order`, given as
// places in the set: what comes of it is what comes of it in volume order.
async fn delivers_in_order(order: &[u32]) {
    let body = body();
    let (scenario, first) = placed(set(&body, checksum::crc32(&body)));
    let posts: Vec<u32> = order.iter().map(|volume| first + volume).collect();
    let outcome = run(&scenario.together(&posts)).await;
    // The fronts were asked for in that order.
    let asked: Vec<u32> = outcome
        .order
        .iter()
        .filter(|(post, ordinal)| *ordinal == 0 && posts.contains(post))
        .map(|(post, _)| post - first)
        .collect();
    assert_eq!(asked, order, "fronts asked for");
    assert!(outcome.completed(), "{order:?}: {}", describe(&outcome));
    assert_eq!(
        outcome.files.get(&delivered(MEMBER)).map(Vec::as_slice),
        Some(body.as_slice()),
        "{order:?}: delivered {:?}",
        outcome.files.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_set_whose_middle_parts_overstate_its_size_delivers_with_its_last_volume_first() {
    delivers_in_order(&[5, 4, 3, 2, 1, 0]).await;
}

#[tokio::test]
async fn a_set_whose_middle_parts_overstate_its_size_delivers_with_a_middle_volume_before_the_one_before_it()
 {
    delivers_in_order(&[0, 3, 2, 1, 4, 5]).await;
}

#[tokio::test]
async fn a_set_whose_parts_disagree_on_its_size_and_whose_bytes_fail_its_crc_fails() {
    let body = body();
    let outcome = run(&posted(set(&body, checksum::crc32(&body) ^ 1))).await;
    // The set is laid whole, so the member is read to its end and judged by
    // its CRC, not lost to a split.
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{}",
        describe(&outcome)
    );
    let delivery = &outcome.graph().delivery;
    assert!(
        delivery.whole.iter().all(|name| name != MEMBER)
            && delivery.damaged.iter().any(|name| name == MEMBER)
            && delivery.withheld.is_empty(),
        "{}",
        describe(&outcome)
    );
}

// The same fault in a RAR5 set, numbered and closed by end records as RAR5
// writes it, posted under the poster's stem with a PAR2 set over the true
// names: its middle parts overstating the member's size.
const RAR5_BASE: &str = "final.girl.x264-ulshd";

fn rar5_posted(set: Vec<(String, Vec<u8>)>) -> Scenario {
    let files: Vec<(&str, &[u8])> = set
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = par2_volumes_sliced(RAR5_BASE, &files, FRAME as u64, 4, 1);
    let first = posts.len() as u32;
    posts.extend(set.iter().enumerate().map(|(index, (_, bytes))| {
        Post::plain(&format!("65606-1.part{:02}.rar", index + 1), bytes.clone())
    }));
    Scenario::new(posts)
        .framed(FRAME)
        .lost(first + 2, 1)
        .lost(first + 4, 2)
}

#[tokio::test]
async fn a_rar5_set_whose_middle_parts_overstate_its_size_delivers_the_member_its_crc_proves() {
    let body = body();
    let size = body.len() as u64;
    let set = rar5_misstated_set(
        RAR5_BASE,
        &body,
        VOLUMES,
        |volume| match volume {
            0 => (MEMBER, size),
            last if last + 1 == VOLUMES => (MEMBER, size),
            _ => (MEMBER, size + u64::from(OVERSTATED)),
        },
        checksum::crc32(&body),
    );
    let outcome = run(&rar5_posted(set)).await;
    assert!(outcome.completed(), "{}", describe(&outcome));
    assert_eq!(
        outcome.files.get(&delivered(MEMBER)).map(Vec::as_slice),
        Some(body.as_slice()),
        "delivered {:?}",
        outcome.files.keys().collect::<Vec<_>>()
    );
}

// The set with every part stating the size `stated` gives it, its last part
// stating the member's true CRC, posted as the film's was.
fn stating(body: &[u8], stated: impl Fn(usize, u32) -> u32) -> Scenario {
    let size = body.len() as u32;
    posted(rar4_misstated_set(
        TRUE_BASE,
        MEMBER,
        body,
        VOLUMES,
        |volume| stated(volume, size),
        checksum::crc32(body),
    ))
}

// Parts that carry more bytes than every part states: unrar writes only the
// stated size, which fails the CRC of the whole member, so the job fails.
#[tokio::test]
async fn a_set_whose_every_part_understates_its_size_fails() {
    let body = body();
    let outcome = run(&stating(&body, |_, size| size - 4096)).await;
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{}",
        describe(&outcome)
    );
    assert!(
        outcome
            .graph()
            .delivery
            .whole
            .iter()
            .all(|name| name != MEMBER),
        "{}",
        describe(&outcome)
    );
}

// The 548b set with no recovery, its last two volumes posted under each
// other's names, so nothing renames them: the store orders the two by
// their headers once the job is quiet, linking the parts by the name they
// give the member as the claim does, and the member is delivered whole.
#[tokio::test]
async fn a_set_whose_middle_parts_overstate_its_size_delivers_with_its_last_two_swapped() {
    let body = body();
    let set = set(&body, checksum::crc32(&body));
    let posts = set
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            let posted = match index {
                0 => return Post::plain("Final.Girl.x264-ULSHD.part01.rar", bytes.clone()),
                last if last + 1 == VOLUMES => last - 1,
                before_last if before_last + 2 == VOLUMES => before_last + 1,
                middle => middle,
            };
            Post::plain(&format!("65606-1.r{:02}", posted - 1), bytes.clone())
        })
        .collect();
    let outcome = run(&Scenario::new(posts).framed(FRAME)).await;
    assert!(outcome.completed(), "{}", describe(&outcome));
    assert_eq!(
        outcome.files.get(&delivered(MEMBER)).map(Vec::as_slice),
        Some(body.as_slice()),
        "delivered {:?}",
        outcome.files.keys().collect::<Vec<_>>()
    );
}

// A set whose first part states more than its parts carry, every part
// alike or the first alone. unrar writes a stored member until its parts
// end, never past the size the first part states (CmdExtract::UnstoreFile,
// extract.cpp), so it writes every byte the parts carry and checks them
// against the CRC the last part states: the member is whole at that sum.
const SHAPES: [&str; 2] = ["every part", "the first part"];

fn rar4_overstating(body: &[u8], shape: &str, crc: u32) -> Scenario {
    let size = body.len() as u32;
    posted(rar4_misstated_set(
        TRUE_BASE,
        MEMBER,
        body,
        VOLUMES,
        |volume| match (shape, volume) {
            ("every part", _) | (_, 0) => size + OVERSTATED,
            _ => size,
        },
        crc,
    ))
}

fn rar5_overstating(body: &[u8], shape: &str, crc: u32) -> Scenario {
    let size = body.len() as u64;
    let over = size + u64::from(OVERSTATED);
    rar5_posted(rar5_misstated_set(
        RAR5_BASE,
        body,
        VOLUMES,
        |volume| match (shape, volume) {
            ("every part", _) | (_, 0) => (MEMBER, over),
            _ => (MEMBER, size),
        },
        crc,
    ))
}

fn assert_whole(outcome: &Outcome, body: &[u8], case: &str) {
    assert!(outcome.completed(), "{case}: {}", describe(outcome));
    assert_eq!(
        outcome.files.get(&delivered(MEMBER)).map(Vec::as_slice),
        Some(body),
        "{case}: delivered {:?}",
        outcome.files.keys().collect::<Vec<_>>()
    );
    assert!(
        outcome.graph().delivery.withheld.is_empty(),
        "{case}: {}",
        describe(outcome)
    );
}

fn assert_failed(outcome: &Outcome, case: &str) {
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{case}: {}",
        describe(outcome)
    );
    assert!(
        outcome
            .graph()
            .delivery
            .whole
            .iter()
            .all(|name| name != MEMBER),
        "{case}: {}",
        describe(outcome)
    );
}

#[tokio::test]
async fn a_set_whose_first_part_overstates_its_size_delivers_the_bytes_its_parts_carry() {
    let body = body();
    let crc = checksum::crc32(&body);
    for shape in SHAPES {
        assert_whole(
            &run(&rar4_overstating(&body, shape, crc)).await,
            &body,
            shape,
        );
    }
}

#[tokio::test]
async fn a_rar5_set_whose_first_part_overstates_its_size_delivers_the_bytes_its_parts_carry() {
    let body = body();
    let crc = checksum::crc32(&body);
    for shape in SHAPES {
        assert_whole(
            &run(&rar5_overstating(&body, shape, crc)).await,
            &body,
            shape,
        );
    }
}

#[tokio::test]
async fn a_set_whose_first_part_overstates_its_size_and_whose_bytes_fail_its_crc_fails() {
    let body = body();
    let crc = checksum::crc32(&body) ^ 1;
    for shape in SHAPES {
        assert_failed(&run(&rar4_overstating(&body, shape, crc)).await, shape);
        let rar5 = format!("RAR5 {shape}");
        assert_failed(&run(&rar5_overstating(&body, shape, crc)).await, &rar5);
    }
}

// A restart after every article ends the job as a run without one does.
async fn delivers_restarted(scenario: Scenario, body: &[u8]) {
    let restarted = super::restart::run_restarting(&scenario, &mut |_| {}).await;
    assert!(restarted.restarts > 0);
    assert_eq!(restarted.status, Some(JobStatus::Complete));
    assert_eq!(
        restarted.files.get(&delivered(MEMBER)).map(Vec::as_slice),
        Some(body),
        "delivered {:?}",
        restarted.files.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_set_whose_every_part_overstates_its_size_restarted_midway_delivers_the_same() {
    let body = body();
    let crc = checksum::crc32(&body);
    delivers_restarted(rar4_overstating(&body, "every part", crc), &body).await;
}

#[tokio::test]
async fn a_rar5_set_whose_first_part_overstates_its_size_restarted_midway_delivers_the_same() {
    let body = body();
    let crc = checksum::crc32(&body);
    delivers_restarted(rar5_overstating(&body, "the first part", crc), &body).await;
}

// A set stating its member truly is delivered at that size, as before.
#[tokio::test]
async fn a_set_stating_its_size_truly_delivers_it_unchanged() {
    let body = body();
    assert_whole(&run(&stating(&body, |_, size| size)).await, &body, "true");
}
