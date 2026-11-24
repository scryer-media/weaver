// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Standalone RAR recovery volumes: a RAR5 or RAR4 set
// posted without some of its data volumes, beside `.rev` files that can
// rebuild them, or too few that cannot. The set is stored, so the member is
// whole only when every volume is.

use super::archive_scenario_fixtures::rar_rev::{
    MEMBER, posted, rar5_set, records_and_recovery_volumes_rebuild,
};
pub(super) use super::archive_scenario_fixtures::rar_rev::{assert_lz_four_rebuilt, rar3_rev};
use super::harness::run_holding;
use super::harness::{ARTICLE, Outcome, Post, Scenario, payload, run};
use super::rar::{delivered, posts};
pub(super) use super::rar_fixtures::{body, lz_four, rar5_revs};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::protected_rar4_store_set;
use crate::pipeline::tests::direct_store::scenarios::{HeaderCheck, header_encrypted_store_set};
use crate::pipeline::tests::direct_store::{protected_store_set, single_member_store_set};
use crate::pipeline::tests::{
    single_member_rar4_old_numbering_set, single_recovery_volume_for_rar4_set,
    with_rar4_header_crcs,
};
use weaver_graph::recovery::RepairOutcome;

const SECRET: &str = "quill-ember-secret";

// The member alone, every byte as written, after one ruling that rebuilt
// the absent volumes: the `.rev` files are spent and never delivered.
fn assert_rebuilt(outcome: &Outcome, body: &[u8], volumes: u64) {
    assert_eq!(
        outcome.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: volumes })]
    );
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered(MEMBER)],
        "delivered names",
    );
    assert!(outcome.files[&delivered(MEMBER)] == body, "member bytes",);
}

#[tokio::test]
async fn rar_rev_rebuilds_one_absent_rar5_volume_from_one_recovery_volume() {
    let (body, scenario) = rar5_set(&[1], 1);
    assert_rebuilt(&run(&scenario).await, &body, 1);
}

// A volume posted short of the size the set's table gives, its last
// article ending early and arriving clean. The volume takes the table's
// size, the set rebuilds it to that size, and the member is delivered
// whole.
#[tokio::test]
async fn rar_rev_rebuilds_a_rar5_volume_posted_short_of_its_table_to_its_size() {
    let body = body();
    let mut volumes = single_member_store_set(MEMBER, &body, 4);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 1);
    let short = volumes[3].1.len() - 24;
    volumes[3].1.truncate(short);
    let scenario = Scenario::new(posted(volumes, &[], revs));
    assert_rebuilt(&run(&scenario).await, &body, 1);
}

// A volume posted one article past the size the set's table gives, every
// byte arriving clean: the stray bytes do not stop the set or the unpack,
// and the member is delivered whole.
#[tokio::test]
async fn rar_rev_delivers_a_rar5_volume_posted_long_of_its_table() {
    let body = body();
    let mut volumes = single_member_store_set(MEMBER, &body, 4);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 1);
    volumes[1].1.extend_from_slice(&[0x5a; ARTICLE]);
    let scenario = Scenario::new(posted(volumes, &[], revs));
    let outcome = run(&scenario).await;
    assert!(
        outcome.completed(),
        "{:?} {:?} {:?}",
        outcome.status,
        outcome.failure,
        outcome.graph().outcomes
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered(MEMBER)],
        "delivered names",
    );
    assert!(outcome.files[&delivered(MEMBER)] == body, "member bytes");
}

#[tokio::test]
async fn rar_rev_rebuilds_two_absent_rar5_volumes_from_two_recovery_volumes() {
    let (body, scenario) = rar5_set(&[1, 3], 2);
    assert_rebuilt(&run(&scenario).await, &body, 2);
}

#[tokio::test]
async fn rar5_records_and_recovery_volumes_rebuild_damage_and_an_absent_volume() {
    // The recovery-record headers require four more payload bytes to keep
    // this set's longest volume even for RAR5 recovery-volume parity words.
    let body = payload(11 * ARTICLE + 33, 61);
    let volumes = protected_store_set(MEMBER, &body, 4, 1);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 1);
    records_and_recovery_volumes_rebuild(Scenario::new(posted(volumes, &[2], revs)), &body).await;
}

// A RAR5 archive posted under a recovery volume's name, after another
// archive. The other archive is founded first, and the `.rev` name, its
// bytes not yet come, founds a recovery set. Its front then signs an
// archive: bytes outrank names, so it leaves the set, opens as the archive
// it is and delivers its member.
#[tokio::test]
async fn rar_rev_named_member_whose_bytes_are_an_archive_opens_as_one() {
    let body = body();
    let other = payload(3 * ARTICLE + 7, 67);
    let mut posted: Vec<(String, Vec<u8>)> = single_member_store_set("other.bin", &other, 1)
        .into_iter()
        .map(|(_, bytes)| ("other.rar".to_owned(), bytes))
        .collect();
    posted.extend(
        single_member_store_set(MEMBER, &body, 1)
            .into_iter()
            .map(|(_, bytes)| ("x.part1.rev".to_owned(), bytes)),
    );
    let outcome = run(&Scenario::new(posts(posted))).await;
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    let mut names = outcome.files.keys().cloned().collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec![delivered(MEMBER), delivered("other.bin")]);
    assert!(outcome.files[&delivered(MEMBER)] == body, "member bytes");
    assert!(
        outcome.files[&delivered("other.bin")] == other,
        "other bytes"
    );
}

// A whole two-volume RAR5 set beside its one real recovery volume,
// `.part1.rev`, and a `.part2.rev` of the same stem whose bytes are a RAR4
// archive of their own, which the RAR5 set's store does not claim. The recovery set, founded by the real one, is
// offered the second by its name, but its front signs an archive: bytes
// outrank names, so the set keeps only the first and rules clean, and the
// second opens as the archive it is and delivers its member.
#[tokio::test]
async fn rar_rev_set_refuses_a_same_stem_member_whose_bytes_are_an_archive() {
    let body = body();
    let volumes = single_member_store_set(MEMBER, &body, 2);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 1);
    assert!(revs[0].name.ends_with(".part1.rev"));
    let other = payload(3 * ARTICLE + 7, 73);
    let archive = single_member_rar4_old_numbering_set("other", "other.bin", &other, 1)
        .into_iter()
        .map(|(_, bytes)| Post::plain("silver.horizon.part2.rev", with_rar4_header_crcs(bytes)));
    let mut posted = posted(volumes, &[], revs);
    posted.extend(archive);
    let outcome = run(&Scenario::new(posted)).await;
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.graph().rulings,
        vec![("rar_recovery".to_owned(), Some(RepairOutcome::Clean))]
    );
    let mut names = outcome.files.keys().cloned().collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec![delivered(MEMBER), delivered("other.bin")]);
    assert!(outcome.files[&delivered(MEMBER)] == body, "member bytes");
    assert!(
        outcome.files[&delivered("other.bin")] == other,
        "other bytes"
    );
}

#[tokio::test]
async fn rar_rev_rebuilds_one_absent_rar4_volume_from_its_recovery_volume() {
    let body = body();
    let volumes: Vec<(String, Vec<u8>)> =
        single_member_rar4_old_numbering_set("silver.horizon", MEMBER, &body, 4)
            .into_iter()
            .map(|(name, bytes)| (name, with_rar4_header_crcs(bytes)))
            .collect();
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let rev = Post::plain(
        "silver.horizon1.rev",
        single_recovery_volume_for_rar4_set(&bytes),
    );
    let scenario = Scenario::new(posted(volumes, &[2], vec![rev]));
    assert_rebuilt(&run(&scenario).await, &body, 1);
}

// A compressed RAR4 set as RAR writes it, under part numbering, its third
// volume never posted, beside the one RAR3 recovery volume RAR names
// `.part1.rev`. Its fronts state no volume number, so the fourth volume is
// placed by its name, not beside the second it would link to across the
// hole, and the rebuilt third takes the place its name gives it.
#[tokio::test]
async fn rar_rev_rebuilds_an_absent_volume_of_a_compressed_part_numbered_rar4_set() {
    let volumes = lz_four();
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let rev = Post::plain("silver.horizon.part1.rev", rar3_rev(&bytes));
    let mut scenario = Scenario::new(posted(volumes, &[2], vec![rev]));
    // Each volume over a few articles, as a post carries it: the fourth
    // volume's front reaches the engine before the third is rebuilt.
    scenario.article = 4096;
    assert_lz_four_rebuilt(&run(&scenario).await);
}

#[tokio::test]
async fn rar_rev_rebuilds_two_absent_header_encrypted_volumes() {
    let body = payload(11 * ARTICLE, 61);
    let volumes = header_encrypted_store_set(MEMBER, &body, 5, SECRET, HeaderCheck::For(SECRET));
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 2);
    let scenario = Scenario::new(posted(volumes, &[1, 2], revs)).password(SECRET);
    assert_rebuilt(&run(&scenario).await, &body, 2);
}

// Five volumes of an `-hp` set, its member's checksum keyed, as the set was
// made.
fn header_encrypted_set(body: &[u8]) -> Vec<(String, Vec<u8>)> {
    header_encrypted_store_set(MEMBER, body, 5, SECRET, HeaderCheck::For(SECRET))
}

// The volumes, the one at `altered` changed after the set was made: 16
// bytes in its middle, every article consistent with what was posted.
fn altered(mut volumes: Vec<(String, Vec<u8>)>, altered: usize) -> Vec<(String, Vec<u8>)> {
    let middle = volumes[altered].1.len() / 2;
    volumes[altered].1[middle..middle + 16]
        .iter_mut()
        .for_each(|byte| *byte ^= 0x5a);
    volumes
}

// With no recovery posted, the member fails its keyed checksum and nothing
// is delivered whole, as unrar reports the file's CRC failed: in a middle
// volume, whose part's own check fails, and in the last, which only the
// file's checksum covers.
#[tokio::test]
async fn an_altered_header_encrypted_set_without_recovery_fails_its_keyed_checksum() {
    let body = payload(45 * ARTICLE, 61);
    for volume in [1, 4] {
        let set = altered(header_encrypted_set(&body), volume);
        let graph = run(&Scenario::new(posts(set)).password(SECRET)).await;
        assert!(
            matches!(graph.status, Some(JobStatus::Failed { .. })),
            "volume {volume}: {:?} {:?}",
            graph.status,
            graph.failure
        );
        assert!(
            !graph.files.values().any(|bytes| *bytes == body),
            "volume {volume}: {:?}",
            graph.files.keys()
        );
    }
}

// Beside a recovery volume made with the set, the altered volume is rebuilt,
// and it alone, and the member comes out whole.
#[tokio::test]
async fn rar_rev_rebuilds_a_header_encrypted_volume_altered_after_the_set_was_made() {
    let body = payload(45 * ARTICLE, 61);
    let made = header_encrypted_set(&body);
    let bytes: Vec<&[u8]> = made.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    for volume in [1, 4] {
        let revs = rar5_revs("silver.horizon", &bytes, 1);
        let set = posted(altered(made.clone(), volume), &[], revs);
        assert_rebuilt(&run(&Scenario::new(set).password(SECRET)).await, &body, 1);
    }
}

// The same with nothing enciphered, the member's checksum its CRC32.
#[tokio::test]
async fn rar_rev_rebuilds_a_rar5_volume_altered_after_the_set_was_made() {
    let body = body();
    let made = single_member_store_set(MEMBER, &body, 4);
    let bytes: Vec<&[u8]> = made.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    for volume in [1, 3] {
        let revs = rar5_revs("silver.horizon", &bytes, 1);
        let set = posted(altered(made.clone(), volume), &[], revs);
        assert_rebuilt(&run(&Scenario::new(set)).await, &body, 1);
    }
}

#[tokio::test]
async fn rar_rev_short_of_recovery_volumes_fails_the_job_and_delivers_nothing() {
    let (_, scenario) = rar5_set(&[1, 2], 1);
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?} {:?}",
        graph.status,
        graph.failure
    );
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    let delivery = &graph.graph().delivery;
    assert!(delivery.whole.is_empty(), "{delivery:?}");
    assert!(matches!(
        graph.graph().outcomes.as_slice(),
        [Some(RepairOutcome::Impossible {
            damaged: 2,
            reachable: 1
        })]
    ));
}

#[tokio::test]
async fn rar4_records_and_recovery_volumes_rebuild_damage_and_an_absent_volume() {
    let body = body();
    let volumes = protected_rar4_store_set(MEMBER, &body, 4, 8);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let rev = Post::plain(
        "silver.horizon.part1.rev",
        single_recovery_volume_for_rar4_set(&bytes),
    );
    records_and_recovery_volumes_rebuild(Scenario::new(posted(volumes, &[2], vec![rev])), &body)
        .await;
}

// Two data volumes never posted and two recovery volumes, the server
// holding the second's every article. The set, founded by the first, took
// the second by its posted name and counts it as coming: one volume in hand
// is short of the two it needs, so it waits rather than ruling short, and
// the job reads as fetching repair data until the server lets it go.
#[tokio::test]
async fn rar_rev_held_recovery_volume_reads_as_fetching_repair_data() {
    let (body, scenario) = rar5_set(&[1, 3], 2);
    let last = scenario.posts.len() as u32 - 1;
    assert!(scenario.posts[last as usize].name.ends_with(".part2.rev"));
    let held = run_holding(&scenario, last).await;
    assert_eq!(held.outcomes, vec![Some(RepairOutcome::Waiting)]);
    assert_eq!(held.report.status, JobStatus::Downloading);
    assert_eq!(
        held.report.post_state,
        weaver_server_composition::PostState::Idle
    );
    assert!(held.report.fetching_repair_data, "not fetching while held");
    assert!(
        held.after.iter().all(|(_, fetching)| !fetching),
        "still fetching once the volume came: {:?}",
        held.after
    );
    assert_eq!(held.status, Some(JobStatus::Complete));
    assert!(held.files[&delivered(MEMBER)] == body, "member bytes");
}

// A two-volume RAR5 set, its second volume damaged, beside the one
// recovery volume RAR wrote for it, the server holding that volume's every
// article. None of its bytes have come, so once the archive is founded the
// `.rev` founds its own recovery set by its posted name, and that set counts
// it as coming: it waits rather than ruling short, and the job reads as
// fetching repair data until the server lets the volume go and the set
// rebuilds the damaged volume.
#[tokio::test]
async fn rar_rev_lone_held_recovery_volume_reads_as_fetching_repair_data() {
    let body = body();
    let volumes = single_member_store_set(MEMBER, &body, 2);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 1);
    let scenario = Scenario::new(posted(volumes, &[], revs)).damaged(1, 1);
    let last = scenario.posts.len() as u32 - 1;
    assert_eq!(last, 2);
    assert!(scenario.posts[last as usize].name.ends_with(".part1.rev"));
    let held = run_holding(&scenario, last).await;
    assert_eq!(held.outcomes, vec![Some(RepairOutcome::Waiting)]);
    assert_eq!(held.report.status, JobStatus::Downloading);
    assert_eq!(
        held.report.post_state,
        weaver_server_composition::PostState::Idle
    );
    assert!(held.report.fetching_repair_data, "not fetching while held");
    assert!(
        held.after.iter().all(|(_, fetching)| !fetching),
        "still fetching once the volume came: {:?}",
        held.after
    );
    assert_eq!(held.status, Some(JobStatus::Complete));
    assert!(held.files[&delivered(MEMBER)] == body, "member bytes");
}
