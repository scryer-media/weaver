// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Encrypted stored RAR sets in the shapes posts carry them beyond the
// wrong-then-right trials: a RAR4 `-p` set written with no salt, a RAR4
// `-p` set and no password at all, a header-encrypted RAR5 set opened by the
// last of several candidates, encrypted sets restarted after every article,
// and encrypted sets that lost a whole volume beside PAR2.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use super::rar::{delivered, posts};
use super::restart::restart_row;
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_rar4_store_set, encrypted_store_set, header_encrypted_store_set,
};

const MEMBER: &str = "beacon.bin";
const SECRET: &str = "moonlit-harbour";

fn body() -> Vec<u8> {
    payload(9 * ARTICLE + 21, 81)
}

fn assert_member(outcome: &Outcome, body: &[u8]) {
    assert!(
        outcome.completed(),
        "{:?} {:?} {:?}",
        outcome.status,
        outcome.failure,
        outcome.files.keys().collect::<Vec<_>>()
    );
    let names: Vec<String> = outcome
        .files
        .keys()
        .filter(|name| !name.ends_with(".par2"))
        .cloned()
        .collect();
    assert_eq!(names, vec![delivered(MEMBER)], "delivered names");
    assert!(outcome.files[&delivered(MEMBER)] == body, "member bytes");
}

// The volumes, then an index and one volume of `blocks` recovery blocks.
fn with_par2(volumes: Vec<(String, Vec<u8>)>, blocks: u32) -> Vec<Post> {
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let set = par2_set("moonlit", &files, blocks);
    let mut out = posts(volumes.clone());
    out.extend(set);
    out
}

// Every article of post `file` lost.
fn whole_post_lost(mut scenario: Scenario, file: u32, len: usize) -> Scenario {
    for ordinal in 0..len.div_ceil(ARTICLE) as u32 {
        scenario = scenario.lost(file, ordinal);
    }
    scenario
}

// RAR4 derives its key from the password alone when the archive carries no
// salt, as WinRAR 2.x wrote `-p` archives: the job's password opens it.
#[tokio::test]
async fn a_saltless_rar4_set_opens_with_the_job_password() {
    let volumes = encrypted_rar4_store_set(MEMBER, &body(), 3, SECRET, None);
    let graph = run(&Scenario::new(posts(volumes)).password(SECRET)).await;
    assert_member(&graph, &body());
}

#[tokio::test]
async fn a_saltless_rar4_set_opens_with_a_global_candidate() {
    let volumes = encrypted_rar4_store_set(MEMBER, &body(), 3, SECRET, None);
    let graph = run(&Scenario::new(posts(volumes)).global_password(SECRET)).await;
    assert_member(&graph, &body());
}

// A RAR4 `-p` set and no password from anywhere: the job fails for the
// password, naming the volumes, nothing delivered and nothing refetched.
#[tokio::test]
async fn a_rar4_set_with_no_password_fails_naming_the_password() {
    let volumes = encrypted_rar4_store_set(MEMBER, &body(), 3, SECRET, Some([5; 8]));
    let names: Vec<String> = volumes.iter().map(|(name, _)| name.clone()).collect();
    let graph = run(&Scenario::new(posts(volumes))).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?} {:?}",
        graph.status,
        graph.failure
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Password));
    assert_eq!(
        graph.failure,
        Some(format!(
            "WEAVER_PASSWORD_REQUIRED: {} needs a password and none was given",
            names.join(", ")
        ))
    );
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A header-encrypted RAR5 set offered three candidates, the right one last:
// its header check turns the two wrong ones away and the third opens it.
#[tokio::test]
async fn a_header_encrypted_set_opens_with_the_last_of_several_candidates() {
    let volumes = header_encrypted_store_set(MEMBER, &body(), 3, SECRET, HeaderCheck::For(SECRET));
    let scenario = Scenario::new(posts(volumes))
        .global_password("moonlit-harbor")
        .global_password("MOONLIT-HARBOUR")
        .global_password(SECRET);
    assert_member(&run(&scenario).await, &body());
}

// The header-encrypted set with the job's password, every article served
// last first: the volume headers land after the bytes behind them.
#[tokio::test]
async fn a_header_encrypted_set_served_tail_first_delivers() {
    let volumes = header_encrypted_store_set(MEMBER, &body(), 3, SECRET, HeaderCheck::For(SECRET));
    let scenario = Scenario::new(posts(volumes)).password(SECRET).backwards();
    assert_member(&run(&scenario).await, &body());
}

#[tokio::test]
async fn a_rar4_set_with_its_password_restarted_after_every_article_delivers_alike() {
    let volumes = encrypted_rar4_store_set(MEMBER, &body(), 3, SECRET, Some([5; 8]));
    restart_row(Scenario::new(posts(volumes)).password(SECRET)).await;
}

#[tokio::test]
async fn a_header_encrypted_set_restarted_after_every_article_delivers_alike() {
    let volumes = header_encrypted_store_set(MEMBER, &body(), 3, SECRET, HeaderCheck::For(SECRET));
    restart_row(Scenario::new(posts(volumes)).password(SECRET)).await;
}

// An encrypted RAR5 set whose middle volume never came, beside PAR2 with
// recovery enough for it: the volume is rebuilt and the member delivered.
#[tokio::test]
async fn an_encrypted_rar5_set_missing_its_middle_volume_is_rebuilt_by_par2() {
    let volumes = encrypted_store_set(MEMBER, &body(), 3, SECRET, Some(SECRET), false);
    let middle = volumes[1].1.len();
    let blocks = middle.div_ceil(ARTICLE) as u32 + 1;
    let scenario = whole_post_lost(
        Scenario::new(with_par2(volumes, blocks)).password(SECRET),
        1,
        middle,
    );
    assert_member(&run(&scenario).await, &body());
}

#[tokio::test]
async fn a_header_encrypted_set_missing_its_last_volume_is_rebuilt_by_par2() {
    let volumes = header_encrypted_store_set(MEMBER, &body(), 3, SECRET, HeaderCheck::For(SECRET));
    let last = volumes[2].1.len();
    let blocks = last.div_ceil(ARTICLE) as u32 + 1;
    let scenario = whole_post_lost(
        Scenario::new(with_par2(volumes, blocks)).password(SECRET),
        2,
        last,
    );
    assert_member(&run(&scenario).await, &body());
}

#[tokio::test]
async fn a_rar4_set_missing_its_first_volume_is_rebuilt_by_par2() {
    let volumes = encrypted_rar4_store_set(MEMBER, &body(), 3, SECRET, Some([5; 8]));
    let first = volumes[0].1.len();
    let blocks = first.div_ceil(ARTICLE) as u32 + 1;
    let scenario = whole_post_lost(
        Scenario::new(with_par2(volumes, blocks)).password(SECRET),
        0,
        first,
    );
    assert_member(&run(&scenario).await, &body());
}
