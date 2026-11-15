// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Old-numbering RAR4 sets (`.rar`, `.r00`, `.r01`, ...) whose end records
// read as the RAR versions that wrote such sets wrote them: a CRC32 of the
// volume and seven reserved bytes, with or without the volume's number.

use super::harness::{ARTICLE, Scenario, payload, try_run};
use super::rar::{posts, with_par2};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::single_member_rar4_old_numbering_set_ending;

const MEMBER: &str = "beacon.bin";

// Enough articles that each of four volumes holds several.
fn body() -> Vec<u8> {
    payload(9 * ARTICLE + 17, 73)
}

// Every volume's end record states number 0: the store holds `.r00` at the
// place its name gives until its end record says it is not there, then
// gives it back. Nothing places the volumes after the first, so nothing
// usable comes out, and the job fails with a reason, never waiting.
async fn assert_fails_unplaced(posts: Vec<super::harness::Post>) {
    let outcome = try_run(&Scenario::new(posts))
        .await
        .unwrap_or_else(|stall| panic!("stalled at {:?}: {}", stall.status, stall.state));
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    assert!(
        outcome
            .failure
            .as_deref()
            .is_some_and(|failure| failure.contains("never placed every member it named")),
        "{:?}",
        outcome.failure
    );
    assert!(
        !outcome.files.keys().any(|name| name.ends_with(MEMBER)),
        "{:?}",
        outcome.files.keys().collect::<Vec<_>>()
    );
}

fn numbered_alike() -> Vec<(String, Vec<u8>)> {
    single_member_rar4_old_numbering_set_ending("silver.horizon", MEMBER, &body(), 4, Some(0))
}

#[tokio::test]
async fn a_set_whose_volumes_all_state_one_number_fails() {
    assert_fails_unplaced(posts(numbered_alike())).await;
}

// With a PAR2 set over the volumes, `.r00` given back must come again in
// full: the bytes the store placed of it before it gave it back are not in
// the file it gave back, and a set rules only on members that closed.
#[tokio::test]
async fn a_set_whose_volumes_all_state_one_number_fails_beside_its_par2() {
    assert_fails_unplaced(with_par2(numbered_alike())).await;
}
