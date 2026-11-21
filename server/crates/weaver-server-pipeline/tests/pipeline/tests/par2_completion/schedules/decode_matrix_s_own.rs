// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a set reaches its recovery: a recovery article refused or spoiled on
// its first ask, a set whose index never arrives, a damaged file beside clean
// ones, and a repair taken down while it runs.

use super::{Expect, articles, check, covered, lose_all, row};
use crate::history::HistoryFailureKind;
use crate::pipeline::tests::engines::harness::{
    ARTICLE, Post, Scenario, one_block_article, par2_volumes, payload, run,
};
use crate::pipeline::tests::engines::restart::run_restarted_while_repairing;

// A recovery article the server refuses on its first ask goes back on the
// queue still wanted, and the repair waits for it rather than ruling short.
#[tokio::test]
async fn a_refused_recovery_article_is_asked_again_and_repairs() {
    let body = payload(5 * ARTICLE, 111);
    let posts = covered(&[("lantern.bin", &body)], 2, 1);
    let ask = one_block_article(&posts[2]);
    let scenario = Scenario::new(posts)
        .damaged(0, 1)
        .damaged(0, 3)
        .refused_once(2, ask);
    let outcome = row(
        &scenario,
        Expect::Complete(&[("lantern.bin", &body)]),
        "refused once",
    )
    .await;
    assert!(
        outcome.requests[&(2, ask)] >= 2,
        "the refused article was asked again"
    );
}

// A recovery article the primary spoils is fetched whole from the backup.
#[tokio::test]
async fn a_spoiled_recovery_article_is_fetched_from_the_backup() {
    let body = payload(5 * ARTICLE, 112);
    let posts = covered(&[("lantern.bin", &body)], 2, 1);
    let spoiled = one_block_article(&posts[2]);
    row(
        &Scenario::new(posts)
            .damaged(0, 2)
            .corrupt_on_primary(2, spoiled),
        Expect::Complete(&[("lantern.bin", &body)]),
        "spoiled on the primary",
    )
    .await;
}

// A short unprotected file beside a covered one: the set repairs what it
// covers, and the file it does not cover, posted whole, is delivered as it
// came without holding the job back.
#[tokio::test]
async fn a_short_unprotected_file_does_not_hold_back_the_repair() {
    let body = payload(5 * ARTICLE, 113);
    let notes = payload(ARTICLE / 2, 114);
    let mut posts = covered(&[("lantern.bin", &body)], 2, 1);
    posts.push(Post::plain("notes.txt", notes.clone()));
    row(
        &Scenario::new(posts).damaged(0, 3),
        Expect::Complete(&[("lantern.bin", &body), ("notes.txt", &notes)]),
        "unprotected beside",
    )
    .await;
}

// Every article of the index lost: the set's description comes from its
// volumes, which carry the same packets, and the damage is repaired.
#[tokio::test]
async fn a_lost_index_is_replaced_by_the_packets_its_volumes_carry() {
    let body = payload(6 * ARTICLE, 115);
    let scenario = lose_all(
        Scenario::new(covered(&[("lantern.bin", &body)], 3, 2)).damaged(0, 2),
        1,
    );
    row(
        &scenario,
        Expect::Complete(&[("lantern.bin", &body)]),
        "index lost",
    )
    .await;
}

// The first volume the set asks for is missing on every server: the set
// turns to the next one, and fails only once no volume can bring the
// blocks it is short of.
#[tokio::test]
async fn a_volume_missing_everywhere_moves_the_set_to_the_next_volume() {
    let body = payload(6 * ARTICLE, 116);
    let posts = covered(&[("lantern.bin", &body)], 4, 2);
    let enough = lose_all(Scenario::new(covered(&[("lantern.bin", &body)], 4, 2)), 2)
        .damaged(0, 1)
        .damaged(0, 4);
    row(
        &enough,
        Expect::Complete(&[("lantern.bin", &body)]),
        "first volume missing",
    )
    .await;
    let short = lose_all(lose_all(Scenario::new(posts), 2), 3)
        .damaged(0, 1)
        .damaged(0, 4);
    row(
        &short,
        Expect::Failed(HistoryFailureKind::Repair),
        "every volume missing",
    )
    .await;
}

// One damaged file among three under one set: the clean files are never
// asked for again, and the job completes with all three whole.
#[tokio::test]
async fn a_damaged_file_beside_clean_ones_repairs_without_refetching_them() {
    let first = payload(3 * ARTICLE, 117);
    let second = payload(4 * ARTICLE + 9, 118);
    let third = payload(2 * ARTICLE + 1, 119);
    let files: [(&str, &[u8]); 3] = [
        ("first.bin", &first),
        ("second.bin", &second),
        ("third.bin", &third),
    ];
    let outcome = row(
        &Scenario::new(covered(&files, 2, 1)).damaged(1, 2),
        Expect::Complete(&files),
        "one damaged among three",
    )
    .await;
    for file in [0u32, 2] {
        for ordinal in 0..articles(&Scenario::new(covered(&files, 2, 1)).posts[file as usize]) {
            assert_eq!(
                outcome.requests.get(&(file, ordinal)),
                Some(&1),
                "clean article {file}:{ordinal} asked once"
            );
        }
    }
}

// The job's articles arrive last first, so the damage is seen before the
// set's index: the set rules once its downloads drain, on everything.
#[tokio::test]
async fn a_damaged_job_rules_once_its_downloads_drain() {
    let body = payload(7 * ARTICLE + 3, 120);
    row(
        &Scenario::new(covered(&[("lantern.bin", &body)], 2, 1))
            .damaged(0, 5)
            .backwards(),
        Expect::Complete(&[("lantern.bin", &body)]),
        "last article first",
    )
    .await;
}

// A posted name the set does not give: the set binds the file by its
// contents, repairs it, and delivers it under the name it describes.
#[tokio::test]
async fn a_damaged_file_under_another_name_is_repaired_under_its_own() {
    let body = payload(5 * ARTICLE, 121);
    let mut posts = covered(&[("lantern.bin", &body)], 2, 1);
    posts[0] = Post::plain("a1b2c3d4e5", body.clone());
    row(
        &Scenario::new(posts).damaged(0, 1),
        Expect::Complete(&[("lantern.bin", &body)]),
        "obfuscated and damaged",
    )
    .await;
}

// The pipeline drained and taken down while the repair is out on a thread:
// the restored job repairs again from what is on disk and completes.
#[tokio::test]
async fn a_shutdown_while_repairing_leaves_the_job_resumable() {
    let body = payload(6 * ARTICLE, 122);
    let scenario = Scenario::new(covered(&[("lantern.bin", &body)], 2, 1)).damaged(0, 2);
    let whole = run(&scenario).await;
    check(
        &whole,
        &Expect::Complete(&[("lantern.bin", &body)]),
        "posting order",
    );
    let (restarted, _) = run_restarted_while_repairing(&scenario).await;
    assert_eq!(restarted.status, whole.status, "status");
    assert_eq!(restarted.files, whole.files, "delivery");
}

// A set damaged far past its blocks: the set rules impossible at once and
// the job fails as a repair, with nothing delivered.
#[tokio::test]
async fn a_set_damaged_far_past_its_blocks_fails_as_repair() {
    let body = payload(12 * ARTICLE, 123);
    let mut scenario = Scenario::new(par2_volumes_over(&body, 2));
    for ordinal in 0..12 {
        scenario = scenario.damaged(0, ordinal);
    }
    row(
        &scenario,
        Expect::Failed(HistoryFailureKind::Repair),
        "every slice damaged",
    )
    .await;
}

fn par2_volumes_over(body: &[u8], blocks: u32) -> Vec<Post> {
    let mut posts = vec![Post::plain("lantern.bin", body.to_vec())];
    posts.extend(par2_volumes("lantern", &[("lantern.bin", body)], blocks, 1));
    posts
}
