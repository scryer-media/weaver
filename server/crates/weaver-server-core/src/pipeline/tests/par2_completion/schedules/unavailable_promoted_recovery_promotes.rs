// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Recovery the job cannot get, and jobs that lose articles: the graph has no
// health abort, so a job short of articles is judged by its sets and its
// files, never by a count of what went missing.

use super::{Expect, covered, lose_all, row};
use crate::history::HistoryFailureKind;
use crate::pipeline::tests::direct_store::single_member_store_set;
use crate::pipeline::tests::engines::harness::{ARTICLE, Post, Scenario, par2_volumes, payload};

// The first volume missing on every server: the next one is asked for
// before the set rules short, and it brings the blocks.
#[tokio::test]
async fn unavailable_recovery_moves_to_the_next_volume_before_failing() {
    let body = payload(6 * ARTICLE, 171);
    row(
        &lose_all(
            Scenario::new(covered(&[("lantern.bin", &body)], 6, 2)).damaged(0, 3),
            2,
        ),
        Expect::Complete(&[("lantern.bin", &body)]),
        "first volume missing",
    )
    .await;
}

// More damage than the set holds blocks for: the set rules short and the
// job fails as a repair with nothing delivered.
#[tokio::test]
async fn a_repair_fails_when_recovery_is_insufficient() {
    let body = payload(6 * ARTICLE, 172);
    row(
        &Scenario::new(covered(&[("lantern.bin", &body)], 2, 1))
            .damaged(0, 0)
            .damaged(0, 2)
            .damaged(0, 4),
        Expect::Failed(HistoryFailureKind::Repair),
        "three damaged against two blocks",
    )
    .await;
}

// A clean RAR set whose PAR2 index never arrives: nothing needs the set, so
// the member extracts and the job completes.
#[tokio::test]
async fn a_clean_archive_completes_without_its_lost_index() {
    let member = payload(7 * ARTICLE + 11, 173);
    let volumes = single_member_store_set("ember.bin", &member, 2);
    let described: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = volumes
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_volumes("silver.horizon", &described, 2, 1));
    row(
        &lose_all(Scenario::new(posts), 2),
        Expect::Complete(&[("ember.bin", &member)]),
        "index lost, nothing damaged",
    )
    .await;
}

// Most of a file's articles missing and no set: no file is whole, so the
// job fails, filed as the unpack it could not make rather than as a health
// verdict.
#[tokio::test]
async fn most_articles_missing_without_par2_fails_with_no_file_whole() {
    let body = payload(10 * ARTICLE, 174);
    let mut scenario = Scenario::new(vec![Post::plain("lantern.bin", body)]);
    for ordinal in 2..8 {
        scenario = scenario.lost(0, ordinal);
    }
    row(
        &scenario,
        Expect::Failed(HistoryFailureKind::Health),
        "six of ten missing",
    )
    .await;
}

// The same losses under a set with blocks for them: the job is not cut
// short for what it lost, and the set rebuilds the file.
#[tokio::test]
async fn most_articles_missing_with_par2_is_repaired() {
    let body = payload(10 * ARTICLE, 175);
    let mut scenario = Scenario::new(covered(&[("lantern.bin", &body)], 8, 2));
    for ordinal in 2..8 {
        scenario = scenario.lost(0, ordinal);
    }
    row(
        &scenario,
        Expect::Complete(&[("lantern.bin", &body)]),
        "six of ten missing, eight blocks",
    )
    .await;
}

// The same losses with every recovery volume missing too: the set is
// starved and fails as a repair.
#[tokio::test]
async fn most_articles_missing_without_obtainable_recovery_fails_as_repair() {
    let body = payload(10 * ARTICLE, 176);
    let mut scenario = Scenario::new(covered(&[("lantern.bin", &body)], 8, 2));
    for ordinal in 2..8 {
        scenario = scenario.lost(0, ordinal);
    }
    row(
        &lose_all(lose_all(scenario, 2), 3),
        Expect::Failed(HistoryFailureKind::Repair),
        "six of ten missing, no volume",
    )
    .await;
}

// A clean set completes with every file whole.
#[tokio::test]
async fn a_clean_job_verifies_and_completes() {
    let body = payload(5 * ARTICLE + 1, 177);
    row(
        &Scenario::new(covered(&[("lantern.bin", &body)], 2, 1)),
        Expect::Complete(&[("lantern.bin", &body)]),
        "clean",
    )
    .await;
}

// A file the set covers, posted under an unrelated name and with an article
// lost: bound by its contents, repaired and renamed.
#[tokio::test]
async fn an_obfuscated_payload_reconciles_through_its_set() {
    let body = payload(5 * ARTICLE, 178);
    let mut posts = covered(&[("lantern.bin", &body)], 2, 1);
    posts[0] = Post::plain("0a9b8c7d6e5f", body.clone());
    row(
        &Scenario::new(posts).lost(0, 2),
        Expect::Complete(&[("lantern.bin", &body)]),
        "obfuscated, one lost",
    )
    .await;
}

// A covered payload whole and an unprotected file damaged beside it: the
// unprotected file is not whole and is not delivered, the payload is, and
// the job is partial.
#[tokio::test]
async fn an_unprotected_damaged_file_leaves_the_job_partial() {
    let body = payload(5 * ARTICLE, 179);
    let notes = payload(3 * ARTICLE, 180);
    let mut posts = covered(&[("lantern.bin", &body)], 2, 1);
    posts.push(Post::plain("notes.txt", notes.clone()));
    let at = posts.len() as u32 - 1;
    let outcome = row(
        &Scenario::new(posts).damaged(at, 1),
        Expect::Partial(&[("lantern.bin", &body)]),
        "unprotected damaged",
    )
    .await;
    // Delivered as it stands: not the posted bytes.
    assert!(super::delivered(&outcome.files, "notes.txt").is_some_and(|bytes| *bytes != notes));
}

// An unprotected file lost whole beside a covered payload: the payload is
// delivered and the job is partial. No article of the file arrived, so no
// file stands in for it.
#[tokio::test]
async fn a_missing_unprotected_file_leaves_the_job_partial() {
    let body = payload(5 * ARTICLE, 181);
    let mut posts = covered(&[("lantern.bin", &body)], 2, 1);
    posts.push(Post::plain("notes.txt", payload(3 * ARTICLE, 182)));
    let at = posts.len() as u32 - 1;
    let outcome = row(
        &lose_all(Scenario::new(posts), at),
        Expect::Partial(&[("lantern.bin", &body)]),
        "unprotected missing",
    )
    .await;
    assert_eq!(super::delivered(&outcome.files, "notes.txt"), None);
}
