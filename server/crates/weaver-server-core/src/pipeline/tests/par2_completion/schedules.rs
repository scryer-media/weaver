// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The rows' shared shape, and the rows on which side of a verdict a new
// arrival or a restart lands. A row runs its job in posting order, holds the
// outcome to what it expects, then runs the job again restarted after every
// article: the restarted run must finish as the first did and ask again only
// for what a restart found in memory.

use std::collections::BTreeMap;

use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::engines::harness::{
    ARTICLE, Outcome, Post, Scenario, par2_volumes, payload, run,
};
use crate::pipeline::tests::engines::restart::{Restarted, run_restarting};

mod decode_matrix_s_own;
mod ignorable_furniture_inside_recovery;
mod par2_session_io_errors;
mod stall_regressions;
mod unavailable_promoted_recovery_promotes;

// What a row's job must come to.
pub(super) enum Expect<'a> {
    // Complete, each named file delivered with these bytes.
    Complete(&'a [(&'a str, &'a [u8])]),
    // Partial, each named file delivered with these bytes.
    Partial(&'a [(&'a str, &'a [u8])]),
    // Failed under this kind, with nothing delivered.
    Failed(HistoryFailureKind),
}

// The delivered file that carries `name`, wherever the job put it.
pub(super) fn delivered<'a>(
    files: &'a BTreeMap<String, Vec<u8>>,
    name: &str,
) -> Option<&'a Vec<u8>> {
    files
        .iter()
        .find(|(path, _)| path.rsplit('/').next() == Some(name))
        .map(|(_, bytes)| bytes)
}

pub(super) fn check(outcome: &Outcome, expect: &Expect<'_>, context: &str) {
    let named = |files: &[(&str, &[u8])]| {
        for (name, bytes) in files {
            assert!(
                delivered(&outcome.files, name).map(Vec::as_slice) == Some(*bytes),
                "{context}: {name} not delivered whole: {:?}",
                outcome.files.keys().collect::<Vec<_>>()
            );
        }
    };
    match expect {
        Expect::Complete(files) => {
            assert_eq!(
                outcome.status,
                Some(JobStatus::Complete),
                "{context}: {:?}",
                outcome.failure
            );
            named(files);
        }
        Expect::Partial(files) => {
            assert_eq!(
                outcome.status,
                Some(JobStatus::Partial),
                "{context}: {:?}",
                outcome.failure
            );
            named(files);
        }
        Expect::Failed(kind) => {
            assert!(
                matches!(outcome.status, Some(JobStatus::Failed { .. })),
                "{context}: {:?}",
                outcome.status
            );
            assert_eq!(
                outcome.failure_kind,
                Some(*kind),
                "{context}: {:?}",
                outcome.failure
            );
            assert!(
                outcome.files.is_empty(),
                "{context}: a failed job delivered {:?}",
                outcome.files.keys().collect::<Vec<_>>()
            );
        }
    }
}

pub(super) fn agree(restarted: &Restarted, whole: &Outcome, context: &str) {
    assert_eq!(
        restarted.status, whole.status,
        "{context}: restarted status"
    );
    assert_eq!(
        restarted.files, whole.files,
        "{context}: restarted delivery"
    );
    assert!(
        restarted.from_disk.is_empty(),
        "{context}: fetched again though on disk: {:?}",
        restarted.from_disk
    );
}

// The job in posting order, held to `expect`, then restarted after every
// article and held to the first run.
pub(super) async fn row(scenario: &Scenario, expect: Expect<'_>, context: &str) -> Outcome {
    let whole = run(scenario).await;
    check(&whole, &expect, context);
    let restarted = run_restarting(scenario, &mut |_| {}).await;
    agree(&restarted, &whole, context);
    whole
}

// Plain files followed by one PAR2 set over them of `blocks` recovery blocks
// in `volumes` volumes: the files are numbered as listed, the index and the
// volumes after.
pub(super) fn covered(files: &[(&str, &[u8])], blocks: u32, volumes: u32) -> Vec<Post> {
    let mut posts: Vec<Post> = files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.to_vec()))
        .collect();
    posts.extend(par2_volumes("lantern", files, blocks, volumes));
    posts
}

// The number of articles a post is cut into.
pub(super) fn articles(post: &Post) -> u32 {
    post.bytes.len().div_ceil(ARTICLE) as u32
}

// Every article of post `file` lost.
pub(super) fn lose_all(mut scenario: Scenario, file: u32) -> Scenario {
    for ordinal in 0..articles(&scenario.posts[file as usize]) {
        scenario = scenario.lost(file, ordinal);
    }
    scenario
}

// A damaged source and a recovery volume that holds the one block it needs
// twice, a copy with its last byte flipped before or after the valid one.
// Whichever arrives first, the malformed copy never takes the valid one's
// place, and the repair it was asked for runs on the valid block.
#[tokio::test]
async fn a_corrupt_duplicate_cannot_displace_valid_recovery() {
    let body = payload(4 * ARTICLE, 101);
    for corrupt_first in [false, true] {
        let mut posts = covered(&[("lantern.bin", &body)], 1, 1);
        let valid = posts[2].bytes.clone();
        let mut corrupt = valid.clone();
        *corrupt.last_mut().unwrap() ^= 0x80;
        posts[2].bytes = if corrupt_first {
            [corrupt, valid].concat()
        } else {
            [valid, corrupt].concat()
        };
        row(
            &Scenario::new(posts).damaged(0, 2),
            Expect::Complete(&[("lantern.bin", &body)]),
            &format!("corrupt_first={corrupt_first}"),
        )
        .await;
    }
}

// A verdict is never kept across a change it did not see: the job is taken
// down after every article, between its set ruling and its repair too, and
// each restored job rules again on what reached disk.
#[tokio::test]
async fn a_restart_never_keeps_a_verdict_older_than_its_bytes() {
    let body = payload(6 * ARTICLE + 17, 102);
    row(
        &Scenario::new(covered(&[("lantern.bin", &body)], 2, 1))
            .damaged(0, 1)
            .damaged(0, 4),
        Expect::Complete(&[("lantern.bin", &body)]),
        "two damaged slices",
    )
    .await;
}

// An article the primary serves damaged and a backup serves whole: what
// the job rules on is the bytes it kept, never the copy it set aside.
#[tokio::test]
async fn a_rewritten_article_is_judged_on_its_new_bytes() {
    let body = payload(5 * ARTICLE, 103);
    row(
        &Scenario::new(covered(&[("lantern.bin", &body)], 2, 1)).corrupt_on_primary(0, 2),
        Expect::Complete(&[("lantern.bin", &body)]),
        "corrupt on the primary",
    )
    .await;
}
