// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR set posted under names that number nothing, its last volume posted
// first, with no recovery set and beside a PAR2 set that names its volumes:
// every volume arrives whole, so the job completes with the member as
// written.
//
// Run on 2026-11-18, each volume's articles served in order:
// - the stored three-volume set, no PAR2: Failed, "never placed every member
//   it named". The last volume, given back from the store it founded, is
//   offered first to the store the first volume founds, and the lowest free
//   place its links fit is the second; the middle volume then fits no place.
// - the compressed four-volume set, whose end records state each volume's
//   number, no PAR2: Failed, "no file was delivered whole". The tree alone
//   places all four in one engine; in the server the first and last volumes
//   end damaged, though every article arrived whole.
// - the compressed set beside PAR2: Failed, "unable to find
//   silver.horizon.part5.rar".
// The stored set beside PAR2, and every case served last article first,
// complete.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use super::rar_rev::lz_four;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::single_member_rar4_old_numbering_set;

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// A stored three-volume RAR4 set of one member whose headers state no
// volume number, and the compressed four-volume set, each with its member
// and that member's bytes where the test knows them.
fn sets() -> Vec<(
    &'static str,
    Vec<(String, Vec<u8>)>,
    &'static str,
    Option<Vec<u8>>,
)> {
    let body = payload(9 * ARTICLE, 83);
    vec![
        (
            "stored",
            single_member_rar4_old_numbering_set("silver.horizon", "beacon.bin", &body, 3),
            "beacon.bin",
            Some(body),
        ),
        ("lz_four", lz_four(), "compressible.txt", None),
    ]
}

// The set's volumes posted last first, each under a name that numbers
// nothing, beside a PAR2 set over the volumes as named when `par2`; each
// batch the queue hands out served last article first when `backwards`.
fn reversed(volumes: &[(String, Vec<u8>)], par2: bool, backwards: bool) -> Scenario {
    let mut posts: Vec<Post> = volumes
        .iter()
        .enumerate()
        .rev()
        .map(|(volume, (_, bytes))| {
            Post::plain(&format!("{:032x}", 0x0b5c_0000 + volume), bytes.clone())
        })
        .collect();
    if par2 {
        let files: Vec<(&str, &[u8])> = volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        posts.extend(par2_set("silver.horizon", &files, 4));
    }
    let mut scenario = Scenario::new(posts);
    scenario.article = 4096;
    match backwards {
        true => scenario.backwards(),
        false => scenario,
    }
}

fn said(case: &str, outcome: &Outcome) {
    eprintln!(
        "CASE {case}: status {:?} kind {:?} failure {:?} files {:?} claims {:?}",
        outcome.status,
        outcome.failure_kind,
        outcome.failure,
        outcome.files.keys().collect::<Vec<_>>(),
        outcome.graph.claims,
    );
}

#[tokio::test]
#[ignore = "a nameless RAR set posted last volume first fails: stored without PAR2, compressed with and without"]
async fn a_nameless_rar_set_posted_last_volume_first_completes() {
    let mut failed = Vec::new();
    for (label, volumes, member, body) in sets() {
        for par2 in [false, true] {
            for backwards in [false, true] {
                let case = format!("{label} par2 {par2} backwards {backwards}");
                let outcome = run(&reversed(&volumes, par2, backwards)).await;
                said(&case, &outcome);
                let whole = outcome
                    .files
                    .get(&delivered(member))
                    .is_some_and(|bytes| body.as_ref().is_none_or(|body| bytes == body));
                if outcome.status != Some(JobStatus::Complete) || !whole {
                    failed.push(case);
                }
            }
        }
    }
    assert!(failed.is_empty(), "{failed:?}");
}
