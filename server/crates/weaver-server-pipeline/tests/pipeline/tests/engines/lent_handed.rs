// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A sealed RAR set's embedded records read their volumes under the candidate
// the store opened the headers on, which the store hands each record once.
// A record whose repair comes back after the loop moved something it read
// is rebuilt from its facts, which keep no candidate: the rebuilt record is
// handed the candidate before it hears anything, so it rules under it as the
// lent one did.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Post, Scenario, payload};
use crate::jobs::model::JobStatus;
use weaver_server_pipeline::pipeline::tests::direct_store::protected_sealed_rar4_store_set;
use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::protected_header_encrypted_store_set;
use weaver_server_pipeline::pipeline::tests::engines::recovery_actor::{
    Settled, run_settling_while_lent,
};

const MEMBER: &str = "beacon.bin";
const SECRET: &str = "quill-lantern-secret";

fn posts(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    volumes
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect()
}

// The last article of the post at `file`.
fn last_article(scenario: &Scenario, file: u32) -> (u32, u32) {
    let post = &scenario.posts[file as usize];
    (file, post.articles(scenario.article).count() as u32 - 1)
}

// The first volume's record repaired the damaged unit; the second found its
// volume whole. The member is delivered whole and nothing was asked twice.
fn assert_repaired_under_the_candidate(settled: &Settled, body: &[u8]) {
    assert_eq!(settled.status, Some(JobStatus::Complete));
    assert_eq!(
        settled.files.keys().cloned().collect::<Vec<_>>(),
        vec![format!("lantern beacon/{MEMBER}")]
    );
    assert!(settled.files[&format!("lantern beacon/{MEMBER}")] == body);
    assert_eq!(
        settled.graph.rulings,
        vec![
            (
                "rar_record".to_string(),
                Some(RepairOutcome::Repaired { slices: 1 })
            ),
            ("rar_record".to_string(), Some(RepairOutcome::Clean)),
        ]
    );
    let again: Vec<_> = settled
        .asked
        .iter()
        .filter(|(_, asked)| **asked > 1)
        .collect();
    assert!(again.is_empty(), "{again:?}");
}

// RAR4 `-hp -rr`: two volumes, each with a record of one parity sector, one
// article of the first served damaged. The second volume's last article
// lands while the first volume's record is out repairing, so the record
// comes back to a loop that moved what it read and is rebuilt from its
// facts.
#[tokio::test]
async fn a_sealed_record_rebuilt_from_its_facts_on_return_rules_under_the_candidate_its_store_handed()
 {
    let body = payload(7 * ARTICLE + 13, 51);
    let set = protected_sealed_rar4_store_set(MEMBER, &body, 2, 1, SECRET);
    let scenario = Scenario::new(posts(set)).damaged(0, 2).password(SECRET);
    let held = last_article(&scenario, 1);
    let settled = run_settling_while_lent(&scenario, held).await;
    assert_repaired_under_the_candidate(&settled, &body);
}

// RAR5 `-hp -rr`: the same ordering, the damaged article wholly in the
// first volume's cipher bytes, so the enciphered headers still open. Its
// record is found without the candidate, so the rebuilt record rules the
// same handed or not; the row holds the RAR5 kind to the same outcome.
#[tokio::test]
async fn a_sealed_rar5_record_rebuilt_from_its_facts_on_return_rules_under_the_candidate_its_store_handed()
 {
    let body = payload(7 * ARTICLE + 13, 51);
    let set = protected_header_encrypted_store_set(MEMBER, &body, 2, SECRET, 1);
    let layout = unrar_rs::locate_recovery_record(&mut std::io::Cursor::new(&set[0].1), None)
        .unwrap()
        .unwrap();
    let in_data = (layout.protected_size / ARTICLE as u64 - 1) as u32;
    let scenario = Scenario::new(posts(set))
        .damaged(0, in_data)
        .password(SECRET);
    let held = last_article(&scenario, 1);
    let settled = run_settling_while_lent(&scenario, held).await;
    assert_repaired_under_the_candidate(&settled, &body);
}
