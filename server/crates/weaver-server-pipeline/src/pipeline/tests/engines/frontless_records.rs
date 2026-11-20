// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR5 volume posted under its set's own name, whose first article is
// lost: its signature, main header and file header went with it, so its
// headers say nothing. It carries a recovery record over those bytes, and
// no PAR2 is posted: the record, found from the volume's end, rebuilds the
// front, and the volume takes the place its name gives it.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Outcome, Scenario, payload, run};
use super::rar::{delivered, posts};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::protected_store_set;

const MEMBER: &str = "beacon.bin";

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().rulings,
        graph.files.keys().collect::<Vec<_>>()
    )
}

fn assert_member(graph: &Outcome, body: &[u8], case: &str) {
    assert!(graph.completed(), "{case}: {}", summary(graph));
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered(MEMBER)],
        "{case}: {}",
        summary(graph)
    );
    assert!(
        graph.files[&delivered(MEMBER)] == body,
        "{case}: member bytes"
    );
}

// Four volumes each with a record of one parity unit, the third without
// its first article. The record rebuilds it, every other record finds its
// volume whole, and the member is delivered whole, posted in order or
// backwards.
#[tokio::test]
async fn a_named_rar5_volume_with_no_front_is_rebuilt_by_its_own_recovery_record() {
    let body = payload(23 * ARTICLE + 13, 51);
    let set = protected_store_set(MEMBER, &body, 4, 1);
    for backwards in [false, true] {
        let mut scenario = Scenario::new(posts(set.clone())).lost(2, 0);
        if backwards {
            scenario = scenario.backwards();
        }
        let graph = run(&scenario).await;
        let case = format!("backwards {backwards}");
        assert_member(&graph, &body, &case);
        let rulings = &graph.graph().rulings;
        let count = |outcome: RepairOutcome| {
            rulings
                .iter()
                .filter(|(kind, said)| kind == "rar_record" && *said == Some(outcome))
                .count()
        };
        assert_eq!(rulings.len(), 4, "{case}: {}", summary(&graph));
        assert_eq!(
            count(RepairOutcome::Clean),
            3,
            "{case}: {}",
            summary(&graph)
        );
        assert_eq!(
            count(RepairOutcome::Repaired { slices: 1 }),
            1,
            "{case}: {}",
            summary(&graph)
        );
    }
}

// The first volume of the set without its first article: nothing before it
// names the set, and its record rebuilds it all the same.
#[tokio::test]
async fn a_first_rar5_volume_with_no_front_is_rebuilt_by_its_own_recovery_record() {
    let body = payload(23 * ARTICLE + 13, 51);
    let set = protected_store_set(MEMBER, &body, 4, 1);
    let graph = run(&Scenario::new(posts(set)).lost(0, 0)).await;
    assert_member(&graph, &body, "first volume");
}

// The second volume of two, its record of one parity unit, its first
// article lost and another in a later unit served damaged: the record
// cannot rebuild both, nothing else covers the volume, and the job fails
// with nothing delivered.
#[tokio::test]
async fn a_frontless_rar5_volume_damaged_past_its_record_fails() {
    let body = payload(80 * ARTICLE + 7, 55);
    let set = protected_store_set(MEMBER, &body, 2, 1);
    // The front alone is within the record.
    let graph = run(&Scenario::new(posts(set.clone())).lost(1, 0)).await;
    assert_member(&graph, &body, "front alone");
    let graph = run(&Scenario::new(posts(set)).lost(1, 0).damaged(1, 20)).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{}",
        summary(&graph)
    );
    assert!(graph.files.is_empty(), "{}", summary(&graph));
    assert!(
        graph
            .graph()
            .rulings
            .iter()
            .any(|(kind, said)| kind == "rar_record"
                && matches!(said, Some(RepairOutcome::Impossible { .. }))),
        "{}",
        summary(&graph)
    );
}
