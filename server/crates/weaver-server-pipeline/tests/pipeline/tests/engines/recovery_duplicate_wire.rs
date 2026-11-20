// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeSet;

use weaver_server_pipeline::JobStatus;
use weaver_server_pipeline::pipeline::tests::engines::recovery_duplicate_wire::{
    Protection, Witness, drive_wire,
};

async fn witness(protection: Protection, batches: &[u32]) {
    let Witness {
        expected,
        files,
        observed,
        status,
        trace,
    } = drive_wire(protection, batches).await;
    if matches!(protection, Protection::TruncatedCarrier) {
        assert_eq!(observed.absent, BTreeSet::from([(0, 16), (1, 16)]));
        assert_eq!(observed.truncated.len(), 2);
        assert_eq!(observed.holes.len(), 2);
        eprintln!(
            "salvaged prefixes={:?}; uncovered suffixes={:?}",
            observed.truncated, observed.holes
        );
    }
    if let Some(overlong) = protection.transport() {
        assert_eq!(observed.replies.len(), 100, "{observed:?}");
        if !overlong {
            assert_eq!(
                observed.rejected.len(),
                100,
                "wrong CRCs are rejected: {observed:?}"
            );
        }
        assert!(
            observed
                .rejected
                .iter()
                .all(|id| observed.exhausted.contains(id) || observed.graph_damaged.contains(id)),
            "every rejected reply reaches a terminal loss or graph-owned damaged backing: {observed:?}"
        );
        eprintln!(
            "transport replies={} rejected={} exhausted={} graph_damaged={} accepted_lengths={:?}",
            observed.replies.len(),
            observed.rejected.len(),
            observed.exhausted.len(),
            observed.graph_damaged.len(),
            observed.accepted_lengths
        );
    }
    if matches!(protection, Protection::Par2WrongFinal) {
        assert!(
            matches!(status, Some(JobStatus::Failed { .. })),
            "{status:?}: {trace:?}"
        );
        assert!(
            files.is_empty(),
            "a current payload failing its final CRC is never published as usable"
        );
        return;
    }
    assert_eq!(status, Some(JobStatus::Complete), "{trace:?}");
    assert_eq!(files.len(), 1);
    assert!(
        files.values().any(|bytes| bytes == &expected),
        "byte-exact payload"
    );
}

#[tokio::test]
async fn par2_conflicting_front_copies_survive_a_repeated_front_and_reversed_tail() {
    witness(Protection::Par2, &[0, 0, 1, 3, 2]).await;
}

#[tokio::test]
async fn par2_conflicting_front_copies_survive_a_late_second_piece() {
    witness(Protection::Par2, &[0, 0, 2, 3, 1]).await;
}

#[tokio::test]
async fn par3_conflicting_front_copies_survive_a_repeated_front() {
    witness(Protection::Par3 { broad: false }, &[0, 0, 1, 2, 3]).await;
}

#[tokio::test]
async fn par3_conflicting_front_copies_survive_the_tail_arriving_first() {
    witness(Protection::Par3 { broad: false }, &[3, 1, 0, 0, 2]).await;
}

#[tokio::test]
async fn par3_twelve_conflicting_copies_survive_a_repeated_middle_piece() {
    witness(Protection::Par3 { broad: true }, &[2, 0, 1, 1, 3]).await;
}

#[tokio::test]
async fn a_hundred_wrong_transport_crcs_on_rar_are_verified_through_recovery() {
    witness(
        Protection::Par2Transport {
            rar: true,
            overlong: false,
        },
        &[0, 0, 1, 2, 3],
    )
    .await;
}

#[tokio::test]
async fn a_hundred_overlong_rar_articles_are_repaired_at_their_declared_ranges() {
    witness(
        Protection::Par2Transport {
            rar: true,
            overlong: true,
        },
        &[0, 0, 1, 2, 3],
    )
    .await;
}

#[tokio::test]
async fn a_hundred_wrong_transport_crcs_on_sevenz_are_verified_through_recovery() {
    witness(
        Protection::Par2Transport {
            rar: false,
            overlong: false,
        },
        &[0, 0, 1, 2, 3],
    )
    .await;
}

#[tokio::test]
async fn a_hundred_overlong_sevenz_articles_are_repaired_at_their_declared_ranges() {
    witness(
        Protection::Par2Transport {
            rar: false,
            overlong: true,
        },
        &[0, 0, 1, 2, 3],
    )
    .await;
}

#[tokio::test]
async fn truncated_par2_carrier_prefixes_do_not_hide_four_intact_later_rows() {
    witness(Protection::TruncatedCarrier, &[0, 1, 2, 3]).await;
}

#[tokio::test]
async fn a_hundred_wrong_crcs_starting_inside_rar5_are_repaired() {
    witness(
        Protection::Par2PositionedTransport {
            rar4: false,
            overlong: false,
            tail: false,
        },
        &[0, 1, 2, 3],
    )
    .await;
}

#[tokio::test]
async fn a_hundred_wrong_crcs_starting_inside_rar4_are_repaired() {
    witness(
        Protection::Par2PositionedTransport {
            rar4: true,
            overlong: false,
            tail: false,
        },
        &[0, 1, 2, 3],
    )
    .await;
}

#[tokio::test]
async fn a_hundred_overlong_rar5_articles_selected_backwards_are_repaired() {
    witness(
        Protection::Par2PositionedTransport {
            rar4: false,
            overlong: true,
            tail: true,
        },
        &[0, 1, 2, 3],
    )
    .await;
}

#[tokio::test]
async fn a_hundred_conflicting_sevenz_copies_are_reconciled_after_the_header_arrives_late() {
    witness(Protection::Par2Many { tail: false }, &[1, 3, 0, 2]).await;
}

#[tokio::test]
async fn conflicting_sevenz_tail_copies_keep_the_three_row_exact_budget() {
    witness(Protection::Par2Many { tail: true }, &[2, 0, 3, 3, 1]).await;
}

#[tokio::test]
async fn twelve_silent_rar5_articles_across_three_volumes_are_repaired() {
    witness(Protection::Par2Silent, &[0, 1, 2, 3]).await;
}

#[tokio::test]
async fn replacement_header_handoff_does_not_publish_a_wrong_final_payload() {
    witness(Protection::Par2WrongFinal, &[2, 0, 3, 3, 1]).await;
}
