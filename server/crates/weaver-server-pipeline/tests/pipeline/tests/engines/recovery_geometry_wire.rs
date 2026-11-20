// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_composition::JobStatus;
use weaver_server_pipeline::pipeline::tests::engines::recovery_geometry_wire::{
    Shape, Witness, drive_wire,
};

async fn witness(shape: Shape) {
    let Witness {
        expected,
        files,
        graph,
        status,
    } = drive_wire(shape).await;
    assert_eq!(
        status,
        Some(JobStatus::Complete),
        "delivery={:?}, rulings={:?}",
        graph.delivery,
        graph.rulings
    );
    assert_eq!(files.len(), 1);
    assert_eq!(
        files.values().next(),
        Some(&expected),
        "byte-exact extracted payload"
    );
}

#[tokio::test]
async fn a_truncated_middle_uu_article_reaches_repair_with_its_tail_present() {
    witness(Shape::TruncatedMiddle).await;
}

#[tokio::test]
async fn wrong_final_uu_lengths_and_a_missing_middle_article_repair() {
    witness(Shape::WrongFinalLength { mixed: false }).await;
}

#[tokio::test]
async fn wrong_final_uu_lengths_in_a_mixed_set_repair() {
    witness(Shape::WrongFinalLength { mixed: true }).await;
}

// A damaged article that waited ahead of its file's cursor is sent on only
// to a server that has not served it. With one server its bytes stand, the
// file closes, and the recovery set repairs it.
#[tokio::test]
async fn a_truncated_uu_article_behind_a_missing_one_reaches_repair() {
    witness(Shape::TruncatedBehindAMissingArticle).await;
}
