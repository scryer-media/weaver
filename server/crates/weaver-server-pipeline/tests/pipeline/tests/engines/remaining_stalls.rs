// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_composition::JobStatus;
use weaver_server_pipeline::pipeline::tests::engines::remaining_stalls::{
    Acknowledgments, Saved, SavedRun, drive_saved_sequence,
};

async fn saved_sequence(saved: Saved, metadata_true_size: bool) {
    saved_with_acknowledgments(saved, metadata_true_size, Acknowledgments::Ready).await;
}

async fn saved_with_acknowledgments(
    saved: Saved,
    metadata_true_size: bool,
    acknowledgments: Acknowledgments,
) {
    let SavedRun {
        stopped,
        status,
        output,
        expected,
    } = drive_saved_sequence(saved, metadata_true_size, acknowledgments).await;
    assert!(
        stopped.is_none(),
        "{saved:?} true_metadata={metadata_true_size}: {}",
        stopped.unwrap_or_default()
    );
    if matches!(saved, Saved::Gap) {
        assert!(
            matches!(status, Some(JobStatus::Failed { .. })),
            "{status:?}"
        );
    } else {
        assert_eq!(status, Some(JobStatus::Complete));
        assert!(output.values().any(|bytes| bytes == &expected));
    }
}

#[tokio::test]
async fn saved_bare_encrypted_sequence_has_a_terminal_owner() {
    saved_sequence(Saved::Bare, false).await;
}
#[tokio::test]
async fn saved_misnumbered_encrypted_sequence_has_a_terminal_owner() {
    saved_sequence(Saved::Misnumbered, false).await;
}
#[tokio::test]
async fn saved_swapped_encrypted_sequence_has_a_terminal_owner() {
    saved_sequence(Saved::Swapped, false).await;
}
#[tokio::test]
async fn saved_gap_encrypted_sequence_has_a_terminal_owner() {
    saved_sequence(Saved::Gap, false).await;
}
#[tokio::test]
async fn bare_encrypted_metadata_true_length_control() {
    saved_sequence(Saved::Bare, true).await;
}

#[tokio::test]
async fn bare_encrypted_sequence_acknowledges_each_early_loan() {
    saved_with_acknowledgments(Saved::Bare, false, Acknowledgments::Early).await;
}

#[tokio::test]
async fn bare_encrypted_sequence_defers_worker_acknowledgments_until_all_arrivals() {
    saved_with_acknowledgments(Saved::Bare, false, Acknowledgments::Deferred).await;
}

#[tokio::test]
async fn misnumbered_encrypted_sequence_defers_worker_acknowledgments_until_all_arrivals() {
    saved_with_acknowledgments(Saved::Misnumbered, false, Acknowledgments::Deferred).await;
}
#[tokio::test]
async fn swapped_encrypted_sequence_defers_worker_acknowledgments_until_all_arrivals() {
    saved_with_acknowledgments(Saved::Swapped, false, Acknowledgments::Deferred).await;
}
#[tokio::test]
async fn gap_encrypted_sequence_defers_worker_acknowledgments_until_all_arrivals() {
    saved_with_acknowledgments(Saved::Gap, false, Acknowledgments::Deferred).await;
}
