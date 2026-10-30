// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{Scenario, par2_volumes_sliced, run};
use super::sevenz;
use crate::jobs::model::JobStatus;
use sevenz_turbo::{EncoderConfiguration, EncoderMethod};

#[tokio::test]
async fn uu_padding_uses_unique_header_proven_geometry() {
    let archive = sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    for padded in [false, true] {
        let mut posts = sevenz::split(&archive, 3);
        if padded {
            for post in &mut posts {
                post.bytes.push(0);
            }
        }
        let outcome = run(&Scenario::new(posts).uuencoded()).await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{:?}",
            outcome.graph
        );
        for (_, bytes) in sevenz::members() {
            assert!(outcome.files.values().any(|file| file == &bytes));
        }
    }
}

#[tokio::test]
async fn known_uu_padding_geometry_survives_restart_after_each_article() {
    let archive = sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    let mut posts = sevenz::split(&archive, 3);
    for post in &mut posts {
        post.bytes.push(0);
    }
    super::restart::restart_row(Scenario::new(posts).uuencoded()).await;
}

#[tokio::test]
async fn par2_clean_prefix_reconciles_uu_split_geometry() {
    padded_with_par2(false).await;
}

#[tokio::test]
async fn par2_repaired_prefix_reconciles_uu_split_geometry() {
    padded_with_par2(true).await;
}

async fn padded_with_par2(corrupt: bool) {
    let archive = sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    let mut padded = sevenz::split(&archive, 3);
    let sources: Vec<_> = padded
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let recovery = par2_volumes_sliced("geometry", &sources, 64, 16, 1);
    for post in &mut padded {
        post.bytes.push(0);
    }
    if corrupt {
        padded[0].bytes[50] ^= 0x5a;
    }
    padded.extend(recovery);
    let outcome = run(&Scenario::new(padded).uuencoded()).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.graph
    );
    for (_, bytes) in sevenz::members() {
        assert!(outcome.files.values().any(|file| file == &bytes));
    }
}

#[tokio::test]
async fn zero_tail_guess_requires_known_uu_source() {
    let archive = sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    let mut posts = sevenz::split(&archive, 3);
    for post in &mut posts {
        post.bytes.push(0);
    }
    let outcome = run(&Scenario::new(posts)).await;
    assert!(matches!(outcome.status, Some(JobStatus::Failed { .. })));
    assert!(outcome.files.is_empty());
}
