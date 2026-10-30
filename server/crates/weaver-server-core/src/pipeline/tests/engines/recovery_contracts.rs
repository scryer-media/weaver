// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{Post, Scenario, par2_volumes_sliced, payload, run};
use crate::jobs::model::JobStatus;

#[tokio::test]
async fn repaired_signature_supersedes_an_early_zip_name() {
    use sevenz_turbo::{EncoderConfiguration, EncoderMethod};
    let archive = super::sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    let mut posts = par2_volumes_sliced(
        "identity",
        &[("bundle.zip", &archive)],
        256,
        archive.len().div_ceil(256) as u32,
        2,
    );
    let source = posts.len() as u32;
    posts.push(Post::plain("bundle.zip", archive));
    let outcome = run(&Scenario::new(posts).damaged(source, 0)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.graph
    );
    for (_, bytes) in super::sevenz::members() {
        assert!(outcome.files.values().any(|file| file == &bytes));
    }
}

#[tokio::test]
async fn swapped_names_with_one_damaged_file_repair_without_rename_collisions() {
    let a = payload(259, 101);
    let b = payload(259, 102);
    let mut damaged = b.clone();
    damaged[130] ^= 0x55;
    let mut posts = vec![
        Post::plain("a.bin", damaged),
        Post::plain("b.bin", a.clone()),
    ];
    posts.extend(par2_volumes_sliced(
        "identity",
        &[("a.bin", &a), ("b.bin", &b)],
        64,
        2,
        1,
    ));
    let scenario = Scenario::new(posts);
    let outcome = run(&scenario).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.graph
    );
    assert_eq!(outcome.files.len(), 2, "{:?}", outcome.files.keys());
    assert_eq!(outcome.files["lantern beacon/a.bin"], a);
    assert_eq!(outcome.files["lantern beacon/b.bin"], b);
}

#[tokio::test]
async fn truncated_uu_archive_is_repaired_before_decoder_retry() {
    use sevenz_turbo::{EncoderConfiguration, EncoderMethod};
    let archive = super::sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    let sources = [("bundle.7z", archive.as_slice())];
    let mut posts = vec![Post::plain(
        "bundle.7z",
        archive[..archive.len() - 12].to_vec(),
    )];
    posts.extend(par2_volumes_sliced("identity", &sources, 64, 4, 1));
    let scenario = Scenario::new(posts).uuencoded();
    let outcome = run(&scenario).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.graph
    );
    for (_, bytes) in super::sevenz::members() {
        assert!(outcome.files.values().any(|file| file == &bytes));
    }
    super::restart::restart_row(scenario).await;
}
