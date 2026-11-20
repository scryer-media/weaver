// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A loss told while a store is out deriving its key. The one stored RAR
// volume is encrypted, so its store is lent for the key as soon as it reads
// its headers, and the volume's tail articles are lost while it is out. The
// loss waits for the store and is placed through its map when it is back,
// so the member keeps its length and the PAR2 set mends it whole.

use super::super::scenarios::encrypted_store_set;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::engines::harness::{
    ARTICLE, Post, Scenario, par2_volumes, payload, run,
};

const MEMBER: &str = "source.bin";
const SECRET: &str = "lantern-coverage-secret";

#[tokio::test]
async fn a_loss_told_while_the_store_derives_its_key_lands_through_its_map() {
    let member = payload(557, 64);
    let volume = encrypted_store_set(MEMBER, &member, 1, SECRET, Some(SECRET), false).remove(0);
    let set = par2_volumes(
        "lantern",
        &[(volume.0.as_str(), volume.1.as_slice())],
        12,
        4,
    );
    let articles = volume.1.len().div_ceil(ARTICLE) as u32;
    let mut posts = vec![Post::plain(&volume.0, volume.1.clone())];
    posts.extend(
        set.into_iter()
            .map(|post| Post::plain(&post.name, post.bytes)),
    );
    let mut scenario = Scenario::new(posts).password(SECRET);
    for ordinal in articles / 2 + 1..articles {
        scenario = scenario.lost(0, ordinal);
    }
    let outcome = run(&scenario).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?} {:?}",
        outcome.failure,
        outcome.files.keys().collect::<Vec<_>>()
    );
    let delivered = outcome
        .files
        .iter()
        .find(|(path, _)| path.rsplit('/').next() == Some(MEMBER))
        .map(|(_, bytes)| bytes);
    assert_eq!(
        delivered,
        Some(&member),
        "the member is not delivered whole"
    );
}
