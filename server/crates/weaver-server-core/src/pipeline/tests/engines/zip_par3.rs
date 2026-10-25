// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A stored zip carrying its own PAR3, the shape an inside-insertion writes:
// the archive, the recovery set, then a copy of the archive's end records.
// Posted as one `.zip`, it is repaired from the set it carries whether its
// member bytes, its first local header, or one of its articles is lost.

use std::sync::Arc;

use par3_rs::creation::CreationOptions;
use par3_rs::inside::{ContainerLimits, InsertionPlan};
use par3_rs::source::{MemorySourceAccess, SourceId};
use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Post, Scenario, run_both};
use super::zip::{archive, assert_members, members, stored};

const NAME: &str = "lantern.zip";

// The stored zip with a PAR3 set over it inserted ahead of a copy of its
// end records.
fn inserted() -> Vec<u8> {
    let original = archive(stored());
    let scratch = tempfile::tempdir().unwrap();
    let source = SourceId(0);
    let mut access = MemorySourceAccess::default();
    access.insert(source, 1, Arc::from(original.as_slice()));
    let path = scratch.path().join(NAME);
    let options = CreationOptions {
        block_size: ARTICLE as u64,
        recovery_count: 4,
        ..CreationOptions::default()
    };
    InsertionPlan::build(
        Arc::new(access),
        source,
        NAME,
        options,
        &ContainerLimits::default(),
    )
    .expect("an insertion plan over the stored zip")
    .execute(&path, scratch.path())
    .expect("the recovery set is inserted");
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..original.len()], original.as_slice());
    assert_eq!(&bytes[original.len()..original.len() + 8], b"PAR3\0PKT");
    bytes
}

// Classic repairs too, though a damaged member it first extracted may stay
// behind in its staging directory; only the members are compared there.
async fn assert_repaired(scenario: Scenario) {
    let (classic, graph) = run_both(&scenario).await;
    assert!(classic.completed(), "classic: {:?}", classic.failure);
    for (name, bytes) in members() {
        assert_eq!(
            classic.files.get(&format!("lantern beacon/{name}")),
            Some(&bytes),
            "classic: {name}"
        );
    }
    assert_members(&graph);
    assert!(
        graph
            .graph()
            .outcomes
            .iter()
            .any(|outcome| matches!(outcome, Some(RepairOutcome::Repaired { .. }))),
        "{:?}",
        graph.graph().outcomes
    );
}

// A hundred bytes of the first member overwritten as posted: every article
// arrives whole, and only the set's hashes say the bytes are wrong.
#[tokio::test]
async fn a_zip_carrying_its_own_par3_with_member_bytes_overwritten_is_repaired_under_both_engines()
{
    let mut bytes = inserted();
    bytes[200..300].fill(0xA5);
    assert_repaired(Scenario::new(vec![Post::plain(NAME, bytes)])).await;
}

// The first local header zeroed as posted: no signature opens the post.
#[tokio::test]
async fn a_zip_carrying_its_own_par3_with_its_first_header_zeroed_is_repaired_under_both_engines() {
    let mut bytes = inserted();
    bytes[..64].fill(0);
    assert_repaired(Scenario::new(vec![Post::plain(NAME, bytes)])).await;
}

// An article of the first member's data never arrives.
#[tokio::test]
async fn a_zip_carrying_its_own_par3_missing_an_article_is_repaired_under_both_engines() {
    let scenario = Scenario::new(vec![Post::plain(NAME, inserted())]).lost(0, 3);
    assert_repaired(scenario).await;
}
