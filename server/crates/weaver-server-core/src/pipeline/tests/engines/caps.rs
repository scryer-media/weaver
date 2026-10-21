// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The extraction caps under graph: one budget per job, held where the
// namespace writes. A breach fails the job as classic's budget fails one,
// with the refusal's rendering and reason word, at the extraction stage; a
// name the operator's policy refuses fails it with the policy's sentence.
// The depth limit is no breach: the tree never builds an object past it,
// the member is delivered as the container it is, and the job goes on. Classic enforces these in its extraction workers, which
// this harness does not run, so every row here is graph-only. The deadline
// has no row: it is a wall clock, and no row here races one.

use super::harness::{ARTICLE, Post, Scenario, payload, run};
use super::restart::run_restarting;
use super::streams::{gzip, tar, xz};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::extraction::ExtractionLimits;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::{multi_member_store_set, single_member_store_set};

const GIB: u64 = 1024 * 1024 * 1024;
pub(super) const FLOOR: u64 = 256 * 1024;
const DEPTH_LIMIT: usize = crate::pipeline::completion::finalize::MAX_GRAPH_NESTING_DEPTH as usize;

// Limits no row here comes near, with `edit` applied.
pub(super) fn limits(edit: impl FnOnce(&mut ExtractionLimits)) -> ExtractionLimits {
    let mut limits = ExtractionLimits {
        max_job_bytes: 2 * 1024 * GIB,
        max_member_bytes: 1024 * GIB,
        max_entries: 100_000,
        max_ratio: 100,
        ratio_floor_bytes: 1 << 30,
        max_seconds: 43_200,
        min_free_bytes: 1,
        max_memory_bytes: 4 * GIB,
    };
    edit(&mut limits);
    limits
}

pub(super) fn assert_refused(outcome: &super::harness::Outcome, reason: &str) {
    let marker = format!("WEAVER_EXTRACTION_REJECTED[{reason}]: ");
    assert!(
        outcome
            .failure
            .as_deref()
            .is_some_and(|failure| failure.starts_with(&marker)),
        "{:?}",
        outcome.status
    );
    assert_eq!(outcome.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(outcome.files.is_empty(), "{:?}", outcome.files.keys());
}

// A zstd frame of `blocks` RLE blocks of 128 KiB of zeros each: a few bytes
// posted for every 128 KiB it inflates to.
pub(super) fn zeros_zstd(blocks: usize) -> Vec<u8> {
    const BLOCK: u32 = 128 * 1024;
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38];
    for block in 0..blocks {
        let last = u32::from(block + 1 == blocks);
        let header = last | (1 << 1) | (BLOCK << 3);
        frame.extend_from_slice(&header.to_le_bytes()[..3]);
        frame.push(0);
    }
    frame
}

#[tokio::test]
async fn a_member_inflating_past_the_ratio_fails_the_job_naming_ratio_under_graph() {
    // Half a MiB from a few dozen bytes, against a ratio of four: the job's
    // limit is the floor a ratio never goes under.
    let scenario =
        Scenario::new(vec![Post::plain("ember.bin.zst", zeros_zstd(4))]).limits(limits(|limits| {
            limits.max_ratio = 4;
            limits.ratio_floor_bytes = FLOOR;
        }));
    let graph = run(&scenario, Engine::Graph).await;
    assert_refused(&graph, "ratio");
    assert!(
        graph
            .failure
            .as_deref()
            .is_some_and(|failure| failure.ends_with(&format!("effective job limit is {FLOOR}"))),
        "{:?}",
        graph.failure
    );
}

#[tokio::test]
async fn a_job_writing_past_its_byte_cap_fails_naming_job_bytes_under_graph() {
    let scenario = Scenario::new(vec![Post::plain(
        "ember.bin.xz",
        xz(&payload(40 * ARTICLE, 71)),
    )])
    .limits(limits(|limits| limits.max_job_bytes = 1000));
    let graph = run(&scenario, Engine::Graph).await;
    assert_refused(&graph, "job_bytes");
}

#[tokio::test]
async fn a_member_past_its_byte_cap_fails_naming_member_bytes_under_graph() {
    let scenario = Scenario::new(vec![Post::plain(
        "ember.bin.xz",
        xz(&payload(40 * ARTICLE, 72)),
    )])
    .limits(limits(|limits| limits.max_member_bytes = 1000));
    let graph = run(&scenario, Engine::Graph).await;
    assert_refused(&graph, "member_bytes");
}

#[tokio::test]
async fn an_archive_past_the_entry_cap_fails_naming_entries_under_graph() {
    let members: Vec<(String, Vec<u8>)> = (0..5)
        .map(|index| (format!("ember{index}.bin"), payload(ARTICLE + index, 73)))
        .collect();
    let borrowed: Vec<(&str, Vec<u8>)> = members
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let scenario = Scenario::new(vec![Post::plain("lantern.tar", tar(&borrowed))])
        .limits(limits(|limits| limits.max_entries = 3));
    let graph = run(&scenario, Engine::Graph).await;
    assert_refused(&graph, "entries");
}

// `beacon.bin` inside `levels` stored RARs, each the only member of the one
// around it, the outermost posted as `lantern.rar`; with the bytes of
// `beacon.bin` and of the innermost RAR, `level0.rar`.
fn chain(levels: usize) -> (Post, Vec<u8>, Vec<u8>) {
    let body = payload(3 * ARTICLE + 5, 74);
    let mut inner = ("beacon.bin".to_string(), body.clone());
    let mut innermost = Vec::new();
    for level in 0..levels {
        let (_, bytes) = single_member_store_set(&inner.0, &inner.1, 1).remove(0);
        if level == 0 {
            innermost = bytes.clone();
        }
        inner = (format!("level{level}.rar"), bytes);
    }
    (Post::plain("lantern.rar", inner.1), body, innermost)
}

#[tokio::test]
async fn a_chain_past_the_depth_limit_is_refused_at_the_limit_under_graph() {
    // Every RAR above the innermost opens; that one would open a namespace
    // past the limit, so it is never opened. No budget is breached: it is
    // handed over as the file it is, and the job completes.
    let (post, _, innermost) = chain(DEPTH_LIMIT + 1);
    let graph = run(&Scenario::new(vec![post]), Engine::Graph).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert!(
        graph.graph().delivery.withheld.is_empty(),
        "{:?}",
        graph.graph().delivery
    );
    let delivered: Vec<_> = graph
        .files
        .iter()
        .filter(|(name, _)| name.ends_with("/level0.rar"))
        .collect();
    assert_eq!(delivered.len(), 1, "{:?}", graph.files.keys());
    assert!(*delivered[0].1 == innermost, "level0.rar bytes");
    assert!(!graph.files.keys().any(|name| name.ends_with("beacon.bin")));
    assert!(
        graph.graph().kinds.contains("rar_store"),
        "{:?}",
        graph.graph().kinds
    );
}

#[tokio::test]
async fn a_chain_at_the_depth_limit_delivers_under_graph() {
    let (post, body, _) = chain(DEPTH_LIMIT);
    let graph = run(&Scenario::new(vec![post]), Engine::Graph).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    let delivered: Vec<_> = graph
        .files
        .iter()
        .filter(|(name, _)| name.ends_with("beacon.bin"))
        .collect();
    assert_eq!(delivered.len(), 1, "{:?}", graph.files.keys());
    assert!(*delivered[0].1 == body, "beacon.bin bytes");
}

#[tokio::test]
async fn caps_at_their_defaults_touch_nothing() {
    let members = super::streams::members();
    let scenario = Scenario::new(vec![Post::plain("lantern.tar.gz", gzip(&tar(&members)))]);
    let (classic, graph) = super::harness::run_both(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    super::harness::assert_same(&classic, &graph);
}

#[tokio::test]
async fn a_member_the_extension_policy_rejects_fails_the_job_at_any_depth_under_graph() {
    let rejected = |posts: Vec<Post>| Scenario::new(posts).unacceptable(&["exe"]);
    let member = || vec![("beacon.exe", payload(2 * ARTICLE + 3, 75))];
    let cases = [
        // A posted file, refused at the root before any of it is fetched.
        (
            vec![Post::plain("beacon.exe", payload(ARTICLE, 75))],
            "beacon.exe",
        ),
        // A member of an archive posted at the root.
        (
            vec![Post::plain("lantern.tar", tar(&member()))],
            "beacon.exe",
        ),
        // The same member two deep, inside a tar inside a gzip.
        (
            vec![Post::plain("lantern.tar.gz", gzip(&tar(&member())))],
            "beacon.exe",
        ),
    ];
    for (posts, name) in cases {
        let graph = run(&rejected(posts), Engine::Graph).await;
        assert_eq!(
            graph.failure.as_deref(),
            Some(format!("unwanted extension '.exe' in '{name}'").as_str()),
            "{:?}",
            graph.status
        );
        assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Unpack));
        assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    }
}

#[tokio::test]
async fn a_member_the_policy_allows_is_untouched_under_graph() {
    let members = vec![
        ("exe.bin", payload(2 * ARTICLE + 3, 76)),
        ("beacon.exe.bin", payload(ARTICLE + 1, 77)),
    ];
    let scenario = Scenario::new(vec![Post::plain("lantern.tar.gz", gzip(&tar(&members)))])
        .unacceptable(&["exe"]);
    let graph = run(&scenario, Engine::Graph).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    for (name, bytes) in &members {
        let delivered = graph
            .files
            .iter()
            .find(|(path, _)| path.rsplit('/').next() == Some(*name));
        assert!(
            delivered.is_some_and(|(_, delivered)| delivered == bytes),
            "{name}: {:?}",
            graph.files.keys()
        );
    }
}

#[tokio::test]
async fn a_job_restarted_mid_extraction_keeps_counting_its_bytes() {
    let body = payload(40 * ARTICLE, 78);
    let scenario =
        Scenario::new(vec![Post::plain("ember.bin.xz", xz(&body))]).limits(limits(|limits| {
            limits.max_job_bytes = body.len() as u64 - 1
        }));
    let whole = run(&scenario, Engine::Graph).await;
    assert_refused(&whole, "job_bytes");
    let restarted = run_restarting(&scenario, &mut |_| {}).await;
    assert!(restarted.restarts > 0, "the run restarted");
    assert!(
        matches!(
            &restarted.status,
            Some(JobStatus::Failed { error })
                if error.starts_with("WEAVER_EXTRACTION_REJECTED[job_bytes]: ")
        ),
        "{:?}",
        restarted.status
    );
    assert!(restarted.files.is_empty(), "{:?}", restarted.files.keys());
}

const MIB: u64 = 1024 * 1024;
// The article a breadth post is cut into: a few dozen of them carry it.
const FRAME: usize = 64 * 1024;
// The breadth a restarted row runs, and the article it is cut into: the run
// restarts after every one.
const BREADTH_RESTARTED: usize = 3;
const RESTARTED_FRAME: usize = 1024;

// A breadth post, `lantern.rar`: `width` stored RARs, each holding `width`
// more, `levels` deep, and each innermost one holding one leaf of one byte.
// Every name is the only one of it in the tree, so every leaf is delivered
// under its own.
struct Breadth {
    post: Vec<u8>,
    leaves: Vec<(String, Vec<u8>)>,
    // Every RAR below the post and every leaf.
    entries: u64,
}

fn breadth(width: usize, levels: usize) -> Breadth {
    fn level(
        path: &str,
        depth: usize,
        shape: (usize, usize),
        leaves: &mut Vec<(String, Vec<u8>)>,
    ) -> Vec<u8> {
        let (width, levels) = shape;
        let (names, bodies): (Vec<String>, Vec<Vec<u8>>) = if depth == levels {
            let leaf = (format!("e{path}.bin"), vec![leaves.len() as u8]);
            leaves.push(leaf.clone());
            (vec![leaf.0], vec![leaf.1])
        } else {
            (0..width)
                .map(|index| {
                    let path = format!("{path}{index}");
                    let bytes = level(&path, depth + 1, shape, leaves);
                    (format!("a{path}.rar"), bytes)
                })
                .unzip()
        };
        let members: Vec<(&str, Vec<u8>)> = names.iter().map(String::as_str).zip(bodies).collect();
        multi_member_store_set(&members, 1).remove(0).1
    }
    let mut leaves = Vec::new();
    let post = level("", 0, (width, levels), &mut leaves);
    let rars: u64 = (1..=levels as u32)
        .map(|depth| (width as u64).pow(depth))
        .sum();
    Breadth {
        post,
        entries: rars + leaves.len() as u64,
        leaves,
    }
}

fn breadth_scenario(post: &[u8], edit: impl FnOnce(&mut ExtractionLimits)) -> Scenario {
    Scenario::new(vec![Post::plain("lantern.rar", post.to_vec())])
        .framed(FRAME)
        .limits(limits(edit))
}

fn assert_leaves(files: &std::collections::BTreeMap<String, Vec<u8>>, breadth: &Breadth) {
    assert_eq!(files.len(), breadth.leaves.len(), "delivered files");
    for (name, bytes) in &breadth.leaves {
        assert!(
            files.get(&format!("lantern beacon/{name}")) == Some(bytes),
            "{name}"
        );
    }
}

#[tokio::test]
async fn a_breadth_post_past_the_memory_limit_fails_naming_memory_under_graph() {
    // Ten RARs a level, four deep: far fewer entries than the limit, and
    // what their namespaces keep resident passes a MiB long before the
    // last is announced.
    let breadth = breadth(10, 4);
    assert!(breadth.entries < 100_000, "{}", breadth.entries);
    let scenario = breadth_scenario(&breadth.post, |limits| limits.max_memory_bytes = MIB);
    let graph = run(&scenario, Engine::Graph).await;
    assert_refused(&graph, "memory");
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(
        failure.starts_with("WEAVER_EXTRACTION_REJECTED[memory]: members hold ")
            && failure.ends_with(&format!(" bytes resident, limit is {MIB}")),
        "{failure}"
    );
}

#[tokio::test]
async fn a_breadth_post_one_entry_past_the_entry_limit_fails_naming_entries_under_graph() {
    let breadth = breadth(5, 4);
    let limit = breadth.entries - 1;
    let scenario = breadth_scenario(&breadth.post, |limits| limits.max_entries = limit);
    let graph = run(&scenario, Engine::Graph).await;
    assert_refused(&graph, "entries");
    assert!(
        graph
            .failure
            .as_deref()
            .is_some_and(|failure| failure.ends_with(&format!(
                "archive contains {} entries, limit is {limit}",
                breadth.entries
            ))),
        "{:?}",
        graph.failure
    );
}

#[tokio::test]
async fn a_breadth_post_under_its_limits_delivers_every_leaf_under_graph() {
    let breadth = breadth(5, 4);
    let graph = run(&breadth_scenario(&breadth.post, |_| {}), Engine::Graph).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_leaves(&graph.files, &breadth);
}

#[tokio::test]
async fn a_breadth_post_restarted_counts_its_entries_and_resident_bytes_as_before() {
    let breadth = breadth(BREADTH_RESTARTED, 4);
    let at = |entries: u64, resident: u64| {
        breadth_scenario(&breadth.post, |limits| {
            limits.max_entries = entries;
            limits.max_memory_bytes = resident;
        })
        .framed(RESTARTED_FRAME)
    };
    let whole = run(&at(breadth.entries, 4 * GIB), Engine::Graph).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    // What the namespaces kept, summed from the tree: the job is held to it
    // exactly, uninterrupted and restarted.
    let resident = whole.graph().resident;
    let exact = run(&at(breadth.entries, resident), Engine::Graph).await;
    assert!(exact.completed(), "{:?} {:?}", exact.status, exact.failure);
    let restarted = run_restarting(&at(breadth.entries, resident), &mut |_| {}).await;
    assert!(restarted.restarts > 0, "the run restarted");
    assert_eq!(restarted.status, Some(JobStatus::Complete));
    assert_leaves(&restarted.files, &breadth);
    for (scenario, reason) in [
        (at(breadth.entries - 1, resident), "entries"),
        (at(breadth.entries, resident - 1), "memory"),
    ] {
        let restarted = run_restarting(&scenario, &mut |_| {}).await;
        assert!(restarted.restarts > 0, "the run restarted");
        assert!(
            matches!(
                &restarted.status,
                Some(JobStatus::Failed { error })
                    if error.starts_with(&format!("WEAVER_EXTRACTION_REJECTED[{reason}]: "))
            ),
            "{reason}: {:?}",
            restarted.status
        );
        assert!(restarted.files.is_empty(), "{:?}", restarted.files.keys());
    }
}
