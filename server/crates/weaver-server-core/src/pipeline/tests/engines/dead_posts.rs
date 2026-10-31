// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Dead posts fail fast, in two vocabularies. The usenet feed rules from
// the NZB and its servers alone: the first-article sample, then the post's
// health against its critical line, with a STAT round's projection naming
// what took it there. The graph rules from what its feed refused: a
// recovery set that can no longer reach enough blocks for the ranges it was
// refused rules impossible, and the job ends on its words.

use super::harness::*;
use super::restart::run_restarting;
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;

// Twelve files of four articles each.
fn files() -> Vec<Post> {
    (0..12u8)
        .map(|at| Post::plain(&format!("lantern{at:02}.mkv"), payload(4 * ARTICLE, at)))
        .collect()
}

#[tokio::test]
async fn a_post_whose_first_articles_are_lost_fails_before_its_payload_is_fetched() {
    // Twelve files and no recovery: the post's health crosses its critical
    // line before the sample reaches its share, but the sample rules first,
    // and ten of twelve first articles lost is past its share.
    let scenario = (0..10).fold(Scenario::new(files()), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    // The feed abandons the files that lost an article and keeps fetching
    // the others, which arrive whole.
    assert_eq!(graph.status, Some(crate::jobs::model::JobStatus::Partial));
    for name in ["lantern10.mkv", "lantern11.mkv"] {
        assert_eq!(graph.claims.get(name), Some(&Claim::Complete), "{name}");
    }
}

// Twelve files and no recovery, every article of the last lost: the others
// arrive whole and the job is Partial. Nothing of the last arrived, so no
// file stands in for it, though the root holds its loss as damage.
#[tokio::test]
async fn a_file_whose_every_article_is_lost_has_no_file() {
    let scenario = (0..4).fold(Scenario::new(files()), |scenario, ordinal| {
        scenario.lost(11, ordinal)
    });
    let graph = run(&scenario).await;
    assert_eq!(graph.status, Some(JobStatus::Partial));
    for at in 0..11 {
        let name = format!("lantern{at:02}.mkv");
        assert_eq!(graph.claims.get(&name), Some(&Claim::Complete), "{name}");
    }
    assert!(
        !graph
            .files
            .keys()
            .any(|path| path.ends_with("lantern11.mkv")),
        "{:?}",
        graph.files.keys().collect::<Vec<_>>()
    );
}

// Ten files of two articles and no recovery, every file's first article
// refused by every server. The post's health falls below its critical line
// at the fourth lost first article, long before the sample reaches its
// share at the eighth; while the first articles still to come could take
// the sample there, health says nothing, and the post fails in the
// sample's words.
#[tokio::test]
async fn a_post_whose_every_first_article_is_lost_fails_on_the_sample_never_on_its_health() {
    let posts: Vec<Post> = (0..10u8)
        .map(|at| Post::plain(&format!("lantern{at:02}.mkv"), payload(2 * ARTICLE, at)))
        .collect();
    let scenario = (0..10).fold(Scenario::new(posts), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(
        failure.starts_with("aborted: ") && failure.contains(" of 10 first articles are missing"),
        "{failure}"
    );
    assert!(!failure.contains("health"), "{failure}");
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
}

// The same post, the pipeline going down once three first articles are
// lost: what the root ruled lost before the restart is still lost after it,
// so the sample counts all ten and the post fails in the sample's words.
#[tokio::test]
async fn a_first_article_lost_before_a_restart_still_counts_toward_the_sample() {
    let posts: Vec<Post> = (0..10u8)
        .map(|at| Post::plain(&format!("lantern{at:02}.mkv"), payload(2 * ARTICLE, at)))
        .collect();
    let scenario = (0..10).fold(Scenario::new(posts), |scenario, file| {
        scenario.lost(file, 0)
    });
    let restarted = super::restart::run_restarting_at(
        &scenario,
        &mut |_| {},
        super::restart::Down::Crash,
        &|asked| asked == 3,
    )
    .await;
    assert_eq!(restarted.restarts, 1, "the pipeline went down once");
    let Some(JobStatus::Failed { error }) = &restarted.status else {
        panic!("{:?}", restarted.status);
    };
    assert!(
        error.starts_with("aborted: ") && error.contains(" of 10 first articles are missing"),
        "{error}"
    );
    assert!(!error.contains("health"), "{error}");
}

#[tokio::test]
async fn committed_feed_verdict_survives_restart_without_fetching_abandoned_files() {
    let first: Vec<_> = (0..10).map(|file| (file, 0)).collect();
    let scenario = (0..10).fold(Scenario::new(files()).first(&first), |scenario, file| {
        scenario.lost(file, 0)
    });
    let mut committed = false;
    let restarted = super::restart::run_restarting_at(
        &scenario,
        &mut |rows| {
            let verdict = rows
                .feed_verdict
                .as_ref()
                .expect("the crash follows a committed feed verdict");
            assert!(verdict.reason.contains("first articles are missing"));
            assert_eq!(verdict.abandoned_files, (0..10).collect());
            committed = true;
        },
        super::restart::Down::Crash,
        &|asked| asked == 10,
    )
    .await;
    assert!(committed);
    assert_eq!(restarted.restarts, 1);
    assert_eq!(restarted.status, Some(JobStatus::Partial));
    for file in 0..10 {
        let fetched: Vec<_> = restarted
            .order
            .iter()
            .copied()
            .filter(|(index, _)| *index == file)
            .collect();
        assert_eq!(fetched, vec![(file, 0)], "abandoned file {file} restarted");
    }
    for file in 10..12u8 {
        let suffix = format!("lantern{file:02}.mkv");
        let (_, delivered) = restarted
            .files
            .iter()
            .find(|(name, _)| name.ends_with(&suffix))
            .expect("healthy files still finish");
        assert_eq!(*delivered, payload(4 * ARTICLE, file));
    }
}

#[tokio::test]
async fn the_sample_is_the_payloads_first_articles_and_not_its_recoverys() {
    // Eleven of twelve files lost at their first article, beside a recovery
    // set of four blocks in four volumes whose first articles all came, far
    // too small to rebuild them. The sample is the twelve files and the
    // index; the recovery volumes would have diluted it below the share.
    let payload_files: Vec<(String, Vec<u8>)> = (0..12u8)
        .map(|at| (format!("lantern{at:02}.mkv"), payload(512 * ARTICLE, at)))
        .collect();
    let named: Vec<(&str, &[u8])> = payload_files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = payload_files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    let recovery = par2_volumes_sliced("lantern", &named, 16 * ARTICLE as u64, 4, 4);
    assert_eq!(recovery.len(), 5, "an index and four volumes");
    posts.extend(recovery);
    let scenario = (0..11).fold(Scenario::new(posts), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    // The sample rules the post dead; the one file whose first article came
    // is still fetched and arrives whole, so the job is partial.
    assert_eq!(
        graph.status,
        Some(crate::jobs::model::JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(graph.claims.get("lantern11.mkv"), Some(&Claim::Complete));
}

#[tokio::test]
async fn a_sample_short_of_the_share_at_or_above_critical_leaves_the_post_to_its_recovery() {
    // Nine of twelve files lost at their first article, short of the
    // sample's share, beside a recovery set that keeps the post's health at
    // or above its critical line: the feed rules nothing, and the set
    // rebuilds what was lost.
    let payload_files: Vec<(String, Vec<u8>)> = (0..12u8)
        .map(|at| (format!("lantern{at:02}.mkv"), payload(4 * ARTICLE, at)))
        .collect();
    let named: Vec<(&str, &[u8])> = payload_files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = payload_files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_set("lantern", &named, 24));
    let scenario = (0..9).fold(Scenario::new(posts), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(graph.requests.contains_key(&(11, 3)));
}

#[tokio::test]
async fn first_articles_the_recovery_covers_leave_the_post_to_its_repair() {
    let payload_files: Vec<(String, Vec<u8>)> = (0..10u8)
        .map(|at| (format!("lantern{at:02}.mkv"), payload(ARTICLE, at)))
        .collect();
    let named: Vec<(&str, &[u8])> = payload_files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = payload_files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_set("lantern", &named, 12));
    let scenario = (0..10).fold(Scenario::new(posts), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    for (name, _) in &payload_files {
        assert_eq!(graph.claims.get(name), Some(&Claim::Complete), "{name}");
    }
}

#[tokio::test]
async fn losses_past_every_reachable_recovery_block_fail_before_the_payload_is_fetched() {
    // Sixty slices of payload, fifteen recovery blocks, and twenty slices
    // lost from the second article on, under the scheduler's leases.
    let bytes = payload(60 * ARTICLE, 7);
    let mut posts = vec![Post::plain("lantern.mkv", bytes.clone())];
    posts.extend(par2_set(
        "lantern",
        &[("lantern.mkv", bytes.as_slice())],
        15,
    ));
    let scenario = (1..=20).fold(Scenario::new(posts).leased(), |scenario, ordinal| {
        scenario.lost(0, ordinal)
    });
    let graph = run(&scenario).await;
    // The set's own words. The post's health is still above its critical
    // line, a third of the payload lost against a recovery set a quarter
    // its size, so the failure says nothing of it.
    assert_eq!(
        graph.failure.as_deref(),
        Some(
            "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern.mkv): not repairable: \
             20 damaged slices, only 15 recovery blocks reachable"
        )
    );
    // A set that cannot rebuild what it covers is a repair failure, as it
    // is when it rules once the post is in.
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    // The losses end the job while the payload's last thirty articles are
    // still to fetch, and none of them was asked for.
    let asked: Vec<(u32, u32)> = graph.requests.keys().copied().collect();
    assert!(
        asked
            .iter()
            .all(|&(file, ordinal)| file != 0 || ordinal < 30),
        "{asked:?}"
    );
}

#[tokio::test]
async fn a_post_whose_every_article_is_gone_fails_below_critical() {
    // Four payload files and a recovery set of fifteen blocks in four
    // volumes: too few files for the sample to rule, and every article
    // deleted, so no recovery block can be had. The payload outweighs its
    // recovery as a real post's does, so the health has a critical line.
    let payload_files: Vec<(String, Vec<u8>)> = (0..4u8)
        .map(|at| (format!("lantern{at:02}.mkv"), payload(512 * ARTICLE, at)))
        .collect();
    let named: Vec<(&str, &[u8])> = payload_files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = payload_files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_volumes_sliced(
        "lantern",
        &named,
        16 * ARTICLE as u64,
        15,
        4,
    ));
    let articles: Vec<(u32, u32)> = posts
        .iter()
        .zip(0u32..)
        .flat_map(|(post, file)| {
            (0..post.bytes.len().div_ceil(ARTICLE) as u32).map(move |ordinal| (file, ordinal))
        })
        .collect();
    let scenario = articles.iter().fold(
        Scenario::new(posts).leased(),
        |scenario, &(file, ordinal)| scenario.lost(file, ordinal),
    );
    let graph = run(&scenario).await;
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(failure.starts_with("health "), "{failure}");
    assert!(failure.contains("below critical"), "{failure}");
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Health));
}

#[tokio::test]
async fn a_release_posted_as_its_first_articles_alone_fails_on_its_probe_before_the_rest_is_fetched()
 {
    // Three files of forty articles and no recovery, every article but each
    // file's first gone: the sample finds every head, so only a probe can
    // tell the post is not coming.
    let posts: Vec<Post> = (0..3u8)
        .map(|at| Post::plain(&format!("lantern{at:02}.mkv"), payload(40 * ARTICLE, at)))
        .collect();
    let scenario = (0..3u32)
        .flat_map(|file| (1..40u32).map(move |ordinal| (file, ordinal)))
        .fold(Scenario::new(posts), |scenario, (file, ordinal)| {
            scenario.lost(file, ordinal)
        });
    let graph = run(&scenario).await;
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(
        failure.starts_with("health probe: 9 of 10 samples missing; health "),
        "{failure}"
    );
    assert!(failure.contains("below critical"), "{failure}");
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Health));
    // The probe asked by STAT alone: of the hundred and twenty articles, the
    // download asked for no more than it had when the probe was armed.
    assert!(graph.order.len() < 40, "{:?}", graph.order);
}

#[tokio::test]
async fn a_post_with_no_recovery_fails_at_critical_before_the_rest_is_fetched() {
    // One file of sixty articles and no recovery, every other article gone:
    // half of what is asked lands, so it does not look like a release nobody
    // uploaded and no probe is armed. With nothing that could rebuild the
    // file, its losses fail the post once its health is at or below
    // critical, long before its last article is asked for.
    let posts = vec![Post::plain("lantern.mkv", payload(60 * ARTICLE, 7))];
    let scenario = (1..60)
        .step_by(2)
        .fold(Scenario::new(posts), |scenario, ordinal| {
            scenario.lost(0, ordinal)
        });
    let graph = run(&scenario).await;
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(failure.starts_with("health "), "{failure}");
    assert!(failure.contains("below critical 85.0%"), "{failure}");
    // The losses rule it, not a probe's projection.
    assert!(!failure.starts_with("health probe"), "{failure}");
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Health));
    assert!(
        !graph.requests.contains_key(&(0, 59)),
        "{failure} {:?}",
        graph.order
    );
}

#[tokio::test]
async fn a_post_whose_only_recovery_is_par3_defers_to_it_below_critical() {
    // Two files of ten articles beside a PAR3 set of twelve blocks, three
    // articles of each lost: a probe projects the post below the 85% a post
    // without PAR2 bytes is held to. The PAR3 set is recovery the post
    // declares and it reaches past the loss, so the verdict waits on it,
    // and it rebuilds both files.
    let payload_files: Vec<(String, Vec<u8>)> = (0..2u8)
        .map(|at| (format!("lantern{at:02}.mkv"), payload(10 * ARTICLE, at)))
        .collect();
    let named: Vec<(&str, &[u8])> = payload_files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = payload_files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par3_set("lantern", &named, 12));
    let scenario = (0..2u32)
        .flat_map(|file| [2, 5, 8].map(|ordinal| (file, ordinal)))
        .fold(Scenario::new(posts), |scenario, (file, ordinal)| {
            scenario.lost(file, ordinal)
        });
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    for (name, _) in &payload_files {
        assert_eq!(graph.claims.get(name), Some(&Claim::Complete), "{name}");
    }
}

// The feed's vocabulary: a post taken down whole but for each file's first
// article. The sample finds every head, a STAT round finds nine of ten
// samples missing, and the post's health falls below the line a post with
// no recovery is held to, before the payload is fetched.
#[tokio::test]
async fn a_post_taken_down_fails_on_the_feeds_health_before_the_graph_rules() {
    let posts: Vec<Post> = (0..3u8)
        .map(|at| Post::plain(&format!("lantern{at:02}.mkv"), payload(40 * ARTICLE, at)))
        .collect();
    let scenario = (0..3u32)
        .flat_map(|file| (1..40u32).map(move |ordinal| (file, ordinal)))
        .fold(Scenario::new(posts), |scenario, (file, ordinal)| {
            scenario.lost(file, ordinal)
        });
    let graph = run(&scenario).await;
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(failure.contains("below critical 85.0%"), "{failure}");
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Health));
    // No recovery object ruled: the feed gave the post up.
    assert!(
        graph.graph().outcomes.is_empty(),
        "{:?}",
        graph.graph().outcomes
    );
}

// The feed's vocabulary yields to the graph's: a post alive above its
// critical line whose PAR2 set of forty blocks is refused forty-one ranges.
// The set rules impossible on the forty-first and the job fails in its
// words, before the payload's tail is fetched.
#[tokio::test]
async fn a_par2_set_refused_one_range_past_its_blocks_fails_in_its_own_words() {
    let bytes = payload(200 * ARTICLE, 7);
    let mut posts = vec![Post::plain("lantern.mkv", bytes.clone())];
    posts.extend(par2_set(
        "lantern",
        &[("lantern.mkv", bytes.as_slice())],
        40,
    ));
    let scenario = (1..=41).fold(Scenario::new(posts).leased(), |scenario, ordinal| {
        scenario.lost(0, ordinal)
    });
    let graph = run(&scenario).await;
    assert_eq!(
        graph.failure.as_deref(),
        Some(
            "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern.mkv): not repairable: \
             41 damaged slices, only 40 recovery blocks reachable"
        )
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert!(!graph.requests.contains_key(&(0, 199)), "{:?}", graph.order);
}

// The graph's vocabulary: a stored RAR5 set of four volumes, three of them
// refused an article each, beside one `.rev` volume. The `.rev` set can
// rebuild one volume, so it rules impossible and the job fails in its words.
#[tokio::test]
async fn a_rev_set_refused_three_volumes_with_one_rev_rules_impossible() {
    let body = super::rar_rev::body();
    let volumes =
        crate::pipeline::tests::direct_store::single_member_store_set("beacon.bin", &body, 4);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let mut posts = super::rar::posts(volumes.clone());
    posts.extend(super::rar_rev::rar5_revs("silver.horizon", &bytes, 1));
    let scenario = (1..4u32).fold(Scenario::new(posts), |scenario, volume| {
        scenario.lost(volume, 1)
    });
    let graph = run(&scenario).await;
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(
        failure.contains("not repairable: 3 damaged slices, only 1 recovery blocks reachable"),
        "{failure}"
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
}

// No recovery at all, every payload article refused: nothing can rebuild
// the file and the job fails.
#[tokio::test]
async fn a_post_with_no_recovery_whose_every_article_is_refused_fails() {
    let posts = vec![Post::plain("lantern.mkv", payload(4 * ARTICLE, 7))];
    let scenario = (0..4).fold(Scenario::new(posts), |scenario, ordinal| {
        scenario.lost(0, ordinal)
    });
    let graph = run(&scenario).await;
    assert!(
        matches!(
            graph.status,
            Some(crate::jobs::model::JobStatus::Failed { .. })
        ),
        "{:?} {:?}",
        graph.status,
        graph.failure
    );
}

// A whole archive volume no object opened is no usable file: its `.rar`
// lost, the `.r01` beside it arrives whole, and the job fails, not partial.
#[tokio::test]
async fn a_whole_volume_no_object_opened_is_not_a_usable_file() {
    let posts = vec![
        Post::plain("lantern.rar", payload(4 * ARTICLE, 3)),
        Post::plain("lantern.r01", payload(4 * ARTICLE, 4)),
    ];
    let scenario = (0..4).fold(Scenario::new(posts), |scenario, ordinal| {
        scenario.lost(0, ordinal)
    });
    let graph = run(&scenario).await;
    assert!(
        matches!(
            graph.status,
            Some(crate::jobs::model::JobStatus::Failed { .. })
        ),
        "{:?} {:?}",
        graph.status,
        graph.failure
    );
}

// A stored RAR holding two files and a PAR2 set over them, one recovery
// block in it. Two articles inside `quill.mkv` are refused by every
// server, past what the nested set can rebuild: the set rules impossible
// and says so, `ember.mkv` unpacks whole, and the job ends Partial on that
// one usable file, never Failed.
#[tokio::test]
async fn a_nested_par2_set_ruled_impossible_leaves_the_whole_file_partial() {
    let ember = payload(4 * ARTICLE + 9, 61);
    let quill = payload(8 * ARTICLE + 3, 62);
    let archive = par2_in_store(
        "inner",
        &[
            ("ember.mkv", ember.as_slice()),
            ("quill.mkv", quill.as_slice()),
        ],
        1,
    );
    let inside = super::deep::articles_inside(&archive.bytes, &quill);
    assert!(inside.len() >= 3, "{inside:?}");
    let scenario = Scenario::new(vec![archive])
        .lost(0, inside[0])
        .lost(0, inside[2]);
    let graph = run(&scenario).await;
    assert_eq!(
        graph.status,
        Some(crate::jobs::model::JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    let delivered = |name: &str| {
        graph
            .files
            .iter()
            .find(|(path, _)| path.rsplit('/').next() == Some(name))
            .map(|(_, bytes)| bytes.clone())
    };
    assert_eq!(
        delivered("ember.mkv"),
        Some(ember),
        "{:?}",
        graph.files.keys()
    );
    assert!(
        graph.graph().rulings.iter().any(|(_, ruling)| matches!(
            ruling,
            Some(weaver_graph::recovery::RepairOutcome::Impossible { .. })
        )),
        "{:?}",
        graph.graph().rulings
    );
}

// The same nested set with blocks to spare: one article inside `quill.mkv`
// refused by every server is rebuilt by the set inside the archive, and
// the job completes with both files whole.
#[tokio::test]
async fn a_nested_par2_set_rebuilds_a_refused_article_inside_a_stored_rar() {
    let ember = payload(4 * ARTICLE + 9, 61);
    let quill = payload(8 * ARTICLE + 3, 62);
    let archive = par2_in_store(
        "inner",
        &[
            ("ember.mkv", ember.as_slice()),
            ("quill.mkv", quill.as_slice()),
        ],
        8,
    );
    let inside = super::deep::articles_inside(&archive.bytes, &quill);
    let scenario = Scenario::new(vec![archive]).lost(0, inside[1]);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    let delivered = |name: &str| {
        graph
            .files
            .iter()
            .find(|(path, _)| path.rsplit('/').next() == Some(name))
            .map(|(_, bytes)| bytes.clone())
    };
    assert_eq!(delivered("ember.mkv"), Some(ember));
    assert_eq!(delivered("quill.mkv"), Some(quill));
}

// A job whose root is done has nothing downloading for the feed's health
// probe to rule on, so the feed forgets its probe as the job leaves the root,
// while the job itself is still in the pipeline.
#[tokio::test]
async fn the_feed_forgets_its_probe_when_the_job_leaves_the_root() {
    use crate::pipeline::graph::root::phase::RootPhase;
    use crate::pipeline::tests::{hear_graph_mail, retire_pipeline_database};

    let post = Post::plain("lantern.mkv", payload(4 * ARTICLE, 7));
    let articles = post.bytes.len().div_ceil(ARTICLE) as u32;
    let scenario = Scenario::new(vec![post]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    pipeline.post_probes.states.insert(
        JOB,
        crate::pipeline::post_health::ProbeState {
            rounds: 1,
            ..Default::default()
        },
    );
    for ordinal in 0..articles {
        serve(&mut pipeline, &scenario, (0, ordinal)).await;
        hear_graph_mail(&mut pipeline, JOB).await;
    }
    assert_eq!(pipeline.graph_jobs[&JOB].phase(), RootPhase::Done);
    assert!(pipeline.jobs.contains_key(&JOB));
    assert!(!pipeline.post_probes.states.contains_key(&JOB));
    retire_pipeline_database(pipeline).await;
}

// A post below its critical line is as dead after a restart as before one:
// the articles a run before the restart lost count against it, as the root
// was restored holding them, so a job restarted after every article fails
// on its health as a job that never restarted does.
#[tokio::test]
async fn a_post_dead_on_its_health_is_dead_after_a_restart() {
    let scenario = (2..8u32).fold(
        Scenario::new(vec![Post::plain("lantern.mkv", payload(10 * ARTICLE, 174))]),
        |scenario, ordinal| scenario.lost(0, ordinal),
    );
    let whole = run(&scenario).await;
    let failure = whole.failure.as_deref().unwrap_or_default();
    assert!(failure.contains("below critical"), "{failure}");
    assert_eq!(whole.failure_kind, Some(HistoryFailureKind::Health));
    let restarted = run_restarting(&scenario, &mut |_| {}).await;
    assert!(restarted.restarts > 1, "the run restarted");
    assert_eq!(restarted.status, whole.status);
    assert!(restarted.files.is_empty(), "{:?}", restarted.files.keys());
}

// A set starved of every recovery volume rules impossible after a restart
// as before one. Its volumes' losses reach it while it is out of the tree
// for a repair, and a crash then drops what waited for its return; a
// restore tells every loss again, so the set hears what the run that went
// down never had the chance to tell it.
#[tokio::test]
async fn a_set_starved_of_its_volumes_is_starved_after_a_restart() {
    let body = payload(10 * ARTICLE, 176);
    let mut posts = vec![Post::plain("lantern.mkv", body.clone())];
    posts.extend(par2_volumes("lantern", &[("lantern.mkv", &body)], 8, 2));
    // The index arrives; both volumes are lost whole.
    let volumes: Vec<(u32, u32)> = posts[2..]
        .iter()
        .zip(2u32..)
        .flat_map(|(post, file)| {
            (0..post.bytes.len().div_ceil(ARTICLE) as u32).map(move |o| (file, o))
        })
        .collect();
    let mut scenario = Scenario::new(posts);
    for ordinal in 2..8 {
        scenario = scenario.lost(0, ordinal);
    }
    for (file, ordinal) in volumes {
        scenario = scenario.lost(file, ordinal);
    }
    let whole = run(&scenario).await;
    assert!(
        matches!(whole.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        whole.status
    );
    assert_eq!(whole.failure_kind, Some(HistoryFailureKind::Repair));
    let restarted = run_restarting(&scenario, &mut |_| {}).await;
    assert!(restarted.restarts > 1, "the run restarted");
    assert_eq!(restarted.status, whole.status);
    assert!(restarted.files.is_empty(), "{:?}", restarted.files.keys());
}
