// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(super) use weaver_server_pipeline::pipeline::tests::engines::sfv::{
    assert_twin, assert_twin_but_text, crc, listing, notes,
};

// SFV listings: a listing that agrees, one that names a
// file the job does not carry, one that disagrees, one whose names differ
// from the members' only in case, one that leaves a file unnamed, two that
// contradict each other, and one naming a file the post lost. A listing
// never claims damage: one that disagrees, or names a file the post lost,
// leaves the job as the same posts without it.

use super::harness::{
    ARTICLE, Claim, JOB, Post, Scenario, assert_fetched_once, par2_set, payload, run, run_then,
};
use crate::jobs::model::JobStatus;
use weaver_server_base::events::model::{PipelineEvent, sfv_mismatch_message};
use weaver_server_pipeline::pipeline::tests::engines::restart_fixtures::{
    assert_restarted, run_restarting,
};

fn claims_all(claims: &std::collections::BTreeMap<String, Claim>, expected: Claim) -> bool {
    !claims.is_empty() && claims.values().all(|claim| *claim == expected)
}

#[tokio::test]
async fn an_sfv_whose_every_entry_matches_verifies_and_the_job_completes() {
    let lantern = payload(3 * ARTICLE, 21);
    let beacon = payload(2 * ARTICLE - 5, 22);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
        listing(
            "lantern.sfv",
            &[("lantern.bin", crc(&lantern)), ("beacon.bin", crc(&beacon))],
        ),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_eq!(notes(&graph), Vec::<String>::new());
}

#[tokio::test]
async fn an_sfv_entry_naming_no_member_never_fails_the_job() {
    let lantern = payload(3 * ARTICLE, 23);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        listing(
            "lantern.sfv",
            &[("lantern.bin", crc(&lantern)), ("quill.bin", 0x1234_5678)],
        ),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
}

// The posts of the mismatch case, with the listing or without it.
fn mismatch(listed: bool) -> Scenario {
    let lantern = payload(3 * ARTICLE, 24);
    let beacon = payload(2 * ARTICLE, 25);
    let mut posts = vec![
        Post::plain("lantern.mkv", lantern.clone()),
        Post::plain("beacon.mkv", beacon.clone()),
    ];
    if listed {
        posts.push(listing(
            "lantern.sfv",
            &[
                ("lantern.mkv", crc(&lantern)),
                ("beacon.mkv", crc(&beacon) ^ 0xffff_ffff),
            ],
        ));
    }
    Scenario::new(posts)
}

// A listing that disagrees with a file whose bytes arrived as posted does
// not mark it: both files are delivered as posted and the job completes,
// as the same posts without the listing do. The job notes the file once.
#[tokio::test]
async fn an_sfv_mismatch_leaves_the_job_as_without_the_listing() {
    let graph = run(&mismatch(true)).await;
    assert!(graph.refetched().is_empty());
    assert_eq!(
        graph.status,
        Some(JobStatus::Complete),
        "{:?}",
        graph.failure
    );
    assert_eq!(graph.failure_kind, None);
    assert_eq!(graph.files.len(), 3, "{:?}", graph.files.keys());
    let view = graph.graph();
    assert_eq!(view.claims["lantern.mkv"], Claim::Complete);
    assert_eq!(view.claims["beacon.mkv"], Claim::Complete);
    assert_eq!(
        notes(&graph),
        ["beacon.mkv does not match its CRC32 in lantern.sfv"]
    );
    assert_twin(&graph, &run(&mismatch(false)).await);
}

// An entry naming a member in another case names it: the listing disagrees
// with the member, so the job notes it, and still ends as the post without
// the listing.
#[tokio::test]
async fn an_sfv_naming_a_member_in_another_case_binds_and_verifies_it() {
    let lantern = payload(3 * ARTICLE, 26);
    let posts = |listed: bool| {
        let mut posts = vec![Post::plain("lantern.bin", lantern.clone())];
        if listed {
            posts.push(listing(
                "lantern.sfv",
                &[("LANTERN.BIN", crc(&lantern) ^ 1)],
            ));
        }
        Scenario::new(posts)
    };
    let graph = run(&posts(true)).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        notes(&graph),
        ["lantern.bin does not match its CRC32 in lantern.sfv"]
    );
    assert_twin(&graph, &run(&posts(false)).await);
}

// A payload file no listing names is left as posted, and the job completes.
#[tokio::test]
async fn a_file_the_listing_does_not_name_completes_unchecked() {
    let lantern = payload(3 * ARTICLE, 30);
    let beacon = payload(2 * ARTICLE, 31);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
        listing("lantern.sfv", &[("lantern.bin", crc(&lantern))]),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.graph().claims["beacon.bin"], Claim::Complete);
    assert_eq!(graph.files["lantern beacon/beacon.bin"], beacon);
    assert_eq!(notes(&graph), Vec::<String>::new());
}

// Two listings that disagree about one name each say what they say: that
// file is not failed on either, and the listing it disagrees with notes it.
#[tokio::test]
async fn listings_that_disagree_about_a_name_fail_neither_way() {
    let lantern = payload(3 * ARTICLE, 33);
    let beacon = payload(2 * ARTICLE, 34);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
        listing(
            "lantern.sfv",
            &[("lantern.bin", crc(&lantern)), ("beacon.bin", crc(&beacon))],
        ),
        listing("lantern.extra.sfv", &[("lantern.bin", 0x0bad_f00d)]),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.graph().claims["beacon.bin"], Claim::Complete);
    assert_eq!(
        notes(&graph),
        ["lantern.bin does not match its CRC32 in lantern.extra.sfv"]
    );
}

// A file the listing names whose every article is lost is the one file not
// delivered whole: the job ends Partial, as the same posts without the
// listing do. The listing claims no damage of its own, and notes nothing of
// a file that never arrived whole.
#[tokio::test]
async fn a_listed_file_whose_articles_are_all_lost_ends_partial_marking_it() {
    let lantern = payload(2 * ARTICLE, 35);
    let beacon = payload(2 * ARTICLE, 36);
    let posts = |listed: bool| {
        let mut posts = vec![
            Post::plain("lantern.mkv", lantern.clone()),
            Post::plain("beacon.mkv", beacon.clone()),
        ];
        if listed {
            posts.push(listing(
                "lantern.sfv",
                &[("lantern.mkv", crc(&lantern)), ("beacon.mkv", crc(&beacon))],
            ));
        }
        Scenario::new(posts).lost(1, 0).lost(1, 1)
    };
    let graph = run(&posts(true)).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    let view = graph.graph();
    assert_eq!(view.claims["lantern.mkv"], Claim::Complete);
    assert_ne!(view.claims["beacon.mkv"], Claim::Complete);
    assert_eq!(notes(&graph), Vec::<String>::new());
    assert_twin(&graph, &run(&posts(false)).await);
}

// The note is said once, when the job is done: a pass of the pipeline after
// it, with nothing to do, says nothing more.
#[tokio::test]
async fn an_idle_pass_after_the_job_is_done_notes_nothing_again() {
    let mut heard = Vec::new();
    let graph = run_then(&mismatch(true), async |pipeline, _| {
        let mut sent = pipeline.event_tx.subscribe();
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        weaver_server_pipeline::pipeline::tests::pump_pipeline_runtime_queues(pipeline).await;
        while let Ok(event) = sent.try_recv() {
            heard.push(event);
        }
    })
    .await;
    assert_eq!(
        notes(&graph),
        ["beacon.mkv does not match its CRC32 in lantern.sfv"]
    );
    assert!(
        !heard
            .iter()
            .any(|event| matches!(event, PipelineEvent::SfvMismatch { .. })),
        "{heard:?}"
    );
}

// A job taken down and restored on the way notes the file as the run with
// no restart does: the restored listing asks the source again.
#[tokio::test]
async fn a_restored_job_notes_the_file_as_the_run_without_a_restart() {
    let whole = run(&mismatch(true)).await;
    let restarted = run_restarting(&mismatch(true)).await;
    assert_restarted(&restarted, &whole);
    let noted: Vec<String> = restarted
        .events
        .iter()
        .filter_map(|event| match event {
            PipelineEvent::SfvMismatch {
                job_id,
                file,
                listing,
            } if *job_id == JOB => Some(sfv_mismatch_message(file, listing)),
            _ => None,
        })
        .collect();
    assert_eq!(noted, notes(&whole));
}

// A file a PAR2 set covers is not asked for, so a listing that disagrees
// with it notes nothing: PAR2 says what became of it. The job ends as PAR2
// rules it.
#[tokio::test]
async fn a_file_a_par2_set_covers_is_not_noted() {
    let lantern = payload(3 * ARTICLE, 37);
    let posts = |listed: bool| {
        let mut posts = vec![Post::plain("lantern.bin", lantern.clone())];
        posts.extend(par2_set("lantern", &[("lantern.bin", &lantern)], 4));
        if listed {
            posts.push(listing(
                "lantern.sfv",
                &[("lantern.bin", crc(&lantern) ^ 1)],
            ));
        }
        Scenario::new(posts)
    };
    let graph = run(&posts(true)).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(notes(&graph), Vec::<String>::new());
    assert_twin(&graph, &run(&posts(false)).await);
}

// The delivered files by base name.
fn delivered(files: &std::collections::BTreeMap<String, Vec<u8>>) -> Vec<(String, Vec<u8>)> {
    let mut named: Vec<_> = files
        .iter()
        .map(|(path, bytes)| {
            let name = path.rsplit('/').next().unwrap_or(path).to_owned();
            (name, bytes.clone())
        })
        .collect();
    named.sort();
    named
}

fn names(files: &std::collections::BTreeMap<String, Vec<u8>>) -> Vec<String> {
    delivered(files).into_iter().map(|(name, _)| name).collect()
}

// A file posted under a name that says nothing, and a listing whose entry
// for it no file answers: `lose` loses the file's last article.
fn obfuscated(lose: bool) -> Scenario {
    let lantern = payload(3 * ARTICLE, 40);
    let beacon = payload(2 * ARTICLE, 41);
    let scenario = Scenario::new(vec![
        Post::plain("e93a07c1d25b4f68", lantern.clone()),
        Post::plain("beacon.mkv", beacon.clone()),
        listing(
            "release.sfv",
            &[("Lantern.MKV", crc(&lantern)), ("beacon.mkv", crc(&beacon))],
        ),
    ]);
    if lose { scenario.lost(0, 2) } else { scenario }
}

// The listing names the file whose CRC32 its unanswered entry states, in
// the listing's case, and the job ends as it would have under that name.
#[tokio::test]
async fn a_listing_names_the_one_file_with_its_unanswered_crc() {
    let graph = run(&obfuscated(false)).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        names(&graph.files),
        ["Lantern.MKV", "beacon.mkv", "release.sfv"]
    );
    assert_eq!(
        delivered(&graph.files)[0].1,
        payload(3 * ARTICLE, 40),
        "the named file is the posted one"
    );
    assert_eq!(notes(&graph), Vec::<String>::new());
}

// A file that did not arrive whole is not named: its bytes were never
// checked whole.
#[tokio::test]
async fn a_listing_names_no_file_that_arrived_damaged() {
    let graph = run(&obfuscated(true)).await;
    let named = names(&graph.files);
    assert!(
        !named.iter().any(|name| name == "Lantern.MKV"),
        "{named:?} {:?}",
        graph.status
    );
}

// Two unanswered entries that state the CRC32 of one file: the listing
// cannot say which name is its, and gives neither.
#[tokio::test]
async fn a_listing_names_no_file_two_of_its_entries_state_the_crc_of() {
    let lantern = payload(3 * ARTICLE, 42);
    let graph = run(&Scenario::new(vec![
        Post::plain("e93a07c1d25b4f68", lantern.clone()),
        listing(
            "release.sfv",
            &[
                ("lantern.mkv", crc(&lantern)),
                ("beacon.mkv", crc(&lantern)),
            ],
        ),
    ]))
    .await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(names(&graph.files), ["e93a07c1d25b4f68", "release.sfv"]);
}

// A file a PAR2 set describes takes the set's name, whatever the listing
// says of it, and keeps it.
#[tokio::test]
async fn a_par2_name_stands_over_the_listings() {
    let lantern = payload(3 * ARTICLE, 43);
    let mut posts = vec![Post::plain("e93a07c1d25b4f68", lantern.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &lantern)], 4));
    posts.push(listing("release.sfv", &[("other.bin", crc(&lantern))]));
    let graph = run(&Scenario::new(posts)).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let named = names(&graph.files);
    assert!(named.iter().any(|name| name == "lantern.bin"), "{named:?}");
    assert!(!named.iter().any(|name| name == "other.bin"), "{named:?}");
}

// A job taken down and restored on the way names the file as the run with
// no restart does.
#[tokio::test]
async fn a_restored_job_names_the_file_as_the_run_without_a_restart() {
    let whole = run(&obfuscated(false)).await;
    let restarted = run_restarting(&obfuscated(false)).await;
    assert_restarted(&restarted, &whole);
    assert_eq!(
        names(&restarted.files),
        ["Lantern.MKV", "beacon.mkv", "release.sfv"]
    );
}
