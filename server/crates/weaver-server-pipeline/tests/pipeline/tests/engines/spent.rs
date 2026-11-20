// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a graph job spends is left out of what it hands over: recovery
// volumes, bound or only named so, and the pieces a join consumed.

use super::harness::{ARTICLE, JOB, Post, Scenario, par2_set, payload, run, run_then};
use crate::jobs::model::JobStatus;
use weaver_server_pipeline::pipeline::tests::engines::spent_fixtures::{
    owned_working_dirs, pin_spent,
};
use weaver_server_pipeline::pipeline::tests::history_fixture::delete_history_batch;

// A PAR2 volume whose first article is lost never says by its bytes which
// set it is, but the post names it a volume: it is spent all the same, and
// the job hands over the data member alone.
#[tokio::test]
async fn a_recovery_volume_whose_head_was_lost_is_spent() {
    let data = payload(4 * ARTICLE, 41);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let graph = run(&Scenario::new(posts).lost(2, 0)).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/lantern.bin".to_string()],
    );
    assert!(graph.files["lantern beacon/lantern.bin"] == data);
}

// A consumed input that cannot be removed, held open by a scanner or
// refused by a share, costs the delivery nothing: the job completes with
// its payload delivered, and the input stays in the working directory.
// The input is made a directory with an entry in it, which no file
// removal takes away on any host or under any user.
#[tokio::test]
async fn a_consumed_input_that_cannot_be_removed_stays_and_the_job_completes() {
    let data = payload(4 * ARTICLE, 47);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let (pinned_tx, pinned_rx) = std::sync::mpsc::channel();
    let scenario = Scenario::new(posts);
    let scenario = pin_spent(scenario, move |spent: &[std::path::PathBuf]| {
        let pinned = spent
            .iter()
            .find(|path| path.is_file())
            .expect("a recovery set is spent before delivery")
            .clone();
        std::fs::remove_file(&pinned).unwrap();
        std::fs::create_dir(&pinned).unwrap();
        std::fs::write(pinned.join("held"), b"held").unwrap();
        let _ = pinned_tx.send(pinned.file_name().unwrap().to_owned());
    });
    let graph = run(&scenario).await;
    let pinned = pinned_rx
        .try_recv()
        .expect("the delivery removed its consumed inputs");
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/lantern.bin".to_string()],
    );
    assert!(graph.files["lantern beacon/lantern.bin"] == data);
    let left = format!("/{}/held", pinned.to_string_lossy());
    assert!(
        graph
            .retained_files
            .keys()
            .any(|path| path.ends_with(&left)),
        "the consumed input left the working directory: {:?}",
        graph.retained_files.keys().collect::<Vec<_>>()
    );
}

// The working directory a consumed input stayed in stays the job's, marker
// and all, once the job is done: deleting the job's history with its files
// takes it, as it takes the complete directory, once the input can go.
#[tokio::test]
async fn a_left_input_s_working_directory_goes_with_the_history_delete() {
    let data = payload(4 * ARTICLE, 53);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let scenario = Scenario::new(posts);
    let scenario = pin_spent(scenario, move |spent: &[std::path::PathBuf]| {
        let pinned = spent
            .iter()
            .find(|path| path.is_file())
            .expect("a recovery set is spent before delivery");
        std::fs::remove_file(pinned).unwrap();
        std::fs::create_dir(pinned).unwrap();
        std::fs::write(pinned.join("held"), b"held").unwrap();
        // The harness admits with the empty marker older versions wrote; an
        // admission now writes the job's own.
        let working_dir = pinned.parent().unwrap();
        crate::jobs::working_dir::mark_weaver_owned_working_dir(
            working_dir.parent().unwrap(),
            working_dir,
            JOB,
        )
        .unwrap();
    });
    let graph = run_then(&scenario, async |pipeline, complete_dir| {
        let jobs = std::collections::BTreeSet::from([JOB]);
        let owned = owned_working_dirs(pipeline, &jobs);
        let [working_dir] = owned[&JOB].as_slice() else {
            panic!("the job owns one working directory: {owned:?}");
        };
        let delivered = complete_dir.join("lantern beacon");
        assert!(delivered.join("lantern.bin").is_file());

        let outcome = delete_history_batch(pipeline, &[JOB], true).await;
        assert!(outcome.refused.is_empty(), "{:?}", outcome.refused);
        let deleted = outcome.deleted[&JOB].as_ref().unwrap();
        assert!(deleted.left_in_place.is_empty());
        assert!(!working_dir.exists(), "the intermediate directory stayed");
        assert!(!delivered.exists(), "the complete directory stayed");
    })
    .await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
}

// While the root fetches recovery volumes for a lost article the job shows
// it is fetching repair data, not repairing; once the repair run is out it
// shows repairing, and is no longer fetching. The payload's last article is
// the one lost, so the loss is told once the rest of the payload is in and
// the recovery comes after it: a loss told earlier raises the recovery
// beside the payload, and the repair may find it in hand.
#[tokio::test]
async fn a_graph_job_fetches_repair_data_before_it_repairs() {
    let data = payload(4 * ARTICLE, 43);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let graph = run(&Scenario::new(posts).lost(0, 3)).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert!(graph.files["lantern beacon/lantern.bin"] == data);
    let shown = &graph.graph().shown;
    assert!(
        shown.contains(&(JobStatus::Downloading, true)),
        "never shown fetching: {shown:?}"
    );
    assert!(
        shown.contains(&(JobStatus::Repairing, false)),
        "never shown repairing: {shown:?}"
    );
    assert!(
        !shown
            .iter()
            .any(|(status, fetching)| *fetching && *status != JobStatus::Downloading),
        "fetching shown outside the download: {shown:?}"
    );
}

// A whole movie beside a file posted under an older recovery volume's name,
// `.p01` or `.par01`, beside a `.par2` of its stem, that no object holds:
// its bytes say nothing an object reads. It is recovery data by its name
// among the names beside it, as the `.par2` is by its own: no file of the
// delivery, whole or short. The job hands over the movie and ends
// Complete, also when the recovery file lost an article, and that loss
// costs the job's health nothing.
#[tokio::test]
async fn a_recovery_file_no_object_holds_is_no_file_of_the_delivery() {
    let movie = payload(4 * ARTICLE + 17, 91);
    for name in ["lantern.p01", "lantern.par01", "lantern.vol00+01.par2"] {
        for lost in [false, true] {
            let posts = vec![
                Post::plain("lantern.mkv", movie.clone()),
                Post::plain(name, payload(3 * ARTICLE + 5, 203)),
                Post::plain("lantern.par2", payload(ARTICLE, 7)),
            ];
            let mut scenario = Scenario::new(posts);
            if lost {
                scenario = scenario.lost(1, 1);
            }
            let graph = run(&scenario).await;
            let delivery = &graph.graph().delivery;
            let said = format!(
                "{name}, lost {lost}: {:?} {:?} whole {:?} damaged {:?}",
                graph.status, graph.failure, delivery.whole, delivery.damaged
            );
            assert_eq!(graph.status, Some(JobStatus::Complete), "{said}");
            assert_eq!(delivery.whole, ["lantern.mkv"], "{said}");
            assert!(delivery.damaged.is_empty(), "{said}");
            assert_eq!(
                graph.files.keys().cloned().collect::<Vec<_>>(),
                vec!["lantern beacon/lantern.mkv".to_string()],
                "{said}"
            );
            assert_eq!(
                graph.report.as_ref().map(|report| report.health),
                Some(1000),
                "{said}"
            );
        }
    }
}

// A file whose name has a letter and digits for an extension, `.p12` or
// `.par01`, with no `.par` or `.par2` of its stem posted, is a plain file:
// a name alone never makes a posted file recovery data. Posted alone or
// beside a movie, it is handed over.
#[tokio::test]
async fn a_file_named_like_an_old_recovery_volume_with_no_set_is_delivered() {
    let movie = payload(4 * ARTICLE + 17, 91);
    let notes = payload(3 * ARTICLE + 5, 203);
    let alone = run(&Scenario::new(vec![Post::plain(
        "notes.p12",
        notes.clone(),
    )]))
    .await;
    assert_eq!(
        alone.status,
        Some(JobStatus::Complete),
        "{:?}",
        alone.failure
    );
    assert_eq!(alone.graph().delivery.whole, ["notes.p12"]);
    assert!(alone.files["lantern beacon/notes.p12"] == notes);
    let beside = run(&Scenario::new(vec![
        Post::plain("lantern.mkv", movie.clone()),
        Post::plain("notes.p12", notes.clone()),
        Post::plain("notes.par01", payload(2 * ARTICLE + 9, 17)),
    ]))
    .await;
    let delivery = &beside.graph().delivery;
    assert_eq!(
        beside.status,
        Some(JobStatus::Complete),
        "{:?}",
        beside.failure
    );
    assert_eq!(delivery.whole, ["lantern.mkv", "notes.p12", "notes.par01"]);
    assert_eq!(
        beside.files.keys().cloned().collect::<Vec<_>>(),
        [
            "lantern beacon/lantern.mkv",
            "lantern beacon/notes.p12",
            "lantern beacon/notes.par01",
        ],
    );
}

// A plain `.p12` that lost an article is a damaged file of the delivery,
// and its loss counts toward the job's health as any payload file's does.
#[tokio::test]
async fn a_plain_file_named_like_an_old_recovery_volume_counts_its_loss() {
    let movie = payload(4 * ARTICLE + 17, 91);
    let graph = run(&Scenario::new(vec![
        Post::plain("lantern.mkv", movie.clone()),
        Post::plain("notes.p12", payload(3 * ARTICLE + 5, 203)),
    ])
    .lost(1, 1))
    .await;
    let delivery = &graph.graph().delivery;
    let report = graph
        .report
        .as_ref()
        .map(|report| (report.health, report.failed_bytes));
    let said = format!("{:?} {:?} {report:?}", graph.status, graph.failure);
    assert_eq!(graph.status, Some(JobStatus::Partial), "{said}");
    assert_eq!(delivery.whole, ["lantern.mkv"], "{said}");
    assert_eq!(delivery.damaged, ["notes.p12"], "{said}");
    assert!(
        report.is_some_and(|(health, failed)| health < 1000 && failed > 0),
        "{said}"
    );
}

// A `.p01` beside a PAR2 set of its stem that covers the movie, its bytes
// no volume of the set, is recovery data: an article it lost leaves the
// movie whole, the job Complete, the `.p01` out of the delivery and the
// job's health untouched.
#[tokio::test]
async fn an_unused_old_volume_beside_its_set_costs_nothing_when_it_loses_an_article() {
    let movie = payload(4 * ARTICLE + 17, 91);
    let mut posts = vec![
        Post::plain("lantern.mkv", movie.clone()),
        Post::plain("lantern.p01", payload(3 * ARTICLE + 5, 203)),
    ];
    posts.extend(par2_set("lantern", &[("lantern.mkv", &movie)], 2));
    let graph = run(&Scenario::new(posts).lost(1, 1)).await;
    let delivery = &graph.graph().delivery;
    let report = graph
        .report
        .as_ref()
        .map(|report| (report.health, report.failed_bytes));
    let said = format!("{:?} {:?} {report:?}", graph.status, graph.failure);
    assert_eq!(graph.status, Some(JobStatus::Complete), "{said}");
    assert_eq!(delivery.whole, ["lantern.mkv"], "{said}");
    assert!(delivery.damaged.is_empty(), "{said}");
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/lantern.mkv".to_string()],
        "{said}"
    );
    assert_eq!(report.map(|(health, _)| health), Some(1000), "{said}");
}
