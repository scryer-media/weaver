// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Zips that hold the same member names. The ledger names each member by its
// zip's rank: the first zip's members keep their names and every later
// zip's are numbered, whatever order the zips arrive in. A member the
// ledger names again while the job downloads keeps its file where it opened;
// every file moves once, when the job is ready to deliver, to the name the
// ledger settled on. So the delivered names are the same for every arrival
// order, each file is renamed at most once, and the database is asked to
// change a name only a few times, however many times the ledger changed it.

use std::collections::BTreeMap;
use std::io::{Cursor, Write};

use ::zip::ZipWriter;

use super::harness::{ARTICLE, Outcome, Post, Scenario, payload, run};
use crate::jobs::model::JobStatus;
use weaver_server_pipeline::pipeline::tests::direct_store::multi_member_store_set;
use weaver_server_pipeline::pipeline::tests::engines::archive_scenario_fixtures::sets::{
    namespace_move_counts, namespace_rename_count,
};
use weaver_server_pipeline::pipeline::tests::engines::restart_fixtures::assert_restarted;
use weaver_server_pipeline::pipeline::tests::engines::restart_move_fixture::{
    At, Stop, run_stopping_at, startup_checks, waiting,
};

const FOLDER: &str = "lantern beacon";
const NAMES: usize = 10;

// The order the zips arrive in.
#[derive(Clone, Copy, Debug)]
enum Arrival {
    // In rank order: no member is ever named again.
    Rank,
    // Last ranked first: each zip that arrives outranks every one before it.
    Reverse,
    // Every zip in rank order but the first, which arrives last: the
    // longest chain of moves, one link a zip.
    TopLast,
}

fn stored_zip(entries: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, bytes) in entries {
        writer
            .start_file(name.as_str(), super::zip::stored())
            .unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

// `zips` stored zips, `pack000.zip` onward, each holding `names` members
// `data0.bin` onward; each member's bytes say its zip and its name.
fn packs(zips: usize, names: usize) -> Vec<(String, Vec<u8>)> {
    (0..zips)
        .map(|at| {
            let entries: Vec<(String, Vec<u8>)> = (0..names)
                .map(|name| {
                    (
                        format!("data{name}.bin"),
                        format!("z{at:03}m{name:03}").into_bytes(),
                    )
                })
                .collect();
            (format!("pack{at:03}.zip"), stored_zip(&entries))
        })
        .collect()
}

fn scenario(zips: usize, names: usize, arrival: Arrival, settle: bool) -> Scenario {
    let posts = packs(zips, names)
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    let mut scenario = Scenario::new(posts);
    scenario = match arrival {
        Arrival::Rank => scenario,
        Arrival::Reverse => scenario.backwards(),
        Arrival::TopLast => {
            let order: Vec<u32> = (1..zips as u32).chain([0]).collect();
            scenario.together(&order)
        }
    };
    if settle {
        scenario = scenario.settled();
    }
    scenario
}

// The names the job delivered before files moved only at delivery, captured
// from a run of that code: the first zip's members keep their names, and
// zip `z`'s member `data{n}.bin` is `data{n} (z).bin`.
fn expected(zips: usize, names: usize) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    for at in 0..zips {
        for name in 0..names {
            let file = match at {
                0 => format!("data{name}.bin"),
                _ => format!("data{name} ({at}).bin"),
            };
            files.insert(
                format!("{FOLDER}/{file}"),
                format!("z{at:03}m{name:03}").into_bytes(),
            );
        }
    }
    files
}

// What a run cost in names, read from the writer's and the checkpoint's
// counts.
#[derive(Debug, PartialEq, Eq)]
struct Cost {
    // Times the ledger named again a member with a file.
    renamed: u64,
    // Files moved on disk.
    moves: u64,
    // Files opened under a waiting name.
    waiting_opens: u64,
    // Database writes that changed a member's name, and the member rows
    // they changed so.
    naming_writes: usize,
    naming_rows: usize,
}

async fn delivered(scenario: &Scenario) -> (Outcome, Cost) {
    let (moves, waiting_opens) = namespace_move_counts();
    let renamed = namespace_rename_count();
    let outcome = run(scenario).await;
    let (after_moves, after_waiting) = namespace_move_counts();
    let (naming_writes, naming_rows) = outcome.graph.naming_writes();
    let cost = Cost {
        renamed: namespace_rename_count() - renamed,
        moves: after_moves - moves,
        waiting_opens: after_waiting - waiting_opens,
        naming_writes,
        naming_rows,
    };
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    (outcome, cost)
}

// The names are the rank's names in every order, settled or not, and each
// file moved at most once: in rank order none, last ranked first every one
// (each but the last ranked's from under a waiting name), and with the first
// ranked last every one, the first ranked's from under a waiting name.
async fn shared_names(zips: usize, arrival: Arrival, settle: bool) {
    let names = NAMES;
    let (outcome, cost) = delivered(&scenario(zips, names, arrival, settle)).await;
    assert_eq!(outcome.files, expected(zips, names), "{arrival:?} {settle}");
    let (k, n) = (zips as u64, names as u64);
    let (moves, waiting_opens, chain) = match arrival {
        Arrival::Rank => (0, 0, 0),
        Arrival::Reverse => (n * k, n * (k - 1), 2),
        Arrival::TopLast => (n * k, n, k as usize),
    };
    eprintln!(
        "NAMES AT DELIVERY | {arrival:?} | settle {settle} | K {zips} | N {names} | {cost:?}"
    );
    assert_eq!(cost.moves, moves, "{arrival:?} {settle}: {cost:?}");
    assert_eq!(
        cost.waiting_opens, waiting_opens,
        "{arrival:?} {settle}: {cost:?}"
    );
    assert!(cost.moves <= n * k, "at most one move a file: {cost:?}");
    // Every write that changes a name carries the ledger's names while the
    // job downloads, or one link of the chain of moves at delivery; the
    // first then records every name wanted, and each later link records
    // the files that moved in the one before. No member row changes its
    // names more than once a write.
    // Settled, each arrival's ordinary checkpoint commits before the next
    // arrives, and carries the names that arrival changed: one write an
    // arrival, never one a rename.
    let arrivals = if settle { zips - 1 } else { 0 };
    assert!(
        cost.naming_writes <= arrivals + chain + 1,
        "{arrival:?} {settle}: {cost:?}"
    );
    assert!(
        cost.naming_rows <= cost.naming_writes * names * zips,
        "{arrival:?} {settle}: {cost:?}"
    );
}

#[tokio::test]
async fn shared_names_in_rank_order_k5() {
    shared_names(5, Arrival::Rank, false).await;
}
#[tokio::test]
async fn shared_names_in_rank_order_settled_k5() {
    shared_names(5, Arrival::Rank, true).await;
}
#[tokio::test]
async fn shared_names_in_reverse_k5() {
    shared_names(5, Arrival::Reverse, false).await;
}
#[tokio::test]
async fn shared_names_in_reverse_settled_k5() {
    shared_names(5, Arrival::Reverse, true).await;
}
#[tokio::test]
async fn shared_names_top_ranked_last_k5() {
    shared_names(5, Arrival::TopLast, false).await;
}
#[tokio::test]
async fn shared_names_top_ranked_last_settled_k5() {
    shared_names(5, Arrival::TopLast, true).await;
}
#[tokio::test]
async fn shared_names_in_rank_order_k20() {
    shared_names(20, Arrival::Rank, false).await;
}
#[tokio::test]
async fn shared_names_in_rank_order_settled_k20() {
    shared_names(20, Arrival::Rank, true).await;
}
#[tokio::test]
async fn shared_names_in_reverse_k20() {
    shared_names(20, Arrival::Reverse, false).await;
}
#[tokio::test]
async fn shared_names_in_reverse_settled_k20() {
    shared_names(20, Arrival::Reverse, true).await;
}
#[tokio::test]
async fn shared_names_top_ranked_last_k20() {
    shared_names(20, Arrival::TopLast, false).await;
}
#[tokio::test]
async fn shared_names_top_ranked_last_settled_k20() {
    shared_names(20, Arrival::TopLast, true).await;
}
#[tokio::test]
async fn shared_names_in_reverse_k40() {
    shared_names(40, Arrival::Reverse, false).await;
}
#[tokio::test]
async fn shared_names_in_reverse_settled_k40() {
    shared_names(40, Arrival::Reverse, true).await;
}

// The same zips as the members of one outer archive: a stored RAR5 set of
// `zips` volumes, or one stored zip. The inner zips' members are delivered
// under the same names, in either order.
fn nested_scenario(outer_rar: bool, reverse: bool) -> Scenario {
    let (zips, names) = (5, NAMES);
    let inner = packs(zips, names);
    let posts = if outer_rar {
        let entries: Vec<(&str, Vec<u8>)> = inner
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.clone()))
            .collect();
        multi_member_store_set(&entries, zips)
            .into_iter()
            .enumerate()
            .map(|(index, (_, bytes))| {
                Post::plain(&format!("Duck.Tales.part{:02}.rar", index + 1), bytes)
            })
            .collect()
    } else {
        vec![Post::plain("outer.zip", stored_zip(&inner))]
    };
    let mut scenario = Scenario::new(posts);
    if reverse {
        scenario = scenario.backwards();
    }
    scenario
}

async fn nested_names(outer_rar: bool, reverse: bool) {
    let (zips, names) = (5, NAMES);
    let (outcome, cost) = delivered(&nested_scenario(outer_rar, reverse)).await;
    assert_eq!(outcome.files, expected(zips, names), "{cost:?}");
    assert!(cost.moves <= (zips * names) as u64, "{cost:?}");
}

#[tokio::test]
async fn nested_in_an_outer_rar_delivers_the_rank_names() {
    nested_names(true, false).await;
}
#[tokio::test]
async fn nested_in_an_outer_rar_last_first_delivers_the_rank_names() {
    nested_names(true, true).await;
}
#[tokio::test]
async fn nested_in_an_outer_zip_delivers_the_rank_names() {
    nested_names(false, false).await;
}
#[tokio::test]
async fn nested_in_an_outer_zip_last_first_delivers_the_rank_names() {
    nested_names(false, true).await;
}

// Three zips each holding one member of three articles, the first ranked
// last, so each later zip's member is named again as the first arrives,
// with the middle article of the zips at `lost` gone and nothing to rebuild
// it. Whatever end the job comes to, the files it delivers or keeps sit at
// the names they had when files moved while the job downloaded: the names
// below were captured from a run of that code, and are the ledger's.
async fn short_end(member: &str, lost: &[u32]) -> (Outcome, Vec<String>) {
    let posts = (0..3)
        .map(|at| {
            let bytes = payload(3 * ARTICLE, 40 + at as u8);
            let name = format!("pack{at:03}.zip");
            Post::plain(&name, stored_zip(&[(member.to_owned(), bytes)]))
        })
        .collect();
    let mut scenario = Scenario::new(posts).together(&[1, 2, 0]);
    for zip in lost {
        scenario = scenario.lost(*zip, 1);
    }
    let renamed = namespace_rename_count();
    let outcome = run(&scenario).await;
    assert!(
        namespace_rename_count() > renamed,
        "a member is named again"
    );
    // A delivered job's files, or the files a failed one keeps, but for the
    // job's own marker and the envelopes the zips read through.
    let names = outcome
        .files
        .keys()
        .chain(outcome.retained_files.keys())
        .filter(|name| !name.ends_with(".weaver-job-dir") && !name.contains("graph-envelope-"))
        .cloned()
        .collect();
    (outcome, names)
}

fn kept(names: &[&str]) -> Vec<String> {
    names
        .iter()
        .map(|name| format!("{FOLDER}/{name}"))
        .collect()
}

// The two whole zips' members are delivered, the short zip as posted in
// place of its own.
#[tokio::test]
async fn a_job_ending_partial_delivers_the_ledger_names() {
    let (outcome, names) = short_end("clip.mkv", &[2]).await;
    assert_eq!(outcome.status, Some(JobStatus::Partial));
    assert_eq!(names, kept(&["clip (1).mkv", "clip.mkv", "pack002.zip"]));
}

// Nothing usable comes whole: the job is ruled Failed at its delivery and
// keeps the two whole members at the ledger's names.
#[tokio::test]
async fn a_job_ruled_failed_keeps_its_files_at_the_ledger_names() {
    let (outcome, names) = short_end("data0.bin", &[2]).await;
    assert!(
        matches!(&outcome.status, Some(JobStatus::Failed { error }) if error.starts_with("no usable file")),
        "{:?}",
        outcome.status
    );
    assert_eq!(names, kept(&["data0 (1).bin", "data0.bin", "pack002.zip"]));
}

// Every zip short: the feed gives the post up for its health, and the job
// keeps the zips as posted, no member of them.
#[tokio::test]
async fn a_job_failed_for_its_health_keeps_the_zips_as_posted() {
    let (outcome, names) = short_end("clip.mkv", &[0, 1, 2]).await;
    assert!(
        matches!(&outcome.status, Some(JobStatus::Failed { error }) if error.starts_with("health")),
        "{:?}",
        outcome.status
    );
    assert_eq!(names, kept(&["pack000.zip", "pack001.zip", "pack002.zip"]));
}

// Down mid-job, after the ledger named a member again and before the job
// was ready to deliver. Committed, the database holds where each file sits
// beside the name it wants; not committed, it holds neither the rename nor
// the files that step wrote. Either way the restore opens every file where
// it sits, and each still moves once, at the delivery. The run delivers what
// the run that never went down delivers, and fetches nothing it had on disk.
async fn down_mid_job(stop: Stop) {
    // Last ranked first, as the restart's queue hands the zips out.
    let scenario = scenario(5, 2, Arrival::Rank, false).together(&[4, 3, 2, 1, 0]);
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let (moves, _) = namespace_move_counts();
    let (restarted, rows) = run_stopping_at(&scenario, stop, At::AnyRename).await;
    assert_restarted(&restarted, &whole);
    assert_eq!(restarted.files, expected(5, 2));
    // The checkpoint the job came back from holds the move the rename asked
    // for, not yet run, only once that rename committed.
    assert_eq!(
        !waiting(&rows).is_empty(),
        matches!(stop, Stop::Committed),
        "{stop:?}"
    );
    assert!(
        namespace_move_counts().0 - moves <= 5 * 2,
        "at most one move a file across the restart"
    );
}

#[tokio::test]
async fn down_mid_job_after_a_rename_committed_moves_each_file_once_at_delivery() {
    down_mid_job(Stop::Committed).await;
}
#[tokio::test]
async fn down_mid_job_after_a_rename_not_committed_moves_each_file_once_at_delivery() {
    down_mid_job(Stop::Uncommitted).await;
}

// Three zips holding one name, the first ranked last: at delivery the third
// zip's file moves off `data0 (1).bin`, then the second's onto it from
// `data0.bin`, then the first's onto `data0.bin` from under its waiting name,
// a commit apart. Down anywhere in that pass, the restore finds each file at
// a name the database gives its member, never at another member's, and the
// run delivers what the run that never went down delivers.
async fn down_in_the_delivery_pass(stop: Stop) -> Vec<(String, String)> {
    let scenario = scenario(3, 1, Arrival::TopLast, false);
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    assert_eq!(whole.files, expected(3, 1));
    let checks = startup_checks();
    let (restarted, rows) = run_stopping_at(&scenario, stop, At::Delivery).await;
    // Every article was in: the restore alone queued the check that went on
    // with the delivery.
    assert_eq!(startup_checks(), checks + 1, "{stop:?}");
    assert_restarted(&restarted, &whole);
    let mut waiting: Vec<(String, String)> = waiting(&rows)
        .into_iter()
        .map(|(sat, wants)| {
            // A waiting name is the writer's own; only where it lies matters.
            let sat = if sat.starts_with(weaver_graph::names::INTERNAL_DIR) {
                "(waiting)".to_owned()
            } else {
                sat.to_owned()
            };
            (sat, wants.to_owned())
        })
        .collect();
    waiting.sort();
    waiting
}

fn pair(sat: &str, wants: &str) -> (String, String) {
    (sat.to_owned(), wants.to_owned())
}

// As the delivery pass began, the database gives each file the name it sits
// at and the name it wants: the chain of three.
fn chain() -> Vec<(String, String)> {
    vec![
        pair("(waiting)", "data0.bin"),
        pair("data0 (1).bin", "data0 (2).bin"),
        pair("data0.bin", "data0 (1).bin"),
    ]
}

// The links of the chain the database still holds waiting: the ones whose
// move it has not recorded.
fn links_left(waiting: &[(String, String)]) -> usize {
    assert!(
        waiting.iter().all(|link| chain().contains(link)),
        "{waiting:?}"
    );
    waiting.len()
}

#[tokio::test]
async fn down_as_the_delivery_pass_begins_before_its_checkpoint_commits() {
    assert_eq!(down_in_the_delivery_pass(Stop::Uncommitted).await, chain());
}

// The first link can move inside the checkpoint the delivery takes, and the
// commit records it.
#[tokio::test]
async fn down_as_the_delivery_pass_begins_after_its_checkpoint_commits() {
    let left = links_left(&down_in_the_delivery_pass(Stop::Committed).await);
    assert!((2..=3).contains(&left), "{left}");
}

// Down with one, two or all three files moved and the write that records
// the last move lost: the restore finds that file gone from where the
// database says it sat and at the name it wants, and adopts it there. The
// database never holds a link whose move ran before the move that freed
// its name was recorded.
#[tokio::test]
async fn down_after_the_first_link_of_the_chain_moved() {
    assert_eq!(down_in_the_delivery_pass(Stop::Moves(1)).await, chain());
}

#[tokio::test]
async fn down_after_the_second_link_of_the_chain_moved() {
    assert_eq!(
        links_left(&down_in_the_delivery_pass(Stop::Moves(2)).await),
        2
    );
}

#[tokio::test]
async fn down_after_the_last_link_of_the_chain_moved() {
    assert_eq!(
        links_left(&down_in_the_delivery_pass(Stop::Moves(3)).await),
        1
    );
}
