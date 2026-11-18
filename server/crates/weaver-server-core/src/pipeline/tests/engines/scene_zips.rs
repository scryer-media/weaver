// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Scene "zip with diz" releases: a RAR set whose volumes are each posted
// inside a zip of their own, beside a `file_id.diz` and an `.nfo`. The zips
// sit at the root, or are themselves the members of an outer RAR set. Every
// byte arrives; the file inside the inner set is delivered whole.

use std::io::{Cursor, Write};

use ::zip::ZipWriter;

use super::harness::{ARTICLE, Outcome, Post, Scenario, payload, run};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::lines::Lines;
use crate::pipeline::tests::direct_store::{multi_member_store_set, single_member_store_set};
use crate::pipeline::tests::single_member_rar4_old_numbering_set;

const FOLDER: &str = "lantern beacon";
const BOOK: &str = "ember.pdf";

fn delivered(name: &str) -> String {
    format!("{FOLDER}/{name}")
}

fn book() -> Vec<u8> {
    payload(9 * ARTICLE + 29, 61)
}

fn diz(letter: char) -> Vec<u8> {
    format!("ember volume {letter}\r\n").into_bytes()
}

fn nfo() -> Vec<u8> {
    b"ember release notes\r\n".to_vec()
}

// A stored zip of `entries`, in order.
fn zip(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, bytes) in entries {
        writer.start_file(*name, super::zip::stored()).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

// Each volume of `volumes` in its own zip, `ddt0623a.zip` onward, beside a
// diz and an nfo, as the release's groups pack them.
fn zipped(volumes: &[(String, Vec<u8>)]) -> Vec<(String, Vec<u8>)> {
    volumes
        .iter()
        .zip('a'..)
        .map(|((name, bytes), letter)| {
            let entries = [
                ("file_id.diz", diz(letter)),
                ("ddt0623.nfo", nfo()),
                (name.as_str(), bytes.clone()),
            ];
            (format!("ddt0623{letter}.zip"), zip(&entries))
        })
        .collect()
}

fn old_style(volumes: usize) -> Vec<(String, Vec<u8>)> {
    single_member_rar4_old_numbering_set("ddt0623", BOOK, &book(), volumes)
}

fn part_style(volumes: usize) -> Vec<(String, Vec<u8>)> {
    single_member_store_set(BOOK, &book(), volumes)
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| (format!("ddt0623.part{}.rar", index + 1), bytes))
        .collect()
}

fn at_root(zips: Vec<(String, Vec<u8>)>) -> Scenario {
    Scenario::new(
        zips.into_iter()
            .map(|(name, bytes)| Post::plain(&name, bytes))
            .collect(),
    )
}

// The zips as the members of an outer stored RAR5 set of `outer` volumes.
fn in_outer_rar(zips: Vec<(String, Vec<u8>)>, outer: usize) -> Scenario {
    let entries: Vec<(&str, Vec<u8>)> = zips
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    Scenario::new(
        multi_member_store_set(&entries, outer)
            .into_iter()
            .enumerate()
            .map(|(index, (_, bytes))| {
                Post::plain(&format!("Duck.Tales.part{:02}.rar", index + 1), bytes)
            })
            .collect(),
    )
}

async fn delivers_the_book(scenario: Scenario, case: &str) -> Outcome {
    let (lines, guard) = Lines::capture();
    let graph = run(&scenario).await;
    drop(guard);
    let refused = lines.of("object refused");
    let bound = lines.of("object bound");
    let names: Vec<&String> = graph.files.keys().collect();
    assert!(
        graph.completed() && graph.files.get(&delivered(BOOK)) == Some(&book()),
        "{case}: {:?} {:?} files {names:?} refused {refused:#?} bound {bound:#?}",
        graph.status,
        graph.failure,
    );
    graph
}

#[tokio::test]
async fn old_style_volumes_each_in_a_zip_at_the_root_deliver_the_book() {
    delivers_the_book(at_root(zipped(&old_style(4))), "old style at the root").await;
}

#[tokio::test]
async fn part_style_volumes_each_in_a_zip_at_the_root_deliver_the_book() {
    delivers_the_book(at_root(zipped(&part_style(3))), "part style at the root").await;
}

#[tokio::test]
async fn old_style_volumes_each_in_a_zip_inside_an_outer_rar_deliver_the_book() {
    delivers_the_book(
        in_outer_rar(zipped(&old_style(5)), 3),
        "old style in an outer rar",
    )
    .await;
}

#[tokio::test]
async fn old_style_volumes_each_in_a_zip_served_backwards_deliver_the_book() {
    delivers_the_book(
        at_root(zipped(&old_style(4))).backwards(),
        "old style at the root, backwards",
    )
    .await;
}

// The RARLAB four-volume RAR4 set of one compressed member, its volumes
// named the old way: `.rar`, then `.r00` onward.
fn compressed_old_style() -> Vec<(String, Vec<u8>)> {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles"
    );
    (1..=4)
        .map(|part| {
            let bytes = std::fs::read(format!("{dir}/rar4_lz_four.part0{part}.rar")).unwrap();
            let name = match part {
                1 => "ddt0623.rar".to_owned(),
                _ => format!("ddt0623.r{:02}", part - 2),
            };
            (name, bytes)
        })
        .collect()
}

const COMPRESSED: &str = "compressible.txt";

async fn delivers_compressed(scenario: Scenario, case: &str) -> Outcome {
    let (lines, guard) = Lines::capture();
    let graph = run(&scenario).await;
    drop(guard);
    let refused = lines.of("object refused");
    let bound = lines.of("object bound");
    let names: Vec<&String> = graph.files.keys().collect();
    assert!(
        graph.completed() && graph.files.contains_key(&delivered(COMPRESSED)),
        "{case}: {:?} {:?} files {names:?} refused {refused:#?} bound {bound:#?}",
        graph.status,
        graph.failure,
    );
    graph
}

#[tokio::test]
async fn compressed_old_style_volumes_each_in_a_zip_at_the_root_deliver_the_member() {
    delivers_compressed(
        at_root(zipped(&compressed_old_style())),
        "compressed at the root",
    )
    .await;
}

#[tokio::test]
async fn compressed_old_style_volumes_each_in_a_zip_inside_an_outer_rar_deliver_the_member() {
    delivers_compressed(
        in_outer_rar(zipped(&compressed_old_style()), 3),
        "compressed in an outer rar",
    )
    .await;
}

// The outer set loses an article and nothing repairs it: the job ends, it
// never sits waiting.
#[tokio::test]
async fn compressed_volumes_in_zips_inside_an_outer_rar_missing_an_article_end() {
    let scenario = in_outer_rar(zipped(&compressed_old_style()), 3).lost(2, 1);
    let (lines, guard) = Lines::capture();
    let graph = run(&scenario).await;
    drop(guard);
    assert!(
        graph.status.is_some(),
        "{:?} {:?} refused {:#?}",
        graph.status,
        graph.failure,
        lines.of("object refused")
    );
}

// The outer set loses articles in each of its volumes, a PAR2 set at the
// root holds too few blocks to rebuild them, and every volume of the inner
// set sits alone in its own zip: the job ends, it never sits waiting.
#[tokio::test]
async fn compressed_volumes_in_zips_inside_an_outer_rar_short_of_recovery_end() {
    for (spoiled, backwards) in [vec![0], vec![0, 1], vec![0, 1, 2]]
        .into_iter()
        .flat_map(|spoiled| [(spoiled.clone(), false), (spoiled, true)])
    {
        let zips = zipped(&compressed_old_style());
        let entries: Vec<(&str, Vec<u8>)> = zips
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.clone()))
            .collect();
        let outer = multi_member_store_set(&entries, 3);
        let mut posts: Vec<Post> = outer
            .iter()
            .enumerate()
            .map(|(index, (_, bytes))| {
                Post::plain(
                    &format!("Duck.Tales.part{:02}.rar", index + 1),
                    bytes.clone(),
                )
            })
            .collect();
        let named: Vec<(String, &[u8])> = outer
            .iter()
            .enumerate()
            .map(|(index, (_, bytes))| {
                (
                    format!("Duck.Tales.part{:02}.rar", index + 1),
                    bytes.as_slice(),
                )
            })
            .collect();
        let files: Vec<(&str, &[u8])> = named.iter().map(|(n, b)| (n.as_str(), *b)).collect();
        posts.extend(super::harness::par2_volumes_sliced(
            "Duck.Tales",
            &files,
            4096,
            1,
            1,
        ));
        let mut scenario = Scenario::new(posts).framed(4096);
        if backwards {
            scenario = scenario.backwards();
        }
        for file in &spoiled {
            scenario = scenario.lost(*file, 3).lost(*file, 40);
        }
        let outcome = super::harness::try_run(&scenario)
            .await
            .unwrap_or_else(|stall| {
                panic!(
                    "{spoiled:?} backwards={backwards}: {:?} {}",
                    stall.status, stall.state
                )
            });
        assert!(
            outcome.status.is_some(),
            "{spoiled:?}: {:?}",
            outcome.failure
        );
    }
}

// The last article of the outer set's first volume is lost: the bytes of
// the first zip it carries, which the zip reads itself until its directory
// says whose they are, and the volume's end block, without which the outer
// store never reads on into the second volume. Nothing repairs it. The job
// ends, it never sits waiting for bytes the store will not place.
#[tokio::test]
async fn an_article_lost_on_bytes_the_outer_store_still_holds_ends_the_job() {
    let scenario = in_outer_rar(zipped(&compressed_old_style()), 10)
        .framed(1024)
        .batched(1)
        .lost(0, 4);
    let outcome = super::harness::try_run(&scenario)
        .await
        .unwrap_or_else(|stall| panic!("{:?} {}", stall.status, stall.state));
    assert!(outcome.status.is_some(), "{:?}", outcome.failure);
    // The outer store gives the set up at quiet: no file of it is
    // delivered whole, so nothing usable came.
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        outcome.status
    );
}

// A file in a zip that is the only member of an outer stored RAR5 set of
// ten volumes, cut into articles of 1 KiB, with a PAR2 set at the root over
// the outer volumes sliced at the article size and holding two blocks, the
// last article of the first volume lost: the zip's bytes there, which it
// reads itself until its directory places them, and the volume's end block.
const PLAIN: &str = "ember.bin";

fn plain() -> Vec<u8> {
    payload(40_000, 7)
}

fn held_loss_repairable() -> Scenario {
    let inner = zip(&[("file_id.diz", diz('a')), (PLAIN, plain())]);
    let outer: Vec<(String, Vec<u8>)> = multi_member_store_set(&[("ddt0623a.zip", inner)], 10)
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| (format!("Duck.Tales.part{:02}.rar", index + 1), bytes))
        .collect();
    let last = (outer[0].1.len() as u32).div_ceil(1024) - 1;
    let files: Vec<(&str, &[u8])> = outer
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = outer
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(super::harness::par2_volumes_sliced(
        "Duck.Tales",
        &files,
        1024,
        2,
        1,
    ));
    Scenario::new(posts).framed(1024).lost(0, last)
}

async fn delivers_plain(scenario: Scenario, case: &str) -> Outcome {
    let graph = run(&scenario).await;
    let names: Vec<&String> = graph.files.keys().collect();
    assert!(
        graph.completed() && graph.files.get(&delivered(PLAIN)) == Some(&plain()),
        "{case}: {:?} {:?} files {names:?}",
        graph.status,
        graph.failure,
    );
    graph
}

// The loss repaired ends Complete with the file delivered whole, whatever
// order the articles arrive in.
#[tokio::test]
async fn an_article_lost_on_bytes_a_zip_still_reads_itself_is_repaired() {
    for (case, scenario) in [
        ("in order", held_loss_repairable()),
        ("one at a time", held_loss_repairable().batched(1)),
        ("backwards", held_loss_repairable().backwards()),
        (
            "second volume first",
            held_loss_repairable().reversed(&[0, 1]),
        ),
    ] {
        delivers_plain(scenario, case).await;
    }
}

// The run restarted between articles, the loss told before a restart,
// ends as the run that never restarted does: Complete, the same file, and
// nothing on disk fetched again.
#[tokio::test]
async fn an_article_lost_on_bytes_a_zip_still_reads_itself_is_repaired_across_a_restart() {
    let whole = delivers_plain(held_loss_repairable().batched(1), "whole").await;
    let restarted = super::restart::run_restarting_at(
        &held_loss_repairable().batched(1),
        &mut |_| {},
        super::restart::Down::Crash,
        &|nth| nth % 6 == 0,
    )
    .await;
    super::restart::assert_restarted(&restarted, &whole);
}

// The shape of the first test with the loss nothing repairs, restarted
// between articles, ends as the run that never restarted does.
#[tokio::test]
async fn an_article_lost_on_bytes_the_outer_store_still_holds_ends_the_job_across_a_restart() {
    let scenario = || {
        in_outer_rar(zipped(&compressed_old_style()), 10)
            .framed(1024)
            .batched(1)
            .lost(0, 4)
    };
    let whole = super::harness::try_run(&scenario())
        .await
        .unwrap_or_else(|stall| panic!("{:?} {}", stall.status, stall.state));
    let restarted = super::restart::run_restarting_at(
        &scenario(),
        &mut |_| {},
        super::restart::Down::Crash,
        &|nth| nth % 6 == 0,
    )
    .await;
    assert!(restarted.restarts > 0, "the run restarted");
    assert_eq!(restarted.status, whole.status, "status");
    assert_eq!(
        restarted.files.keys().collect::<Vec<_>>(),
        whole.files.keys().collect::<Vec<_>>(),
        "delivered names"
    );
}

// Each zip of `zips` inside a zip of its own, `levels` times over, the
// outermost keeping the inner one's name: every volume sits alone in its
// namespace at each level, and is lifted once a level on its way up.
fn rezipped(zips: Vec<(String, Vec<u8>)>, levels: usize) -> Vec<(String, Vec<u8>)> {
    zips.into_iter()
        .map(|(name, mut bytes)| {
            for level in 0..levels {
                bytes = zip(&[(format!("level{level}-{name}").as_str(), bytes)]);
            }
            (name, bytes)
        })
        .collect()
}

// A successful run hands over no volume it lifted: once its set read it,
// the lifted file is gone, and only what the set holds is delivered.
fn no_volume_delivered(outcome: &Outcome, case: &str) {
    let volumes: Vec<&String> = outcome
        .files
        .keys()
        .filter(|name| name.contains(".r0") || name.ends_with(".rar") || name.contains(".bin.0"))
        .collect();
    assert!(
        volumes.is_empty(),
        "{case}: lifted volumes delivered {volumes:?}"
    );
}

// RAR in a zip in a zip: each volume is lifted twice, out of its own zip and
// then out of the zip that held that one, and the set meets at the root.
#[tokio::test]
async fn volumes_lifted_twice_deliver_the_book() {
    for (case, scenario) in [
        ("old style", at_root(rezipped(zipped(&old_style(4)), 1))),
        ("part style", at_root(rezipped(zipped(&part_style(3)), 1))),
        (
            "old style, backwards",
            at_root(rezipped(zipped(&old_style(4)), 1)).backwards(),
        ),
    ] {
        let outcome = delivers_the_book(scenario, case).await;
        no_volume_delivered(&outcome, case);
    }
}

// The compressed set lifted twice, its member decoded across the volumes
// once they meet.
#[tokio::test]
async fn compressed_volumes_lifted_twice_deliver_the_member() {
    delivers_compressed(
        at_root(rezipped(zipped(&compressed_old_style()), 1)),
        "compressed lifted twice",
    )
    .await;
}

// One volume of a set, alone in its zip at the root, the others never
// posted: it is lifted to the root, and there the set fails as a set short
// of its volumes does. The job ends Failed; it never sits waiting.
#[tokio::test]
async fn a_volume_alone_at_the_root_fails_cleanly() {
    for (case, volume) in [
        ("second volume", 1),
        ("first volume", 0),
        ("last volume", 3),
    ] {
        let (name, bytes) = old_style(4).remove(volume);
        let scenario = at_root(vec![(
            "ddt0623a.zip".to_owned(),
            zip(&[(name.as_str(), bytes)]),
        )]);
        let outcome = super::harness::try_run(&scenario)
            .await
            .unwrap_or_else(|stall| panic!("{case}: {:?} {}", stall.status, stall.state));
        assert!(
            matches!(outcome.status, Some(JobStatus::Failed { .. })),
            "{case}: {:?} {:?}",
            outcome.status,
            outcome.failure
        );
    }
}

// The pieces of a byte split, each alone in its own zip: each lone piece is
// lifted to the root, where the pieces meet and are joined.
#[tokio::test]
async fn split_pieces_each_in_a_zip_are_joined() {
    let joined = payload(3 * 6 * ARTICLE - 17, 61);
    let pieces: Vec<(String, Vec<u8>)> = joined
        .chunks(6 * ARTICLE)
        .enumerate()
        .map(|(index, piece)| (format!("{PLAIN}.{:03}", index + 1), piece.to_vec()))
        .collect();
    for (case, scenario) in [
        ("in order", at_root(zipped(&pieces))),
        ("backwards", at_root(zipped(&pieces)).backwards()),
        ("lifted twice", at_root(rezipped(zipped(&pieces), 1))),
    ] {
        let outcome = run(&scenario).await;
        assert!(
            outcome.completed() && outcome.files.get(&delivered(PLAIN)) == Some(&joined),
            "{case}: {:?} {:?}",
            outcome.status,
            outcome.failure,
        );
        no_volume_delivered(&outcome, case);
    }
}

// The job restarted once a volume is lifted and before the set it rose to
// is read, and in another run after every article it was served: each ends
// as the run that never restarted does.
#[tokio::test]
async fn a_restart_mid_lift_ends_as_the_run_that_never_restarted() {
    for (case, scenario) in [
        ("at the root", at_root(zipped(&old_style(4)))),
        ("in an outer rar", in_outer_rar(zipped(&old_style(5)), 3)),
        ("lifted twice", at_root(rezipped(zipped(&old_style(4)), 1))),
        ("compressed", at_root(zipped(&compressed_old_style()))),
        (
            "compressed, lifted twice",
            at_root(rezipped(zipped(&compressed_old_style()), 1)),
        ),
    ] {
        let whole = run(&scenario).await;
        assert!(
            whole.completed(),
            "{case}: {:?} {:?}",
            whole.status,
            whole.failure
        );
        for down in [super::restart::Down::Lifting, super::restart::Down::Crash] {
            let mut lifted = 0;
            let restarted = super::restart::run_restarting_at(
                &scenario,
                &mut |rows| {
                    if rows.members.iter().any(|member| member.lifted.is_some()) {
                        lifted += 1;
                    }
                },
                down,
                &|_| true,
            )
            .await;
            super::restart::assert_restarted(&restarted, &whole);
            if down == super::restart::Down::Lifting {
                assert!(lifted > 0, "{case}: the restart restored lifted volumes");
            }
        }
    }
}

// A PAR2 set at the root over the zips, beside the volumes lifted there:
// it repairs the zip an article of which was damaged, and the set the
// lifted volumes make is read. The lifted volumes are no files of its, and
// it is never offered them.
#[tokio::test]
async fn a_recovery_set_beside_lifted_volumes_repairs_and_the_book_is_delivered() {
    let posts = || {
        let zips = zipped(&old_style(4));
        let described: Vec<(&str, &[u8])> = zips
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let mut posts: Vec<Post> = zips
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect();
        posts.extend(super::harness::par2_set("ddt0623", &described, 4));
        posts
    };
    for (case, scenario) in [
        ("in order", Scenario::new(posts()).damaged(1, 1)),
        (
            "backwards",
            Scenario::new(posts()).damaged(1, 1).backwards(),
        ),
    ] {
        let outcome = run(&scenario).await;
        let names: Vec<&String> = outcome.files.keys().collect();
        assert!(
            outcome.completed() && outcome.files.get(&delivered(BOOK)) == Some(&book()),
            "{case}: {:?} {:?} files {names:?}",
            outcome.status,
            outcome.failure
        );
        assert_eq!(
            outcome.graph().outcomes,
            vec![Some(weaver_graph::recovery::RepairOutcome::Repaired {
                slices: 1
            })],
            "{case}: the set repaired the damaged zip"
        );
        no_volume_delivered(&outcome, case);
    }
}

// Two sets' volumes under one name, each alone in its own zip, the sets'
// other volumes never posted: both are lifted to the root and kept there
// when their sets fail, beside a file whole. The volume from the first zip
// keeps the plain name and the one from the second a numbered one,
// as they ranked where they were posted, though the second was lifted
// first; a restart between the lifts and after them keeps both.
#[tokio::test]
async fn a_lifted_volume_keeps_its_rank_across_a_restart() {
    let mut first = single_member_rar4_old_numbering_set("ddt0623", BOOK, &book(), 4);
    let mut second =
        single_member_rar4_old_numbering_set("ddt0623", "quill.pdf", &payload(9 * ARTICLE, 7), 4);
    let (name, ours) = first.remove(1);
    let (_, theirs) = second.remove(1);
    let scenario = Scenario::new(vec![
        Post::plain("ddt0623a.zip", zip(&[(name.as_str(), ours.clone())])),
        Post::plain("ddt0623b.zip", zip(&[(name.as_str(), theirs.clone())])),
        Post::plain("lantern.pdf", book()),
    ]);
    let (lines, guard) = Lines::capture();
    let whole = run(&scenario).await;
    drop(guard);
    let names: Vec<&String> = whole.files.keys().collect();
    assert!(
        matches!(whole.status, Some(JobStatus::Partial)),
        "{:?} {:?} refused {:#?}",
        whole.status,
        whole.failure,
        lines.of("object refused")
    );
    assert_eq!(
        whole.files.get(&delivered(&name)),
        Some(&ours),
        "the first zip's volume keeps the plain name: {names:?}"
    );
    let numbered: Vec<&String> = whole
        .files
        .iter()
        .filter(|(_, bytes)| **bytes == theirs)
        .map(|(name, _)| name)
        .collect();
    assert!(
        numbered.len() == 1 && *numbered[0] != delivered(&name),
        "the second zip's volume takes a numbered name: {names:?}"
    );
    let mut lifted = 0;
    let restarted = super::restart::run_restarting_at(
        &scenario,
        &mut |rows| {
            if rows.members.iter().any(|member| member.lifted.is_some()) {
                lifted += 1;
            }
        },
        super::restart::Down::Lifting,
        &|_| true,
    )
    .await;
    super::restart::assert_restarted(&restarted, &whole);
    assert!(lifted > 0, "the restart restored lifted volumes");
}

// The compressed set each volume of which is alone in its own zip, the
// zips the members of an outer stored set of ten volumes cut into articles
// of 1 KiB, a PAR2 set at the root over the outer volumes holding two
// blocks, the last article of the first outer volume lost. The repair
// rebuilds it, the volumes are lifted, and the member is decoded whole.
fn compressed_held_loss_repairable() -> Scenario {
    let zips = zipped(&compressed_old_style());
    let entries: Vec<(&str, Vec<u8>)> = zips
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let outer: Vec<(String, Vec<u8>)> = multi_member_store_set(&entries, 10)
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| (format!("Duck.Tales.part{:02}.rar", index + 1), bytes))
        .collect();
    let last = (outer[0].1.len() as u32).div_ceil(1024) - 1;
    let files: Vec<(&str, &[u8])> = outer
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = outer
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(super::harness::par2_volumes_sliced(
        "Duck.Tales",
        &files,
        1024,
        2,
        1,
    ));
    Scenario::new(posts).framed(1024).lost(0, last)
}

#[tokio::test]
async fn compressed_volumes_in_zips_with_a_repaired_loss_end_complete_in_every_order() {
    for (case, scenario) in [
        ("in order", compressed_held_loss_repairable()),
        (
            "one at a time",
            compressed_held_loss_repairable().batched(1),
        ),
        ("backwards", compressed_held_loss_repairable().backwards()),
        (
            "second volume first",
            compressed_held_loss_repairable().reversed(&[0, 1]),
        ),
    ] {
        let outcome = delivers_compressed(scenario, case).await;
        assert!(
            matches!(outcome.status, Some(JobStatus::Complete)),
            "{case}: {:?}",
            outcome.status
        );
    }
}

#[tokio::test]
async fn compressed_volumes_in_zips_with_a_repaired_loss_end_complete_across_a_restart() {
    let whole = delivers_compressed(compressed_held_loss_repairable().batched(1), "whole").await;
    for (down, every) in [
        (super::restart::Down::Crash, 6),
        (super::restart::Down::Crash, 1),
        (super::restart::Down::Lifting, 1),
    ] {
        let restarted = super::restart::run_restarting_at(
            &compressed_held_loss_repairable().batched(1),
            &mut |_| {},
            down,
            &|nth| nth % every == 0,
        )
        .await;
        super::restart::assert_restarted(&restarted, &whole);
    }
}

// The set's member a zip of its own, the volumes `levels` zips deep: the
// zip inside the set is as deep as the set's volumes were posted, wherever
// they were lifted to, and the nesting limit counts it there. The same
// set, its volumes side by side in one zip as deep, is read alike.
fn deep_sets(levels: usize) -> (Scenario, Scenario) {
    let inner = zip(&[(BOOK, book())]);
    let volumes = single_member_rar4_old_numbering_set("ddt0623", "ember.zip", &inner, 4);
    let scattered = at_root(rezipped(zipped(&volumes), levels));
    let entries: Vec<(&str, Vec<u8>)> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let together = at_root(rezipped(
        vec![("ddt0623a.zip".to_owned(), zip(&entries))],
        levels,
    ));
    (scattered, together)
}

#[tokio::test]
async fn the_nesting_limit_counts_where_a_lifted_volume_came_from() {
    let depth = crate::pipeline::graph::limits::NESTING as usize;
    let mut unpacked = Vec::new();
    for levels in [depth - 3, depth - 2] {
        let (scattered, together) = deep_sets(levels);
        let alone = run(&scattered).await;
        let side_by_side = run(&together).await;
        let book_out = |outcome: &Outcome| outcome.files.get(&delivered(BOOK)) == Some(&book());
        assert_eq!(
            book_out(&alone),
            book_out(&side_by_side),
            "{levels} levels: the scattered set nests as the one side by side: {:?} {:?}",
            alone.files.keys().collect::<Vec<_>>(),
            side_by_side.files.keys().collect::<Vec<_>>(),
        );
        assert_eq!(alone.status, side_by_side.status, "{levels} levels: status");
        unpacked.push(book_out(&alone));
    }
    // The limit falls between the two depths, so both sides of it are read.
    assert_eq!(unpacked, vec![true, false], "the limit falls between them");
}
