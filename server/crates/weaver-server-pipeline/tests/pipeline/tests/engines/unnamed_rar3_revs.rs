// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR3 recovery volume posted under a name that says nothing. Its front is
// parity and signs nothing, so only its width, the width of a RAR 1.5 to
// 4.x set's volumes, makes it one to try, and only a volume it rebuilds that
// proves whole by the CRC32 its end-of-archive header states proves it the
// set's. Until then, and when nothing proves it, it is the plain file it
// was posted as: delivered whole, as any member that says nothing is.
// A `.rev` whose footer gives counts no RAR3 recovery volume has is refuted
// by that footer, and is a plain file too.

use super::harness::{ARTICLE, Outcome, Post, Scenario, payload, run};
use super::rar::posts;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::single_member_rar4_old_numbering_set_ending;
use weaver_graph::recovery::RepairOutcome;
use weaver_server_pipeline::pipeline::tests::engines::archive_scenario_fixtures::rar_rev::{
    crc, rar3_rev,
};

const MEMBER: &str = "beacon.bin";
const OTHER: &str = "harbor.bin";
const UNNAMED: &str = "e81b6d3f0a4c9705";

type Volumes = Vec<(String, Vec<u8>)>;

// Four volumes of a stored member, every end record stating the CRC32 of
// the bytes before it and reserving the footer's seven bytes, as RAR 3.x
// writes a set it makes recovery volumes for.
fn set(base: &str, member: &str, seed: u8) -> Volumes {
    let body = payload(9 * ARTICLE + 17, seed);
    single_member_rar4_old_numbering_set_ending(base, member, &body, 4, None)
}

fn body(seed: u8) -> Vec<u8> {
    payload(9 * ARTICLE + 17, seed)
}

// The same set, its end records stating no CRC32: the flag cleared and each
// record's CRC16 made again over what is left.
fn without_data_crc(volumes: Volumes) -> Volumes {
    const END: usize = 7 + 4 + 7;
    volumes
        .into_iter()
        .map(|(name, mut bytes)| {
            let at = bytes.len() - END;
            assert_eq!(bytes[at + 2], 0x7b, "{name} ends on its end record");
            let flags = u16::from_le_bytes([bytes[at + 3], bytes[at + 4]]) & !0x0002;
            bytes[at + 3..at + 5].copy_from_slice(&flags.to_le_bytes());
            let check = (crc(&bytes[at + 2..]) & 0xffff) as u16;
            bytes[at..at + 2].copy_from_slice(&check.to_le_bytes());
            (name, bytes)
        })
        .collect()
}

fn rev_of(volumes: &Volumes) -> Vec<u8> {
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    rar3_rev(&bytes)
}

fn width(volumes: &Volumes) -> usize {
    volumes[0].1.len()
}

// The volumes but the one at `absent`, the unnamed file before them or after.
fn post(volumes: &Volumes, absent: Option<usize>, unnamed: Vec<u8>, first: bool) -> Vec<Post> {
    let kept: Volumes = volumes
        .iter()
        .enumerate()
        .filter(|(at, _)| Some(*at) != absent)
        .map(|(_, volume)| volume.clone())
        .collect();
    let mut out = posts(kept);
    let file = Post::plain(UNNAMED, unnamed);
    match first {
        true => out.insert(0, file),
        false => out.push(file),
    }
    out
}

fn delivered<'g>(graph: &'g Outcome, name: &str) -> Option<&'g Vec<u8>> {
    graph
        .files
        .iter()
        .find(|(path, _)| path.ends_with(&format!("/{name}")))
        .map(|(_, bytes)| bytes)
}

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} files {:?} rulings {:?}",
        graph.status,
        graph.failure,
        graph.files.keys().collect::<Vec<_>>(),
        graph.graph().rulings,
    )
}

fn repaired(graph: &Outcome) -> bool {
    graph.graph().rulings.iter().any(|(engine, result)| {
        engine == "rar_recovery" && matches!(result, Some(RepairOutcome::Repaired { .. }))
    })
}

// What the job made of the post with the unnamed file one byte wider, as
// wide as no volume, so nothing tries it: the job before any of this.
async fn control(volumes: &Volumes, absent: Option<usize>, mut unnamed: Vec<u8>) -> Outcome {
    unnamed.push(0x5a);
    run(&Scenario::new(post(volumes, absent, unnamed, false))).await
}

// Tried as a recovery volume, the control's file never, and still the job
// ends as the control did: the unnamed file delivered whole wherever the
// control's was, what the set delivered the same, and no repair made of it.
async fn assert_as_before(volumes: &Volumes, absent: Option<usize>, unnamed: Vec<u8>) {
    let before = control(volumes, absent, unnamed.clone()).await;
    assert!(
        !before.graph().kinds.contains("rar_recovery"),
        "{}",
        summary(&before)
    );
    for first in [false, true] {
        let graph = run(&Scenario::new(post(
            volumes,
            absent,
            unnamed.clone(),
            first,
        )))
        .await;
        let case = format!(
            "first {first}: {} | control {}",
            summary(&graph),
            summary(&before)
        );
        assert!(graph.graph().kinds.contains("rar_recovery"), "{case}");
        assert_eq!(graph.status, before.status, "{case}");
        assert!(!repaired(&graph), "{case}");
        assert_eq!(graph.failure, before.failure, "{case}");
        // Delivered whole where the control's file was delivered.
        let expected = delivered(&before, UNNAMED).map(|_| &unnamed);
        assert_eq!(delivered(&graph, UNNAMED), expected, "{case}");
        let set = |graph: &Outcome| {
            graph
                .files
                .iter()
                .filter(|(path, _)| !path.ends_with(&format!("/{UNNAMED}")))
                .map(|(path, bytes)| (path.clone(), bytes.clone()))
                .collect::<Vec<_>>()
        };
        assert!(set(&graph) == set(&before), "{case}");
    }
}

#[tokio::test]
async fn an_unnamed_rar3_rev_rebuilds_an_absent_rar4_volume_whose_end_record_proves_it() {
    let volumes = set("silver.horizon", MEMBER, 73);
    let rev = rev_of(&volumes);
    assert_eq!(rev.len(), width(&volumes));
    for first in [false, true] {
        let graph = run(&Scenario::new(post(&volumes, Some(2), rev.clone(), first))).await;
        let case = format!("first {first}: {}", summary(&graph));
        assert_eq!(graph.status, Some(JobStatus::Complete), "{case}");
        assert!(repaired(&graph), "{case}");
        assert_eq!(graph.files.len(), 1, "{case}");
        assert_eq!(delivered(&graph, MEMBER), Some(&body(73)), "{case}");
        assert_eq!(delivered(&graph, UNNAMED), None, "{case}");
    }
}

// End records that state no CRC32 prove no rebuilt volume: nothing is
// written, and the job ends as it did with the file a plain one.
#[tokio::test]
async fn an_unnamed_rar3_rev_of_a_set_whose_end_records_state_no_crc_stays_unused() {
    let volumes = without_data_crc(set("silver.horizon", MEMBER, 73));
    let rev = rev_of(&volumes);
    assert_eq!(rev.len(), width(&volumes));
    assert_as_before(&volumes, Some(2), rev).await;
}

// The volumes of `owner` but its third, the third of the other, `whole`
// or short of its third too, and the unnamed recovery volume `rev`.
fn two_sets(owner: &Volumes, other: &Volumes, whole: bool, rev: Vec<u8>, first: bool) -> Vec<Post> {
    let mut all = post(owner, Some(2), rev, first);
    let kept: Volumes = other
        .iter()
        .enumerate()
        .filter(|(at, _)| whole || *at != 2)
        .map(|(_, volume)| volume.clone())
        .collect();
    all.extend(posts(kept));
    all
}

// Two sets as wide: the one the rebuilt volume proves of takes it, whichever
// set it is and whichever comes first. Beside a whole set the job completes;
// beside one short too, that one alone is left short.
#[tokio::test]
async fn an_unnamed_rar3_rev_goes_to_the_one_of_two_sets_as_wide_that_it_proves() {
    let amber = (set("amber.coast", OTHER, 41), OTHER, 41, "amber.coast");
    let silver = (
        set("silver.horizon", MEMBER, 73),
        MEMBER,
        73,
        "silver.horizon",
    );
    assert_eq!(width(&amber.0), width(&silver.0));
    for (owner, other) in [(&silver, &amber), (&amber, &silver)] {
        let rev = rev_of(&owner.0);
        for first in [false, true] {
            let graph = run(&Scenario::new(two_sets(
                &owner.0,
                &other.0,
                true,
                rev.clone(),
                first,
            )))
            .await;
            let case = format!("{} whole, first {first}: {}", other.3, summary(&graph));
            assert_eq!(graph.status, Some(JobStatus::Complete), "{case}");
            assert!(repaired(&graph), "{case}");
            assert_eq!(delivered(&graph, owner.1), Some(&body(owner.2)), "{case}");
            assert_eq!(delivered(&graph, other.1), Some(&body(other.2)), "{case}");
            assert_eq!(delivered(&graph, UNNAMED), None, "{case}");

            let graph = run(&Scenario::new(two_sets(
                &owner.0,
                &other.0,
                false,
                rev.clone(),
                first,
            )))
            .await;
            let case = format!("{} short, first {first}: {}", other.3, summary(&graph));
            assert!(repaired(&graph), "{case}");
            let failure = graph.failure.clone().unwrap_or_default();
            assert!(failure.contains(other.3), "{case}");
            assert!(!failure.contains(owner.3), "{case}");
        }
    }
}

// Two sets as wide, each short of its third volume, the recovery volume a
// third set's: neither proves it, and the job ends as it did with it a
// plain file.
#[tokio::test]
async fn an_unnamed_rar3_rev_neither_of_two_sets_as_wide_proves_stays_unused() {
    let amber = set("amber.coast", OTHER, 41);
    let silver = set("silver.horizon", MEMBER, 73);
    let rev = rev_of(&set("copper.ridge", MEMBER, 59));
    assert_eq!(rev.len(), width(&silver));
    let mut wider = rev.clone();
    wider.push(0x5a);
    let before = run(&Scenario::new(two_sets(
        &silver, &amber, false, wider, false,
    )))
    .await;
    for first in [false, true] {
        let graph = run(&Scenario::new(two_sets(
            &silver,
            &amber,
            false,
            rev.clone(),
            first,
        )))
        .await;
        let case = format!(
            "first {first}: {} | control {}",
            summary(&graph),
            summary(&before)
        );
        assert!(graph.graph().kinds.contains("rar_recovery"), "{case}");
        assert_eq!(graph.status, before.status, "{case}");
        assert_eq!(graph.failure, before.failure, "{case}");
        assert!(!repaired(&graph), "{case}");
        let expected = delivered(&before, UNNAMED).map(|_| &rev);
        assert_eq!(delivered(&graph, UNNAMED), expected, "{case}");
    }
}

// Bytes of no format, as wide as the set's volumes: beside a whole set it
// is never needed, and beside one short of a volume no rebuild from it
// proves, its footer's counts valid and its checksum not. Delivered whole
// either way, as before.
#[tokio::test]
async fn an_unnamed_file_as_wide_as_a_rar4_sets_volumes_is_delivered_as_before() {
    let volumes = set("silver.horizon", MEMBER, 73);
    let mut random = payload(width(&volumes), 17);
    let footer = random.len() - 7;
    random[footer..footer + 3].copy_from_slice(&[3, 0, 0]);
    assert_as_before(&volumes, None, random.clone()).await;
    assert_as_before(&volumes, Some(2), random).await;
}

// Text under RAR's own `.rev` whose last seven bytes give counts no RAR3
// recovery volume has: a plain file, delivered whole and counted as
// payload.
#[tokio::test]
async fn a_rev_of_text_whose_footer_counts_cannot_be_valid_is_delivered_whole() {
    let movie = payload(4 * ARTICLE + 17, 91);
    let mut text: Vec<u8> = b"Release notes, line after line of plain text.\n"
        .iter()
        .copied()
        .cycle()
        .take(3 * ARTICLE)
        .collect();
    text.extend_from_slice(b"see you by the bay!\n");
    // `e`, then ` `, then `b`: a recovery volume placed past every one
    // of its set's.
    assert!(text[text.len() - 5] > text[text.len() - 6]);
    let posts = vec![
        Post::plain("lantern.mkv", movie.clone()),
        Post::plain("notes.rev", text.clone()),
    ];
    let graph = run(&Scenario::new(posts)).await;
    let case = format!(
        "{} graph {:?} report {:?}",
        summary(&graph),
        graph.graph(),
        graph.report
    );
    assert_eq!(graph.status, Some(JobStatus::Complete), "{case}");
    let delivery = &graph.graph().delivery;
    assert!(
        delivery.whole.iter().any(|whole| whole == "notes.rev"),
        "{case} whole {:?}",
        delivery.whole
    );
    assert_eq!(delivered(&graph, "notes.rev"), Some(&text), "{case}");
    assert_eq!(delivered(&graph, "lantern.mkv"), Some(&movie), "{case}");
    assert_eq!(
        graph.report.as_ref().map(|report| report.health),
        Some(1000),
        "{case}"
    );
}
