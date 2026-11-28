// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An SFV packed inside an archive, beside the files it lists, under a PAR2
// set that proved the archive's volumes: every file it lists came out of
// that proved archive, so its bytes are what was posted, and the SFV makes
// no claim of damage, whatever CRCs it states. Elsewhere it claims none
// either: a job with it ends as the same posts without it.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use super::sfv::{assert_twin, crc, listing_text};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::multi_member_store_set;

const FOLDER: &str = "lantern beacon";
const MOVIE: &str = "quill.mkv";
const SFV: &str = "quill.sfv";

fn movie() -> Vec<u8> {
    payload(9 * ARTICLE + 29, 73)
}

// An SFV over the movie stating a CRC its bytes do not have.
fn wrong_sfv() -> Vec<u8> {
    listing_text("; quill\n", &[(MOVIE, crc(&movie()) ^ 0x00ff_00ff)])
}

// The movie and `sfv` as the members of an outer stored RAR set of two
// volumes, under a PAR2 set over those volumes when `recovery` says so.
fn in_outer_rar(sfv: Vec<u8>, recovery: bool) -> Scenario {
    in_outer_rar_of(vec![(MOVIE, movie()), (SFV, sfv)], recovery)
}

// `entries` as the members of an outer stored RAR set of two volumes,
// under a PAR2 set over those volumes when `recovery` says so.
fn in_outer_rar_of(entries: Vec<(&str, Vec<u8>)>, recovery: bool) -> Scenario {
    let outer: Vec<(String, Vec<u8>)> = multi_member_store_set(&entries, 2)
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| (format!("Duck.Tales.part{:02}.rar", index + 1), bytes))
        .collect();
    let described: Vec<(&str, &[u8])> = outer
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = outer
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    if recovery {
        posts.extend(par2_set("Duck.Tales", &described, 2));
    }
    Scenario::new(posts)
}

fn said(outcome: &Outcome) -> String {
    let delivery = &outcome.graph().delivery;
    format!(
        "{:?} {:?}\nrulings {:?}\nwhole {:?}\ndamaged {:?}\nunrepaired {:?}\nfiles {:?}",
        outcome.status,
        outcome.failure,
        outcome.graph().rulings,
        delivery.whole,
        delivery.damaged,
        delivery.unrepaired,
        outcome
            .files
            .keys()
            .chain(outcome.retained_files.keys())
            .collect::<Vec<_>>()
    )
}

// The SFV inside the proved archive states a CRC the movie does not have:
// it makes no claim, and the job ends Complete with the movie delivered as
// it came out.
#[tokio::test]
async fn an_sfv_over_files_of_a_proved_archive_makes_no_damage_claim() {
    for (case, scenario) in [
        ("in order", in_outer_rar(wrong_sfv(), true)),
        ("backwards", in_outer_rar(wrong_sfv(), true).backwards()),
    ] {
        let outcome = run(&scenario).await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{case}: {}",
            said(&outcome)
        );
        assert_eq!(
            outcome.files.get(&format!("{FOLDER}/{MOVIE}")),
            Some(&movie()),
            "{case}: {}",
            said(&outcome)
        );
    }
}

// The same archive with no recovery set over it. Nothing proved its bytes,
// and the SFV still claims no damage: the job ends as the same archive
// without the SFV.
#[tokio::test]
async fn an_sfv_inside_an_archive_nothing_proved_ends_as_without_it() {
    let outcome = run(&in_outer_rar(wrong_sfv(), false)).await;
    let twin = run(&in_outer_rar_of(vec![(MOVIE, movie())], false)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
    assert_twin(&outcome, &twin);
}

// Guard: an SFV posted at the root beside the movie, stating the wrong
// CRC, under a PAR2 set over both: the root keeps its rules, under which
// the PAR2 set that verified the movie clean decides, and the job ends
// Complete as it did.
#[tokio::test]
async fn an_sfv_at_the_root_beside_a_clean_par2_set_ends_as_it_did() {
    let sfv = wrong_sfv();
    let mut posts = vec![Post::plain(MOVIE, movie()), Post::plain(SFV, sfv.clone())];
    posts.extend(par2_set(
        "quill",
        &[(MOVIE, movie().as_slice()), (SFV, sfv.as_slice())],
        2,
    ));
    let outcome = run(&Scenario::new(posts)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
    assert_eq!(
        outcome.graph().rulings,
        vec![(
            "par2".to_owned(),
            Some(weaver_graph::recovery::RepairOutcome::Clean)
        )]
    );
}

// Guard: the SFV inside the proved archive states the right CRC: Complete,
// as it was.
#[tokio::test]
async fn a_right_sfv_over_files_of_a_proved_archive_completes() {
    let sfv = listing_text("; quill\n", &[(MOVIE, crc(&movie()))]);
    let outcome = run(&in_outer_rar(sfv, true)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
}

// A real RARLAB archive of three files, two stored and one compressed.
const RAR_OF_THREE: &str = "rar5_multifile_lz.rar";
// Where the stored member `hello.txt` begins its data in it.
const HELLO_DATA: usize = 0x45;

// `entries` in one LZMA2 7z: the store gives it up and the engine reads it.
fn in_lzma2_7z(entries: &[(String, Vec<u8>)]) -> Vec<u8> {
    use sevenz_turbo::encoder_options::Lzma2Options;
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter};
    use std::io::Cursor;

    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![Lzma2Options::from_level(1).into()]);
    for (name, bytes) in entries {
        let mut entry = ArchiveEntry::new_file(name);
        entry.size = bytes.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(bytes.clone())))
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

// Two folders, each a RAR the RAR engine reads and an SFV over it; the
// first folder's SFV states a CRC its RAR volume does not have. With
// `spoil_member`, a byte of the first RAR's stored member `hello.txt` is
// flipped, so that member fails its own CRC as it extracts. No PAR2 set.
// No real post is named: it stands for a game release posted as one 7z of
// per-disc folders, each a RAR with its SFV, or without it when `listed`
// says not.
fn folders(spoil_member: bool) -> Vec<(String, Vec<u8>)> {
    folders_listed(spoil_member, true)
}

fn folders_listed(spoil_member: bool, listed_sfv: bool) -> Vec<(String, Vec<u8>)> {
    let mut entries = Vec::new();
    for folder in 0..2u8 {
        let name = format!("game{folder}.rar");
        let mut volume = weaver_server_pipeline::pipeline::tests::rar5_fixture_bytes(RAR_OF_THREE);
        assert_eq!(&volume[HELLO_DATA..HELLO_DATA + 5], b"Hello");
        let listed = crc(&volume) ^ u32::from(folder == 0);
        if spoil_member && folder == 0 {
            volume[HELLO_DATA] ^= 0x20;
        }
        entries.push((format!("Game {folder}/{name}"), volume));
        if !listed_sfv {
            continue;
        }
        let sfv = listing_text("; game\n", &[(&name, listed)]);
        entries.push((format!("Game {folder}/game{folder}.sfv"), sfv));
    }
    entries
}

// The SFV below the root disagrees with a RAR volume whose members all
// extract with their own CRCs passing: it makes no damage claim, nothing is
// short, and the job ends Complete.
#[tokio::test]
async fn a_listing_inside_an_archive_that_disagrees_with_a_volume_extracted_whole_does_not_shorten_the_job()
 {
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.7z",
        in_lzma2_7z(&folders(false)),
    )]);
    let outcome = run(&scenario).await;
    let view = outcome.graph();
    assert_eq!(view.delivery.whole.len(), 6, "{}", said(&outcome));
    assert!(view.delivery.damaged.is_empty(), "{}", said(&outcome));
    assert!(view.delivery.unrepaired.is_empty(), "{}", said(&outcome));
    assert!(view.delivery.withheld.is_empty(), "{}", said(&outcome));
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
}

// The same shape, the first RAR's `hello.txt` failing its own CRC. The
// volume did not extract whole: the job is not Complete, for the member's
// own CRC, and ends as the same archive without the SFVs.
#[tokio::test]
async fn a_listing_inside_an_archive_over_a_volume_whose_member_fails_its_crc_ends_as_without_it() {
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", in_lzma2_7z(&folders(true)))]);
    let outcome = run(&scenario).await;
    assert_ne!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
    let twin = Scenario::new(vec![Post::plain(
        "lantern.7z",
        in_lzma2_7z(&folders_listed(true, false)),
    )]);
    assert_twin(&outcome, &run(&twin).await);
}
