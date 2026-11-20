// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An archive set below the root that the poster never shipped whole, under
// a parent a recovery set proved clean: a stray volume without its set's
// first volume, or a set holding its first volume and lacking another. The
// recovery set proved every byte the parent unpacked is what was posted, so
// what the set lacks was never posted. The job ends Complete when any file
// that is no archive volume was delivered anywhere in it, and Failed when
// nothing else was; never Partial. The volumes stay where they landed.
// A set whose own bytes were lost on the way, a nested set that is all there
// and fails to unpack, and the same sets at the root keep their rules.

use std::io::{Cursor, Write};

use ::zip::ZipWriter;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::multi_member_store_set;
use crate::pipeline::tests::single_member_rar4_old_numbering_set;

const FOLDER: &str = "lantern beacon";
const MOVIE: &str = "quill.mkv";
const NFO: &str = "quill.nfo";

fn movie() -> Vec<u8> {
    payload(9 * ARTICLE + 29, 71)
}

fn nfo() -> Vec<u8> {
    b"quill release notes\r\n".to_vec()
}

// The old-numbering set `quill.rar`, `quill.r00` to `quill.r04` holding the
// movie, without the volumes `without` names by index.
fn inner(without: &[usize]) -> Vec<(String, Vec<u8>)> {
    single_member_rar4_old_numbering_set("quill", MOVIE, &movie(), 6)
        .into_iter()
        .enumerate()
        .filter(|(index, _)| !without.contains(index))
        .map(|(_, volume)| volume)
        .collect()
}

fn stored_zip(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, bytes) in entries {
        writer.start_file(*name, super::zip::stored()).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

// `entries` as the members of an outer stored RAR set of two volumes, with
// a PAR2 set over those volumes when `recovery` says so.
fn in_outer_rar(entries: &[(String, Vec<u8>)], recovery: bool) -> Scenario {
    let entries: Vec<(&str, Vec<u8>)> = entries
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
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

// `zips` posted at the root, under a PAR2 set over them.
fn zips_at_the_root(zips: &[(String, Vec<u8>)]) -> Scenario {
    let described: Vec<(&str, &[u8])> = zips
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = zips
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_set("Duck.Tales", &described, 2));
    Scenario::new(posts)
}

fn with_nfo(mut entries: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
    entries.push((NFO.to_owned(), nfo()));
    entries
}

// Every file the job left, delivered or kept in its working directory.
fn left(outcome: &Outcome) -> Vec<&String> {
    outcome
        .files
        .keys()
        .chain(outcome.retained_files.keys())
        .collect()
}

fn said(outcome: &Outcome) -> String {
    let delivery = &outcome.graph().delivery;
    format!(
        "{:?} {:?}\nwhole {:?}\ndamaged {:?}\nunchecked {:?}\nwithheld {:?}\nfiles {:?}",
        outcome.status,
        outcome.failure,
        delivery.whole,
        delivery.damaged,
        delivery.unchecked,
        delivery.withheld,
        left(outcome)
    )
}

fn failed(outcome: &Outcome) -> bool {
    matches!(outcome.status, Some(JobStatus::Failed { .. }))
}

// The volumes the set kept stay where they landed, under the names they
// were posted under.
fn volumes_kept(outcome: &Outcome, volumes: &[(String, Vec<u8>)]) {
    for (name, _) in volumes {
        assert!(
            left(outcome)
                .iter()
                .any(|file| file.ends_with(&format!("/{name}")) || *file == name),
            "{name} is not kept: {}",
            said(outcome)
        );
    }
}

// A set holding its first volume and lacking `quill.r02`, the post's only
// other file an `.nfo` beside it in the outer RAR the root set proved: the
// set never had that volume to give, and the job ends Complete with the
// `.nfo` delivered and the volumes it has kept as they stand.
#[tokio::test]
async fn a_nested_set_short_of_a_volume_never_posted_beside_another_file_ends_complete() {
    let volumes = inner(&[3]);
    let outcome = run(&in_outer_rar(&with_nfo(volumes.clone()), true)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
    assert_eq!(
        outcome.files.get(&format!("{FOLDER}/{NFO}")),
        Some(&nfo()),
        "{}",
        said(&outcome)
    );
    volumes_kept(&outcome, &volumes);
}

// The same set with nothing beside it: no file was delivered, and the job
// ends Failed.
#[tokio::test]
async fn a_nested_set_short_of_a_volume_never_posted_alone_fails() {
    let outcome = run(&in_outer_rar(&inner(&[3]), true)).await;
    assert!(failed(&outcome), "{}", said(&outcome));
}

// A stray volume of a set whose first volume the release never carried,
// beside an `.nfo` in the proved outer RAR: Complete.
#[tokio::test]
async fn a_nested_stray_volume_beside_another_file_ends_complete() {
    let volumes = inner(&[0, 1, 3, 4, 5]);
    let outcome = run(&in_outer_rar(&with_nfo(volumes.clone()), true)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
    volumes_kept(&outcome, &volumes);
}

// The stray volume alone: Failed.
#[tokio::test]
async fn a_nested_stray_volume_alone_fails() {
    let outcome = run(&in_outer_rar(&inner(&[0, 1, 3, 4, 5]), true)).await;
    assert!(failed(&outcome), "{}", said(&outcome));
}

// Scene zips at the root under a PAR2 set over the zips: the set inside
// them lacks `quill.r02`, whose zip was never posted. Each volume is lifted
// out of its zip to the root, where the set meets short of that volume. The
// zips' `.nfo` files were delivered, and the job ends Complete.
#[tokio::test]
async fn volumes_lifted_out_of_proved_zips_short_of_one_never_posted_end_complete() {
    let zips: Vec<(String, Vec<u8>)> = inner(&[3])
        .into_iter()
        .zip('a'..)
        .map(|((name, bytes), letter)| {
            (
                format!("quill{letter}.zip"),
                stored_zip(&[(NFO, nfo()), (name.as_str(), bytes)]),
            )
        })
        .collect();
    let outcome = run(&zips_at_the_root(&zips)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
}

// A stray volume alone in its zip at the root, under a PAR2 set over the
// zip, beside the zip's `.nfo`: lifted to the root, it is the stray volume
// of a set below the root the set proved, and the job ends Complete.
#[tokio::test]
async fn a_stray_volume_lifted_out_of_a_proved_zip_beside_its_nfo_ends_complete() {
    let (name, bytes) = inner(&[0, 1, 3, 4, 5]).remove(0);
    let zips = vec![(
        "quillc.zip".to_owned(),
        stored_zip(&[(NFO, nfo()), (name.as_str(), bytes)]),
    )];
    let outcome = run(&zips_at_the_root(&zips)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
}

// The same zip with nothing beside the volume: Failed.
#[tokio::test]
async fn a_stray_volume_lifted_out_of_a_proved_zip_alone_fails() {
    let (name, bytes) = inner(&[0, 1, 3, 4, 5]).remove(0);
    let zips = vec![(
        "quillc.zip".to_owned(),
        stored_zip(&[(name.as_str(), bytes)]),
    )];
    let outcome = run(&zips_at_the_root(&zips)).await;
    assert!(failed(&outcome), "{}", said(&outcome));
}

// Guard: with no recovery set over the outer RAR nothing proved its bytes,
// and the set short of a volume is short as it was.
#[tokio::test]
async fn a_nested_set_short_of_a_volume_nothing_proved_is_not_complete() {
    let outcome = run(&in_outer_rar(&with_nfo(inner(&[3])), false)).await;
    assert_ne!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
}

// Guard: the same set at the root, beside its `.nfo`, keeps the root's
// rules.
#[tokio::test]
async fn a_set_at_the_root_short_of_a_volume_is_not_complete() {
    let mut posts: Vec<Post> = with_nfo(inner(&[3]))
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    let described: Vec<(String, Vec<u8>)> = inner(&[3]);
    let described: Vec<(&str, &[u8])> = described
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    posts.extend(par2_set("quill", &described, 2));
    let outcome = run(&Scenario::new(posts)).await;
    assert_ne!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
}

// Guard: a nested set that is all there and fails to unpack, enciphered
// with a password the job was never given, under the proved outer RAR and
// beside an `.nfo`: its volumes are withheld as they were, and the job is
// not Complete.
#[tokio::test]
async fn a_nested_set_all_there_that_fails_to_unpack_is_not_complete() {
    let volumes = crate::pipeline::tests::direct_store::scenarios::encrypted_store_set(
        MOVIE,
        &movie(),
        3,
        "lantern",
        Some("lantern"),
        false,
    );
    let outcome = run(&in_outer_rar(&with_nfo(volumes), true)).await;
    assert!(failed(&outcome), "{}", said(&outcome));
    assert!(
        outcome
            .graph()
            .delivery
            .withheld
            .iter()
            .all(|(_, why)| matches!(
                why,
                weaver_graph::Why::Encrypted | weaver_graph::Why::EncryptedHeader
            ))
            && !outcome.graph().delivery.withheld.is_empty(),
        "{}",
        said(&outcome)
    );
}

// Guard: the outer RAR lost an article its recovery set cannot rebuild, so
// nothing proved the nested set's bytes: the volume the loss fell in is
// short, and the job is not Complete.
#[tokio::test]
async fn a_nested_set_whose_bytes_were_lost_on_the_way_is_not_complete() {
    let scenario = in_outer_rar(&with_nfo(inner(&[])), true)
        .lost(0, 1)
        .lost(0, 2)
        .lost(0, 3)
        .lost(1, 1)
        .lost(1, 2);
    let outcome = run(&scenario).await;
    assert_ne!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
}
