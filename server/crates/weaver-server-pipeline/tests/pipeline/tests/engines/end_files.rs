// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// End files whose bytes are a container format: a disk image that opens on a
// bzip2 stream, an app package that is a zip of deflated entries. Each is
// delivered whole under its own name, posted plain or inside a RAR:
// archives unpack by name, and neither name is an archive's.
// A member under a name that says nothing, whose bytes open a bzip2 stream
// that then goes bad, fails the job, plain or inside a RAR. And a deflated zip
// posted under a name the zip engine cannot number, which is still opened,
// reaches a verdict.

use std::io::{Cursor, Write};

use ::zip::CompressionMethod;
use ::zip::ZipWriter;
use ::zip::write::SimpleFileOptions;

use super::harness::{ARTICLE, Outcome, Post, Scenario, noise, run, try_run};
use super::streams::bzip2;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::multi_member_store_set;

const FOLDER: &str = "lantern beacon";
const SECRET: &str = "beacon-lantern-secret";

fn delivered(name: &str) -> String {
    format!("{FOLDER}/{name}")
}

// A UDBZ disk image's shape: a whole bzip2 stream at its head, then bytes
// that are no bzip2 (the image's other chunks and its trailer).
fn disk_image() -> Vec<u8> {
    let mut bytes = bzip2(&noise(2 * ARTICLE, 3));
    bytes.extend(noise(3 * ARTICLE + 11, 4));
    bytes
}

// An app package's shape: a zip of deflated entries.
fn app_package() -> Vec<u8> {
    let deflated = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, bytes) in [
        ("AndroidManifest.xml", vec![b'm'; 4 * ARTICLE]),
        ("classes.dex", noise(3 * ARTICLE + 5, 9)),
    ] {
        writer.start_file(name, deflated).unwrap();
        writer.write_all(&bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

// A stored RAR5 volume of one entry.
fn rar_of(name: &str, bytes: Vec<u8>) -> Vec<u8> {
    multi_member_store_set(&[(name, bytes)], 1).remove(0).1
}

fn assert_whole(outcome: &Outcome, name: &str, bytes: &[u8]) {
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.failure
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered(name)]
    );
    assert!(outcome.files[&delivered(name)] == bytes, "{name} not whole");
}

#[tokio::test]
async fn a_disk_image_opening_on_a_bzip2_stream_is_delivered_whole() {
    let image = disk_image();
    let outcome = run(&Scenario::new(vec![Post::plain(
        "Installer.dmg",
        image.clone(),
    )]))
    .await;
    assert_whole(&outcome, "Installer.dmg", &image);
}

#[tokio::test]
async fn a_disk_image_inside_a_rar_is_delivered_whole() {
    let image = disk_image();
    let posts = vec![Post::plain(
        "lantern.rar",
        rar_of("Installer.dmg", image.clone()),
    )];
    let outcome = run(&Scenario::new(posts)).await;
    assert_whole(&outcome, "Installer.dmg", &image);
}

#[tokio::test]
async fn an_app_package_inside_a_rar_of_a_passworded_job_is_delivered_whole() {
    let package = app_package();
    let posts = vec![Post::plain(
        "lantern.part01.rar",
        rar_of("Sporcle.v3.1.0.apk", package.clone()),
    )];
    let outcome = try_run(&Scenario::new(posts).password(SECRET))
        .await
        .unwrap_or_else(|stall| panic!("stalled at {:?}: {}", stall.status, stall.state));
    assert_whole(&outcome, "Sporcle.v3.1.0.apk", &package);
}

#[tokio::test]
async fn an_app_package_posted_plain_is_delivered_whole() {
    let package = app_package();
    let outcome = run(&Scenario::new(vec![Post::plain(
        "Sporcle.v3.1.0.apk",
        package.clone(),
    )]))
    .await;
    assert_whole(&outcome, "Sporcle.v3.1.0.apk", &package);
}

// A deflated zip whose name is no zip volume's and no end file's, inside a
// RAR of a passworded job: the zip store gives it up as compressed, and the
// engine then bound over it has no name to number it by. The job still
// ends, with the zip's members out, whole or spanned, served either way.
#[tokio::test]
async fn a_deflated_zip_under_an_unnumbered_name_inside_a_rar_reaches_a_verdict() {
    for volumes in [1, 3] {
        for backwards in [false, true] {
            let posts: Vec<Post> =
                multi_member_store_set(&[("a1b2c3d4e5f6", app_package())], volumes)
                    .into_iter()
                    .map(|(name, bytes)| Post::plain(&name, bytes))
                    .collect();
            let mut scenario = Scenario::new(posts).password(SECRET);
            if backwards {
                scenario = scenario.backwards();
            }
            let outcome = try_run(&scenario).await.unwrap_or_else(|stall| {
                panic!(
                    "{volumes} volumes, backwards {backwards}: stalled at {:?}: {}",
                    stall.status, stall.state
                )
            });
            assert_eq!(
                outcome.status,
                Some(JobStatus::Complete),
                "{volumes} volumes, backwards {backwards}: {:?}",
                outcome.failure
            );
            let mut names: Vec<_> = outcome.files.keys().cloned().collect();
            names.sort();
            assert_eq!(
                names,
                vec![delivered("AndroidManifest.xml"), delivered("classes.dex")]
            );
        }
    }
}

// Bytes that open a whole bzip2 block and then go bad: no bzip2 stream, but
// a file of its own.
fn opens_bzip2_then_goes_bad() -> Vec<u8> {
    let mut bytes = bzip2(&noise(2 * ARTICLE, 5));
    bytes.truncate(bytes.len() / 2);
    bytes.extend(noise(2 * ARTICLE + 7, 6));
    bytes
}

// The job fails for a member whose bytes open an archive type that is
// extracted and that then cannot be extracted, whatever it is named: it is
// no file of its own.
fn assert_fails_for(outcome: &Outcome, name: &str) {
    assert_eq!(
        outcome.status,
        Some(JobStatus::Failed {
            error: format!("no file was delivered whole; 1 short: {name}")
        }),
        "{:?}",
        outcome.files.keys()
    );
}

#[tokio::test]
async fn a_member_opening_on_a_bad_bzip2_stream_fails_the_job() {
    let bytes = opens_bzip2_then_goes_bad();
    for name in ["9f3a7c1e5b2d", "lantern.qzx"] {
        let outcome = try_run(&Scenario::new(vec![Post::plain(name, bytes.clone())]))
            .await
            .unwrap_or_else(|stall| {
                panic!("{name}: stalled at {:?}: {}", stall.status, stall.state)
            });
        assert_fails_for(&outcome, name);
    }
}

#[tokio::test]
async fn a_member_opening_on_a_bad_bzip2_stream_inside_a_rar_fails_the_job() {
    for name in ["9f3a7c1e5b2d", "lantern.qzx"] {
        let posts = vec![Post::plain(
            "lantern.rar",
            rar_of(name, opens_bzip2_then_goes_bad()),
        )];
        let outcome = try_run(&Scenario::new(posts))
            .await
            .unwrap_or_else(|stall| {
                panic!("{name}: stalled at {:?}: {}", stall.status, stall.state)
            });
        assert_fails_for(&outcome, name);
    }
}

// The same bytes under a name that says they are a bzip2 stream are no file
// of their own: the stream is what was posted, and it is bad.
#[tokio::test]
async fn a_member_named_a_stream_is_not_delivered_whole_when_refused() {
    let bytes = opens_bzip2_then_goes_bad();
    let outcome = try_run(&Scenario::new(vec![Post::plain("lantern.bz2", bytes)]))
        .await
        .unwrap_or_else(|stall| panic!("stalled at {:?}: {}", stall.status, stall.state));
    assert_ne!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.files.keys()
    );
}

// An e-book, comic books of a zip and of a RAR, an iOS app and an Adobe
// package inside a RAR: each is delivered whole under its own name, never
// unpacked into its pages or entries.
#[tokio::test]
async fn end_files_inside_a_rar_are_delivered_whole() {
    let members: Vec<(&str, Vec<u8>)> = vec![
        ("Lantern.epub", app_package()),
        ("Lantern.cbz", app_package()),
        ("Lantern.cbr", rar_of("page001.jpg", noise(3 * ARTICLE, 12))),
        ("Lantern.ipa", app_package()),
        ("Lantern.pima", app_package()),
    ];
    let posts = vec![Post::plain(
        "lantern.rar",
        multi_member_store_set(&members, 1).remove(0).1,
    )];
    let outcome = try_run(&Scenario::new(posts))
        .await
        .unwrap_or_else(|stall| panic!("stalled at {:?}: {}", stall.status, stall.state));
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.failure
    );
    let mut names: Vec<_> = outcome.files.keys().cloned().collect();
    names.sort();
    let mut expected: Vec<_> = members.iter().map(|(name, _)| delivered(name)).collect();
    expected.sort();
    assert_eq!(names, expected);
    for (name, bytes) in &members {
        assert!(
            outcome.files[&delivered(name)] == *bytes,
            "{name} not whole"
        );
    }
}

// Windows app packages and bundles, posted plain and inside a RAR: each is
// a zip, delivered whole under its own name, never unpacked into its
// entries.
#[tokio::test]
async fn windows_app_packages_are_delivered_whole() {
    for name in [
        "Main.msix",
        "Singleton.msixbundle",
        "Lantern.appx",
        "Lantern.appxbundle",
    ] {
        let package = app_package();
        let outcome = run(&Scenario::new(vec![Post::plain(name, package.clone())])).await;
        assert_whole(&outcome, name, &package);
        let outcome = run(&Scenario::new(vec![Post::plain(
            "lantern.rar",
            rar_of(name, package.clone()),
        )]))
        .await;
        assert_whole(&outcome, name, &package);
    }
}
