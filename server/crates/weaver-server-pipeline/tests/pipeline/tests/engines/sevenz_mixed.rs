// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A 7z that stores some folders and codes others. The store streams the
// stored members as they arrive and keeps only the coded packs and the
// headers on its shelf; its run reads that shelf and decodes the coded
// folders alone. A repair of a coded pack restarts the run, which makes the
// coded members again and leaves the stored ones as the store wrote them; a
// repair of a stored member replays it through the store. A coded pack
// nothing can rebuild costs the coded members only.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use super::harness::{
    ARTICLE, Outcome, Post, Scenario, assert_fetched_once, kinds, par2_set, payload, run,
    run_holding,
};
use super::sevenz_fixtures::delivered;
use crate::jobs::model::JobStatus;
use sevenz_turbo::encoder_options::{DeltaOptions, Lzma2Options};
use sevenz_turbo::{
    ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, SourceReader,
};
use weaver_server_pipeline::pipeline::tests::engines::restart_fixtures::restart_row;

type Files = Vec<(&'static str, Vec<u8>)>;

// The names of the four members, in the archive's order: a stored folder,
// a coded folder of two, and a stored folder.
const MKV: [&str; 4] = ["ember.mkv", "quill.mkv", "cinder.nfo", "lumen.mkv"];
const BIN: [&str; 4] = ["ember.bin", "quill.mkv", "cinder.nfo", "lumen.bin"];

// The store numbers members as the archive does: the coded folder holds
// the second and third.
const CODED: [u32; 2] = [1, 2];

fn copy() -> Vec<EncoderConfiguration> {
    vec![EncoderConfiguration::new(EncoderMethod::COPY)]
}

fn lzma2() -> Vec<EncoderConfiguration> {
    vec![Lzma2Options::from_level(1).into()]
}

// A filter that codes each byte alone: its pack is as long as what it
// holds, so an article of the pack is an article of a coded member.
fn delta() -> Vec<EncoderConfiguration> {
    vec![DeltaOptions::from_distance(1).into()]
}

// `names[0]` stored, `names[1]` and `names[2]` in one folder coded by
// `coded`, `names[3]` stored.
fn mixed(names: [&'static str; 4], coded: Vec<EncoderConfiguration>) -> (Vec<u8>, Files) {
    mixed_under(names, copy(), coded, false)
}

// The same, each stored folder through `stored`: a copy, or a copy under
// AES.
fn mixed_under(
    names: [&'static str; 4],
    stored: Vec<EncoderConfiguration>,
    coded: Vec<EncoderConfiguration>,
    encrypt_header: bool,
) -> (Vec<u8>, Files) {
    let files: Files = vec![
        (names[0], payload(5 * ARTICLE + 11, 41)),
        (names[1], payload(3 * ARTICLE - 3, 42)),
        (names[2], payload(2 * ARTICLE + 5, 43)),
        (names[3], payload(4 * ARTICLE, 44)),
    ];
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_encrypt_header(encrypt_header);
    writer.set_content_methods(stored.clone());
    writer
        .push_archive_entry(
            ArchiveEntry::new_file(files[0].0),
            Some(Cursor::new(files[0].1.clone())),
        )
        .unwrap();
    writer.set_content_methods(coded);
    writer
        .push_archive_entries(
            vec![
                ArchiveEntry::new_file(files[1].0),
                ArchiveEntry::new_file(files[2].0),
            ],
            vec![
                SourceReader::new(Cursor::new(files[1].1.clone())),
                SourceReader::new(Cursor::new(files[2].1.clone())),
            ],
        )
        .unwrap();
    writer.set_content_methods(stored.clone());
    writer
        .push_archive_entry(
            ArchiveEntry::new_file(files[3].0),
            Some(Cursor::new(files[3].1.clone())),
        )
        .unwrap();
    (writer.finish().unwrap().into_inner(), files)
}

// The bytes of the stored members, which no file of the job's holds twice.
fn stored_bytes(files: &Files) -> u64 {
    (files[0].1.len() + files[3].1.len()) as u64
}

// Past the 32-byte signature header: the stored member, then the coded pack.
fn coded_pack(files: &Files) -> std::ops::Range<usize> {
    let start = 32 + files[0].1.len();
    start..start + files[1].1.len() + files[2].1.len()
}

// The first article wholly inside `range`.
fn article_in(range: std::ops::Range<usize>) -> u32 {
    let article = range.start.div_ceil(ARTICLE);
    assert!((article + 1) * ARTICLE <= range.end, "{range:?}");
    article as u32
}

fn with_par2(archive: &[u8]) -> Vec<Post> {
    let mut posts = vec![Post::plain("lantern.7z", archive.to_vec())];
    posts.extend(par2_set("lantern", &[("lantern.7z", archive)], 4));
    posts
}

// Every member, every byte as written, and nothing else.
fn assert_members(outcome: &Outcome, files: &Files) {
    assert!(outcome.completed(), "{:?}", outcome.failure);
    let mut names: Vec<String> = files.iter().map(|(name, _)| delivered(name)).collect();
    names.sort();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        names,
        "delivered names"
    );
    for (name, bytes) in files {
        assert!(outcome.files[&delivered(name)] == *bytes, "{name} bytes");
    }
    assert!(
        !kinds(outcome).contains(&"sevenz_engine"),
        "the store kept the set: {:?}",
        kinds(outcome)
    );
}

// What the job's runs said, by generation, with the members each emit
// named.
type Spoken = Arc<Mutex<Vec<(u64, String)>>>;

fn hearing(scenario: Scenario) -> (Scenario, Spoken) {
    let spoken = Spoken::default();
    let kept = Arc::clone(&spoken);
    let scenario = scenario.on_run_message(move |_, generation, message| {
        kept.lock().unwrap().push((generation, message));
    });
    (scenario, spoken)
}

// Every member a run's emit named, and the generations that emitted.
fn named(spoken: &Spoken) -> (BTreeSet<u32>, BTreeSet<u64>) {
    let mut members = BTreeSet::new();
    let mut generations = BTreeSet::new();
    for (generation, message) in spoken.lock().unwrap().iter() {
        if !(message.starts_with("Emit(") || message.starts_with("Paid(")) {
            continue;
        }
        generations.insert(*generation);
        let mut rest = message.as_str();
        while let Some(at) = rest.find("MemberId(") {
            rest = &rest[at + "MemberId(".len()..];
            let end = rest.find(')').unwrap();
            members.insert(rest[..end].parse().unwrap());
        }
    }
    (members, generations)
}

// No run named, made, wrote or retracted a stored member: the store wrote
// each once, as it streamed.
fn assert_runs_made_only_coded(spoken: &Spoken) -> BTreeSet<u64> {
    let (members, generations) = named(spoken);
    assert_eq!(members, BTreeSet::from(CODED), "the members runs named");
    generations
}

#[tokio::test]
async fn a_mixed_7z_streams_its_stored_members_and_shelves_only_the_rest() {
    let (archive, files) = mixed(MKV, lzma2());
    let (scenario, spoken) = hearing(Scenario::new(vec![Post::plain(
        "lantern.7z",
        archive.clone(),
    )]));
    let graph = run(&scenario).await;
    assert_members(&graph, &files);
    assert_fetched_once(&graph);
    assert_runs_made_only_coded(&spoken);
    assert_eq!(
        graph.graph().shelved,
        archive.len() as u64 - stored_bytes(&files),
        "the shelf keeps the headers and the coded pack, no stored byte"
    );
}

// A byte of the coded pack wrong in the post itself, under a PAR2 set over
// the archive as written: every article arrives intact, so only the coded
// folder's CRC finds it. The recovery volume is held until nothing moves,
// so the run has read the damage and ended before the repair. Once it
// comes, the repair rebuilds the pack and the run decodes the folder again;
// the stored members are the store's, and no run names them.
#[tokio::test]
async fn a_coded_pack_repaired_by_par2_after_its_run_read_it_restarts_the_run_alone() {
    let (archive, files) = mixed(MKV, delta());
    let mut posted = archive.clone();
    posted[coded_pack(&files).start + 7] ^= 0x5a;
    let mut posts = with_par2(&archive);
    posts[0].bytes = posted;
    let (scenario, spoken) = hearing(Scenario::new(posts));
    let held = run_holding(&scenario, 2).await;
    assert_eq!(held.status, Some(JobStatus::Complete));
    let mut names: Vec<String> = files.iter().map(|(name, _)| delivered(name)).collect();
    names.sort();
    assert_eq!(held.files.keys().cloned().collect::<Vec<_>>(), names);
    for (name, bytes) in &files {
        assert!(held.files[&delivered(name)] == *bytes, "{name} bytes");
    }
    let generations = assert_runs_made_only_coded(&spoken);
    let first = *generations.first().unwrap();
    assert!(
        spoken
            .lock()
            .unwrap()
            .iter()
            .any(|(generation, message)| *generation == first && message.contains("Refuse")),
        "the first run read the damage: {:?}",
        spoken.lock().unwrap()
    );
    assert!(
        generations.len() >= 2,
        "the run ran again once repaired: {generations:?}"
    );
}

// An article of the stored member lost, under a PAR2 set: the repair
// rebuilds it and the store replays it through its map. The coded members
// come out of their run whole.
#[tokio::test]
async fn a_damaged_stored_member_is_repaired_and_replayed_beside_the_coded_ones() {
    let (archive, files) = mixed(MKV, delta());
    let damaged = article_in(32..32 + files[0].1.len());
    let (scenario, spoken) = hearing(Scenario::new(with_par2(&archive)).damaged(0, damaged));
    let graph = run(&scenario).await;
    assert_members(&graph, &files);
    assert_fetched_once(&graph);
    assert_runs_made_only_coded(&spoken);
}

// An article of the coded pack lost with no PAR2: the stored members are
// delivered whole and the coded ones are not.
async fn coded_pack_lost(names: [&'static str; 4]) -> (Outcome, Files) {
    let (archive, files) = mixed(names, delta());
    let lost = article_in(coded_pack(&files));
    let (scenario, spoken) =
        hearing(Scenario::new(vec![Post::plain("lantern.7z", archive)]).damaged(0, lost));
    let graph = run(&scenario).await;
    // The run that read the lost bytes is the only one: its refusal ends
    // it under its shelf's generation, and nothing asks it again.
    let (_, generations) = named(&spoken);
    assert_eq!(generations.len(), 1, "{generations:?}");
    for (name, bytes) in [&files[0], &files[3]] {
        assert!(
            graph.files.get(&delivered(name)) == Some(bytes)
                || graph.retained_files.values().any(|kept| kept == bytes),
            "{name} kept whole: {:?} {:?}",
            graph.files.keys(),
            graph.retained_files.keys()
        );
    }
    for (name, bytes) in [&files[1], &files[2]] {
        assert!(
            graph.files.get(&delivered(name)) != Some(bytes),
            "{name} cannot be whole"
        );
    }
    (graph, files)
}

#[tokio::test]
async fn a_coded_pack_beyond_repair_delivers_the_stored_members_partial() {
    let (graph, _) = coded_pack_lost(MKV).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?} {:?}",
        graph.failure,
        graph.files.keys()
    );
}

// The same with nothing usable stored: whole `.bin` files alone are no
// delivery, and the job fails.
#[tokio::test]
async fn a_coded_pack_beyond_repair_with_no_usable_stored_member_fails() {
    let (graph, _) = coded_pack_lost(BIN).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?} {:?}",
        graph.status,
        graph.files.keys()
    );
}

// Taken down and restored after every article: the map, the shelf and the
// run come back from disk, and the job ends as the uninterrupted one does.
#[tokio::test]
async fn a_mixed_7z_restarted_after_every_article_delivers_its_members() {
    let (archive, files) = mixed(MKV, lzma2());
    let whole = restart_row(Scenario::new(vec![Post::plain("lantern.7z", archive)])).await;
    assert_members(&whole, &files);
}

// No decode pool can hold the coded folder's run: its reservation is
// refused before it starts. The run ends where it stands and is not asked
// again, the stored members are delivered, and the job ends Partial.
#[tokio::test]
async fn a_coded_folder_no_decode_pool_holds_ends_partial_beside_the_stored_members() {
    use weaver_server_pipeline::pipeline::graph::engine::{DecodePool, RUN_OVERHEAD};
    let (archive, files) = mixed(MKV, lzma2());
    let (scenario, spoken) = hearing(
        Scenario::new(vec![Post::plain("lantern.7z", archive)])
            .setup(|pipeline| pipeline.graph_decode = Arc::new(DecodePool::new(RUN_OVERHEAD - 1))),
    );
    let graph = run(&scenario).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?} {:?}",
        graph.failure,
        graph.graph().delivery
    );
    for (name, bytes) in [&files[0], &files[3]] {
        assert!(graph.files.get(&delivered(name)) == Some(bytes), "{name}");
    }
    // No run emitted: its reservation was refused before it started, and
    // it was not asked again.
    assert_eq!(named(&spoken), Default::default(), "no run emitted");
    // The coded members are not delivered, and the job ends.
    for (name, _) in [&files[1], &files[2]] {
        assert!(!graph.files.contains_key(&delivered(name)), "{name}");
    }
}

// The stored folders under AES, the coded folder plain, the end header
// enciphered or not.
fn sealed_stored(encrypt_header: bool) -> (Vec<u8>, Files) {
    use sevenz_turbo::Password;
    use sevenz_turbo::encoder_options::AesEncoderOptions;
    let stored = vec![
        AesEncoderOptions::new(Password::new(SECRET)).into(),
        EncoderConfiguration::new(EncoderMethod::COPY),
    ];
    mixed_under(MKV, stored, lzma2(), encrypt_header)
}

const SECRET: &str = "lumen-quill-secret";

// No password ever offered: the store cannot decipher its stored members,
// and the password is what the job reports. With the end header plain, the
// run still decodes the coded folder, whose usable file makes the job
// Partial; with it enciphered nothing is mapped, and the job fails for the
// password.
#[tokio::test]
async fn a_mixed_7z_whose_stored_folders_need_a_password_never_given_reports_the_password() {
    use weaver_graph::Why;
    for encrypt_header in [false, true] {
        let (archive, files) = sealed_stored(encrypt_header);
        let graph = run(&Scenario::new(vec![Post::plain("lantern.7z", archive)])).await;
        let delivery = &graph.graph().delivery;
        assert_eq!(
            graph.failure_kind,
            Some(crate::history::HistoryFailureKind::Password),
            "{encrypt_header}: {:?}",
            graph.failure
        );
        assert_eq!(
            delivery.withheld,
            vec![("lantern.7z".to_owned(), Why::EncryptedHeader)],
            "{encrypt_header}"
        );
        for (name, _) in [&files[0], &files[3]] {
            assert!(!graph.files.contains_key(&delivered(name)), "{name}");
        }
        if encrypt_header {
            assert!(
                matches!(&graph.status, Some(JobStatus::Failed { error }) if error.contains("needs a password")),
                "{:?}",
                graph.status
            );
        } else {
            assert_eq!(graph.status, Some(JobStatus::Partial));
            for (name, bytes) in [&files[1], &files[2]] {
                assert!(graph.files.get(&delivered(name)) == Some(bytes), "{name}");
            }
        }
    }
}

// The password offered: the store deciphers the stored members, and the
// run decodes the coded ones. With the end header enciphered too, the store
// is lent out for its deciphering while the run reads, and the run stands.
#[tokio::test]
async fn a_mixed_7z_whose_stored_folders_need_the_job_password_delivers_every_member() {
    for encrypt_header in [false, true] {
        let (archive, files) = sealed_stored(encrypt_header);
        let (scenario, spoken) =
            hearing(Scenario::new(vec![Post::plain("lantern.7z", archive)]).password(SECRET));
        let graph = run(&scenario).await;
        assert_members(&graph, &files);
        assert_fetched_once(&graph);
        assert_runs_made_only_coded(&spoken);
    }
}

// The end header's article lost under PAR2: the store maps nothing until
// the repair brings it, then streams the stored members and runs for the
// coded ones.
#[tokio::test]
async fn a_mixed_7z_whose_end_header_is_repaired_by_par2_delivers_every_member() {
    let (archive, files) = mixed(MKV, delta());
    let tail = (archive.len() - 1) / ARTICLE;
    let graph = run(&Scenario::new(with_par2(&archive)).damaged(0, tail as u32)).await;
    assert_members(&graph, &files);
    assert_fetched_once(&graph);
}
