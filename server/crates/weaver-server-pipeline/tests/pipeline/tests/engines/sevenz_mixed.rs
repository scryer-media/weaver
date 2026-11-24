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

// A header posted naming a stored and a coded member otherwise, under a
// PAR2 set over the archive as it should be: the store maps and streams
// under the posted header, and its run decodes the coded folder under it.
// The recovery volume is held until nothing moves, so that run ends first.
// The repair rewrites the header; the new map moves both members together.
// Every file ends byte-equal under the repaired names, and the run that
// made the coded members started after the new map was installed.
#[tokio::test]
async fn a_repaired_header_moving_stored_and_coded_members_together_delivers_every_file() {
    let (before, _) = mixed(
        ["ember.mkv", "quill.mkv", "cinder.nfo", "lumen.mkv"],
        delta(),
    );
    let (after, files) = mixed(
        ["embex.mkv", "quill.mkv", "cindex.nfo", "lumen.mkv"],
        delta(),
    );
    assert_eq!(
        before.len(),
        after.len(),
        "the names change, not the layout"
    );
    let mut posts = with_par2(&after);
    posts[0].bytes = before;
    let (scenario, spoken) = hearing(Scenario::new(posts));
    let held = run_holding(&scenario, 2).await;
    assert_eq!(held.status, Some(JobStatus::Complete));
    let mut names: Vec<String> = files.iter().map(|(name, _)| delivered(name)).collect();
    names.sort();
    assert_eq!(held.files.keys().cloned().collect::<Vec<_>>(), names);
    for (name, bytes) in &files {
        assert!(held.files[&delivered(name)] == *bytes, "{name} bytes");
    }
    // The run under the posted header named the coded members as it
    // decoded them; the one under the repaired header made them again.
    let generations = assert_runs_made_only_coded(&spoken);
    assert!(
        generations.len() >= 2,
        "a run started after the repaired map: {generations:?}"
    );
    let last = *generations.last().unwrap();
    let closed: BTreeSet<u32> = spoken
        .lock()
        .unwrap()
        .iter()
        .filter(|(generation, message)| *generation == last && message.contains("Close"))
        .flat_map(|(_, message)| {
            CODED
                .into_iter()
                .filter(move |id| message.contains(&format!("MemberId({id})")))
        })
        .collect();
    assert_eq!(
        closed,
        BTreeSet::from(CODED),
        "the last run closed the coded members: {:?}",
        spoken.lock().unwrap()
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

// The archives the job's events said were given up on, each with why.
fn demoted(graph: &Outcome) -> Vec<(String, String, String)> {
    use weaver_server_base::events::model::PipelineEvent;
    graph
        .events
        .iter()
        .filter_map(|event| match event {
            PipelineEvent::ArchiveDemoted {
                member,
                container,
                reason,
                ..
            } => Some((member.clone(), container.clone(), reason.clone())),
            _ => None,
        })
        .collect()
}

// The run's refusal names the archive once, with why its coded members
// could not be made; the container stands as it did.
fn assert_demoted(graph: &Outcome, reason: &str) {
    assert_eq!(
        demoted(graph),
        vec![(
            "lantern.7z".to_owned(),
            "sevenz_store".to_owned(),
            reason.to_owned()
        )],
        "{:?}",
        graph.events
    );
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
    assert_demoted(&graph, "unrepairable");
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
    assert_demoted(&graph, "unrepairable");
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
// again, and the stored members are delivered. The archive is withheld for
// what the run's reservation said, not as a set that never placed what it
// named, and the job's events say so.
async fn no_decode_pool(names: [&'static str; 4]) -> (Outcome, Files) {
    use weaver_graph::Why;
    use weaver_server_pipeline::pipeline::graph::engine::{DecodePool, RUN_OVERHEAD};
    let (archive, files) = mixed(names, lzma2());
    let (scenario, spoken) = hearing(
        Scenario::new(vec![Post::plain("lantern.7z", archive)])
            .setup(|pipeline| pipeline.graph_decode = Arc::new(DecodePool::new(RUN_OVERHEAD - 1))),
    );
    let graph = run(&scenario).await;
    // No run emitted: its reservation was refused before it started, and
    // it was not asked again.
    assert_eq!(named(&spoken), Default::default(), "no run emitted");
    // The coded members are not delivered, and the job ends.
    for (name, _) in [&files[1], &files[2]] {
        assert!(!graph.files.contains_key(&delivered(name)), "{name}");
    }
    let withheld = &graph.graph().delivery.withheld;
    assert!(
        matches!(
            withheld.as_slice(),
            [(name, Why::TooLarge { needed: Some(_), allowed: Some(_) })] if name == "lantern.7z"
        ),
        "{withheld:?}"
    );
    assert_demoted(&graph, "too_large");
    (graph, files)
}

#[tokio::test]
async fn a_coded_folder_no_decode_pool_holds_ends_partial_beside_the_stored_members() {
    let (graph, files) = no_decode_pool(MKV).await;
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
}

// The same with nothing usable stored: the job fails, and its reason names
// the archive and the decode memory its run needed.
#[tokio::test]
async fn a_coded_folder_no_decode_pool_holds_with_no_usable_stored_member_fails_for_its_memory() {
    let (graph, _) = no_decode_pool(BIN).await;
    let Some(JobStatus::Failed { error }) = &graph.status else {
        panic!("{:?}", graph.status);
    };
    assert!(
        error.starts_with("lantern.7z needs ") && error.contains(" of decode memory, "),
        "{error}"
    );
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

// A mixed 7z below another object, and archives below a mixed 7z. Below the
// root only release layers open, and a container whose name says an archive
// is one: what it holds lands in the folder it sat in, here the job's.
mod nested {
    use std::collections::BTreeMap;
    use std::io::Write;

    use ::zip::ZipWriter;
    use ::zip::write::SimpleFileOptions;
    use weaver_graph::NodeId;

    use super::*;

    // What every run said, by the node it ran for.
    type Heard = Arc<Mutex<Vec<(NodeId, u64, String)>>>;

    fn heard(scenario: Scenario) -> (Scenario, Heard) {
        let heard = Heard::default();
        let kept = Arc::clone(&heard);
        let scenario = scenario.on_run_message(move |node, generation, message| {
            kept.lock().unwrap().push((node, generation, message));
        });
        (scenario, heard)
    }

    // The generations each node's runs emitted under, by node.
    fn generations(heard: &Heard) -> BTreeMap<NodeId, BTreeSet<u64>> {
        let mut by_node: BTreeMap<NodeId, BTreeSet<u64>> = BTreeMap::new();
        for (node, generation, message) in heard.lock().unwrap().iter() {
            if message.starts_with("Emit(") || message.starts_with("Paid(") {
                by_node.entry(*node).or_default().insert(*generation);
            }
        }
        by_node
    }

    fn zip_of(options: SimpleFileOptions, entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        for (name, bytes) in entries {
            writer.start_file(*name, options).unwrap();
            writer.write_all(bytes).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    fn stored_zip() -> SimpleFileOptions {
        SimpleFileOptions::default().compression_method(::zip::CompressionMethod::Stored)
    }

    fn deflated_zip() -> SimpleFileOptions {
        SimpleFileOptions::default().compression_method(::zip::CompressionMethod::Deflated)
    }

    // One folder of a 7z: its methods and the entries it holds.
    type Folder<'a> = (Vec<EncoderConfiguration>, Vec<(&'a str, Vec<u8>)>);

    // A 7z of `folders`.
    fn sevenz(folders: &[Folder<'_>]) -> Vec<u8> {
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
        for (methods, entries) in folders {
            writer.set_content_methods(methods.clone());
            writer
                .push_archive_entries(
                    entries
                        .iter()
                        .map(|(name, _)| ArchiveEntry::new_file(name))
                        .collect(),
                    entries
                        .iter()
                        .map(|(_, bytes)| SourceReader::new(Cursor::new(bytes.clone())))
                        .collect(),
                )
                .unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    // Every file, every byte as written, and nothing else.
    fn assert_delivers(graph_files: &BTreeMap<String, Vec<u8>>, files: &[(&str, Vec<u8>)]) {
        let mut names: Vec<String> = files.iter().map(|(name, _)| delivered(name)).collect();
        names.sort();
        assert_eq!(
            graph_files.keys().cloned().collect::<Vec<_>>(),
            names,
            "delivered names"
        );
        for (name, bytes) in files {
            assert!(graph_files[&delivered(name)] == *bytes, "{name} bytes");
        }
    }

    // A1: the mixed 7z a stored member of a zip. The zip streams it; the
    // 7z store below streams its stored members and keeps only the coded
    // pack and headers on its shelf, as at the root.
    #[tokio::test]
    async fn a_mixed_7z_inside_a_stored_zip_streams_as_at_the_root() {
        let files = vec![
            ("ember.mkv", payload(40 * ARTICLE + 11, 71)),
            ("cinder.nfo", payload(2 * ARTICLE + 5, 72)),
        ];
        let archive = sevenz(&[
            (copy(), vec![files[0].clone()]),
            (lzma2(), vec![files[1].clone()]),
        ]);
        // What the zip keeps of itself, beside a member that is no archive.
        let bare = run(&Scenario::new(vec![Post::plain(
            "Ember.zip",
            zip_of(stored_zip(), &[("lantern.bin", archive.clone())]),
        )]))
        .await;
        assert!(bare.completed(), "{:?}", bare.failure);
        let outer = zip_of(stored_zip(), &[("lantern.7z", archive.clone())]);
        let (scenario, heard) = heard(Scenario::new(vec![Post::plain("Ember.zip", outer)]));
        let graph = run(&scenario).await;
        assert!(graph.completed(), "{:?}", graph.failure);
        assert_delivers(&graph.files, &files);
        assert_fetched_once(&graph);
        assert!(
            kinds(&graph).contains(&"sevenz_store"),
            "{:?}",
            kinds(&graph)
        );
        assert!(
            !kinds(&graph).contains(&"sevenz_engine"),
            "{:?}",
            kinds(&graph)
        );
        // Only the coded folder's run made anything.
        let ran = generations(&heard);
        assert_eq!(ran.len(), 1, "{ran:?}");
        // The 7z's shelf keeps its headers and the coded pack, no stored byte.
        // The shelf is read at its peak over the job, so the zip's own keep
        // and the 7z's cannot be told apart byte for byte; a 7z that kept
        // its stored member would add the whole member to it.
        assert!(
            graph.graph().shelved < bare.graph().shelved + files[0].1.len() as u64 / 2,
            "{} kept beside {} for the zip alone",
            graph.graph().shelved,
            bare.graph().shelved
        );
    }

    // A3: the mixed 7z cut into numbered parts. The split store joins them,
    // and the 7z store below reads the join as one file.
    #[tokio::test]
    async fn a_mixed_7z_in_numbered_parts_streams_as_one_file() {
        let (archive, files) = mixed(MKV, lzma2());
        let cut = (archive.len() / 2).next_multiple_of(ARTICLE);
        let graph = run(&Scenario::new(vec![
            Post::plain("lantern.7z.001", archive[..cut].to_vec()),
            Post::plain("lantern.7z.002", archive[cut..].to_vec()),
        ]))
        .await;
        assert!(graph.completed(), "{:?}", graph.failure);
        assert_delivers(&graph.files, &files);
        assert_fetched_once(&graph);
        assert!(
            !kinds(&graph).contains(&"sevenz_engine"),
            "{:?}",
            kinds(&graph)
        );
        assert_eq!(
            graph.graph().shelved,
            archive.len() as u64 - stored_bytes(&files),
        );
    }

    // A2: the mixed 7z a deflated member of a zip, which the zip's run
    // makes. A byte of the deflate stream is wrong as posted; the zip's run
    // reads it and fails, the PAR2 set over the zip rebuilds it, and the
    // zip's run starts over: it retracts the 7z and writes it again, and
    // the 7z store and its run below start over on the new bytes.
    #[tokio::test]
    async fn a_mixed_7z_a_zip_run_makes_again_after_a_repair_starts_over_below() {
        let (archive, files) = mixed(MKV, delta());
        let outer = zip_of(deflated_zip(), &[("lantern.7z", archive)]);
        let mut posted = outer.clone();
        posted[outer.len() / 3] ^= 0x5a;
        let mut posts = vec![Post::plain("Ember.zip", posted)];
        posts.extend(par2_set("Ember", &[("Ember.zip", &outer)], 4));
        let (scenario, heard) = heard(Scenario::new(posts));
        let held = run_holding(&scenario, 2).await;
        assert_eq!(held.status, Some(JobStatus::Complete));
        assert_delivers(&held.files, &files);
        // The zip's run read the wrong byte, retracted the 7z and gave up;
        // once repaired, the zip opens again on a new node whose run makes
        // the 7z again, and the 7z below makes its coded members once, from
        // the bytes the new run made.
        let made = |name: &str| {
            heard
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, _, message)| {
                    message.starts_with("Emit(Member")
                        && message.contains(&format!("name: {name:?}"))
                })
                .map(|(node, generation, _)| (*node, *generation))
                .collect::<Vec<_>>()
        };
        let zips = made("lantern.7z");
        assert_eq!(zips.len(), 2, "{zips:?}");
        assert_ne!(zips[0].0, zips[1].0, "{zips:?}");
        for (name, _) in &files {
            assert!(made(name).len() <= 1, "{name}: {:?}", made(name));
        }
    }

    fn inner_files() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("spark.mkv", payload(4 * ARTICLE + 3, 61)),
            ("flint.nfo", payload(ARTICLE / 2, 62)),
        ]
    }

    fn inner_zip() -> Vec<u8> {
        zip_of(stored_zip(), &inner_files())
    }

    // What the 7z holds: a stored mkv, the zip, and an nfo.
    fn outer_entries() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("ember.mkv", payload(5 * ARTICLE + 11, 41)),
            ("inner.zip", inner_zip()),
            ("cinder.nfo", payload(2 * ARTICLE + 5, 43)),
        ]
    }

    // What a job of any of them delivers: the 7z's files beside the zip,
    // and what the zip holds, in the job's folder.
    fn delivery() -> Vec<(&'static str, Vec<u8>)> {
        let entries = outer_entries();
        let mut files = vec![entries[0].clone(), entries[2].clone()];
        files.extend(inner_files());
        files
    }

    fn with_par2_of(posted: Vec<u8>, source: &[u8]) -> Vec<Post> {
        let mut posts = vec![Post::plain("lantern.7z", posted)];
        posts.extend(par2_set("lantern", &[("lantern.7z", source)], 4));
        posts
    }

    // B1: a zip a stored member of the mixed 7z, a byte of it wrong as
    // posted. The zip below reads it as the store streams it and finds the
    // damage; the damage is lifted to the 7z's bytes, the PAR2 set rebuilds
    // them, and the store replays the member to the zip.
    #[tokio::test]
    async fn a_zip_stored_in_a_mixed_7z_is_repaired_through_the_store() {
        let entries = outer_entries();
        let archive = sevenz(&[
            (copy(), vec![entries[0].clone()]),
            (copy(), vec![entries[1].clone()]),
            (delta(), vec![entries[2].clone()]),
        ]);
        let mut posted = archive.clone();
        // Inside spark.mkv's bytes, in the zip, in the 7z's second folder.
        let at = 32 + entries[0].1.len() + 100 + ARTICLE;
        posted[at] ^= 0x5a;
        let (scenario, heard) = heard(Scenario::new(with_par2_of(posted, &archive)));
        let graph = run(&scenario).await;
        assert!(graph.completed(), "{:?}", graph.failure);
        assert_delivers(&graph.files, &delivery());
        assert_fetched_once(&graph);
        // The coded folder's run made only the nfo, once.
        let ran = generations(&heard);
        assert_eq!(ran.len(), 1, "{ran:?}");
        assert!(ran.values().all(|ran| ran.len() == 1), "{ran:?}");
    }

    // B2: the zip a coded member of the mixed 7z, a byte of the coded pack
    // wrong as posted. The run reads it and refuses; the PAR2 set rebuilds
    // the pack, and the run starts over: it retracts the zip it made, and
    // the zip below starts over on the bytes the new run makes.
    #[tokio::test]
    async fn a_zip_coded_in_a_mixed_7z_starts_over_when_the_run_does() {
        let entries = outer_entries();
        let archive = sevenz(&[
            (copy(), vec![entries[0].clone()]),
            (delta(), vec![entries[1].clone(), entries[2].clone()]),
        ]);
        let mut posted = archive.clone();
        let at = 32 + entries[0].1.len() + 100 + ARTICLE;
        posted[at] ^= 0x5a;
        let (scenario, heard) = heard(Scenario::new(with_par2_of(posted, &archive)));
        let held = run_holding(&scenario, 2).await;
        assert_eq!(held.status, Some(JobStatus::Complete));
        assert_delivers(&held.files, &delivery());
        let ran = generations(&heard);
        assert_eq!(ran.len(), 1, "only the 7z runs: {ran:?}");
        assert!(
            ran.values().all(|ran| ran.len() >= 2),
            "the run ran again once repaired: {ran:?}"
        );
    }

    // B3: the same coded pack beyond repair: the run refuses, the zip it
    // was making never closes, and the stored mkv beside it is a usable
    // file, so the job is Partial.
    #[tokio::test]
    async fn a_zip_coded_in_a_mixed_7z_beyond_repair_leaves_the_stored_files_partial() {
        let entries = outer_entries();
        let archive = sevenz(&[
            (copy(), vec![entries[0].clone()]),
            (delta(), vec![entries[1].clone(), entries[2].clone()]),
        ]);
        let lost = article_in(32 + entries[0].1.len()..archive.len() - 200);
        let graph =
            run(&Scenario::new(vec![Post::plain("lantern.7z", archive)]).damaged(0, lost)).await;
        assert_eq!(
            graph.status,
            Some(JobStatus::Partial),
            "{:?} {:?}",
            graph.failure,
            graph.graph().delivery
        );
        assert!(graph.files.get(&delivered("ember.mkv")) == Some(&entries[0].1));
        for name in ["spark.mkv", "flint.nfo", "cinder.nfo"] {
            assert!(!graph.files.contains_key(&delivered(name)), "{name}");
        }
    }

    // C: the same nested content packed all stored, mixed and all coded
    // delivers the same files, and the zip opens in each as a layer.
    #[tokio::test]
    async fn the_same_nested_content_stored_mixed_or_coded_delivers_alike() {
        let entries = outer_entries();
        let cases = [
            (
                "stored",
                sevenz(&[
                    (copy(), vec![entries[0].clone()]),
                    (copy(), vec![entries[1].clone()]),
                    (copy(), vec![entries[2].clone()]),
                ]),
            ),
            (
                "mixed",
                sevenz(&[
                    (copy(), vec![entries[0].clone()]),
                    (lzma2(), vec![entries[1].clone(), entries[2].clone()]),
                ]),
            ),
            ("coded", sevenz(&[(lzma2(), entries.clone())])),
        ];
        for (case, archive) in cases {
            let graph = run(&Scenario::new(vec![Post::plain("lantern.7z", archive)])).await;
            assert_eq!(graph.status, Some(JobStatus::Complete), "{case}");
            assert_delivers(&graph.files, &delivery());
            assert_fetched_once(&graph);
        }
    }
}
