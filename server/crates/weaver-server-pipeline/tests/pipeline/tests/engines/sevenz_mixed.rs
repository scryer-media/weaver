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
    // The end header is enciphered by the last methods set: AES with the
    // job's secret, whatever the last folder was.
    if encrypt_header {
        writer.set_content_methods(aes_copy());
    }
    (writer.finish().unwrap().into_inner(), files)
}

// A copy under AES, with the job's secret.
fn aes_copy() -> Vec<EncoderConfiguration> {
    use sevenz_turbo::Password;
    use sevenz_turbo::encoder_options::AesEncoderOptions;
    vec![
        AesEncoderOptions::new(Password::new(SECRET)).into(),
        EncoderConfiguration::new(EncoderMethod::COPY),
    ]
}

// `coded` under AES with the job's secret: AES first, then `coded`.
fn under_aes(coded: Vec<EncoderConfiguration>) -> Vec<EncoderConfiguration> {
    let mut methods = aes_copy();
    methods.truncate(1);
    methods.extend(coded);
    methods
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

// The 7z store kept a shelf on disk during the job, and no file of it is
// left once the job ended: neither among what it delivered nor among what
// a failed job keeps in its folder.
fn assert_shelf_gone(graph: &Outcome) {
    let shelves: Vec<String> = graph
        .graph()
        .shelf_files
        .iter()
        .filter(|(node, _)| {
            graph
                .graph()
                .shelved_by
                .get(node)
                .is_some_and(|(kind, _)| kind == "sevenz_store")
        })
        .flat_map(|(_, files)| files)
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            format!("{}/{name}", weaver_graph::names::INTERNAL_DIR)
        })
        .collect();
    assert!(!shelves.is_empty(), "the store kept a shelf");
    for shelf in &shelves {
        assert!(
            !graph
                .files
                .keys()
                .chain(graph.retained_files.keys())
                .any(|name| name.ends_with(shelf.as_str())),
            "{shelf} left: {:?} {:?}",
            graph.files.keys(),
            graph.retained_files.keys()
        );
    }
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
    assert_shelf_gone(&graph);
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
    assert_shelf_gone(&graph);
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
    assert_shelf_gone(&graph);
}

// The stored folders under AES, the coded folder plain, the end header
// enciphered or not.
fn sealed_stored(encrypt_header: bool) -> (Vec<u8>, Files) {
    mixed_under(MKV, aes_copy(), lzma2(), encrypt_header)
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
    pub(super) type Heard = Arc<Mutex<Vec<(NodeId, u64, String)>>>;

    pub(super) fn heard(scenario: Scenario) -> (Scenario, Heard) {
        let heard = Heard::default();
        let kept = Arc::clone(&heard);
        let scenario = scenario.on_run_message(move |node, generation, message| {
            kept.lock().unwrap().push((node, generation, message));
        });
        (scenario, heard)
    }

    // The generations each node's runs emitted under, by node.
    pub(super) fn generations(heard: &Heard) -> BTreeMap<NodeId, BTreeSet<u64>> {
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
    pub(super) type Folder<'a> = (Vec<EncoderConfiguration>, Vec<(&'a str, Vec<u8>)>);

    // A 7z of `folders`.
    pub(super) fn sevenz(folders: &[Folder<'_>]) -> Vec<u8> {
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
    pub(super) fn assert_delivers(
        graph_files: &BTreeMap<String, Vec<u8>>,
        files: &[(&str, Vec<u8>)],
    ) {
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
        let sevenz: Vec<u64> = graph
            .graph()
            .shelved_by
            .values()
            .filter(|(kind, _)| kind == "sevenz_store")
            .map(|(_, kept)| *kept)
            .collect();
        assert_eq!(
            sevenz,
            vec![archive.len() as u64 - files[0].1.len() as u64],
            "{:?}",
            graph.graph().shelved_by
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

    pub(super) fn inner_files() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("spark.mkv", payload(4 * ARTICLE + 3, 61)),
            ("flint.nfo", payload(ARTICLE / 2, 62)),
        ]
    }

    pub(super) fn inner_zip() -> Vec<u8> {
        zip_of(stored_zip(), &inner_files())
    }

    // What the 7z holds: a stored mkv, the zip, and an nfo.
    pub(super) fn outer_entries() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("ember.mkv", payload(5 * ARTICLE + 11, 41)),
            ("inner.zip", inner_zip()),
            ("cinder.nfo", payload(2 * ARTICLE + 5, 43)),
        ]
    }

    // What a job of any of them delivers: the 7z's files beside the zip,
    // and what the zip holds, in the job's folder.
    pub(super) fn delivery() -> Vec<(&'static str, Vec<u8>)> {
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

    // B2 under AES: the zip a member of a folder compressed under AES. The
    // run deciphers it under the job's password, reads the damage and
    // fails; the PAR2 set rebuilds the pack, and the run starts over under
    // the same password: the zip below starts over on what it makes.
    #[tokio::test]
    async fn a_zip_coded_under_aes_in_a_mixed_7z_starts_over_when_the_run_does() {
        let entries = outer_entries();
        let archive = sevenz(&[
            (copy(), vec![entries[0].clone()]),
            (
                under_aes(delta()),
                vec![entries[1].clone(), entries[2].clone()],
            ),
        ]);
        let mut posted = archive.clone();
        let at = 32 + entries[0].1.len() + 100 + ARTICLE;
        posted[at] ^= 0x5a;
        let (scenario, heard) =
            heard(Scenario::new(with_par2_of(posted, &archive)).global_password(SECRET));
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

// A mixed 7z whose coded folder is compressed under AES, the stored folders
// plain unless said. The store streams the stored members with no password,
// and its run deciphers the coded folder under the password the job offers
// it: the run's ask is the node asking, and each candidate offered starts
// the run again.
mod aes_coded {
    use weaver_graph::Why;

    use super::*;
    use crate::history::HistoryFailureKind;

    fn sealed(coded: Vec<EncoderConfiguration>, encrypt_header: bool) -> (Vec<u8>, Files) {
        mixed_under(MKV, copy(), under_aes(coded), encrypt_header)
    }

    // How many candidates each node was offered.
    fn offered(graph: &Outcome) -> Vec<usize> {
        graph.graph().offered.values().copied().collect()
    }

    // The generations whose run closed a coded member.
    fn closing(spoken: &Spoken) -> BTreeSet<u64> {
        spoken
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, message)| message.contains("MemberClosed"))
            .map(|(generation, _)| *generation)
            .collect()
    }

    // Every member delivered whole, with the stored ones written once and
    // only the coded ones made by runs.
    async fn delivers(scenario: Scenario, files: &Files) -> Outcome {
        let (scenario, spoken) = hearing(scenario);
        let graph = run(&scenario).await;
        assert_members(&graph, files);
        assert_fetched_once(&graph);
        assert_runs_made_only_coded(&spoken);
        assert_eq!(
            graph.graph().shelved,
            graph
                .graph()
                .shelved_by
                .values()
                .map(|(_, kept)| *kept)
                .sum::<u64>()
        );
        graph
    }

    // (i) The right password a candidate.
    #[tokio::test]
    async fn the_right_candidate_delivers_every_member() {
        let (archive, files) = sealed(lzma2(), false);
        let graph = delivers(
            Scenario::new(vec![Post::plain("lantern.7z", archive.clone())]).global_password(SECRET),
            &files,
        )
        .await;
        assert_eq!(offered(&graph), [1]);
        assert_eq!(
            graph.graph().shelved,
            archive.len() as u64 - stored_bytes(&files),
            "the shelf keeps the headers and the coded pack, no stored byte"
        );
        assert_shelf_gone(&graph);
    }

    // (ii) A wrong candidate first: the run fails under it and asks again,
    // and the next candidate makes the coded members.
    #[tokio::test]
    async fn a_wrong_candidate_then_the_right_one_delivers_every_member() {
        let (archive, files) = sealed(lzma2(), false);
        let graph = delivers(
            Scenario::new(vec![Post::plain("lantern.7z", archive)])
                .global_password("ember-wrong-guess")
                .global_password(SECRET),
            &files,
        )
        .await;
        assert_eq!(offered(&graph), [2]);
    }

    // (iii) No password: the stored members are delivered, the run's ask
    // costs the coded members only, and the job is Partial for the password.
    #[tokio::test]
    async fn no_password_delivers_the_stored_members_partial_for_the_password() {
        let (archive, files) = sealed(lzma2(), false);
        let graph = run(&Scenario::new(vec![Post::plain("lantern.7z", archive)])).await;
        assert_eq!(
            graph.status,
            Some(JobStatus::Partial),
            "{:?}",
            graph.failure
        );
        assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Password));
        assert_eq!(
            graph.graph().delivery.withheld,
            vec![("lantern.7z".to_owned(), Why::EncryptedHeader)]
        );
        for (name, bytes) in [&files[0], &files[3]] {
            assert!(graph.files.get(&delivered(name)) == Some(bytes), "{name}");
        }
        for (name, _) in [&files[1], &files[2]] {
            assert!(!graph.files.contains_key(&delivered(name)), "{name}");
        }
        assert_demoted(&graph, "encrypted_header");
        assert_shelf_gone(&graph);
    }

    // (iv) The end header enciphered too: the store opens it with the
    // password, and the run decodes under the one that opened it.
    #[tokio::test]
    async fn an_encrypted_header_with_the_password_delivers_every_member() {
        let (archive, files) = sealed(lzma2(), true);
        let graph = delivers(
            Scenario::new(vec![Post::plain("lantern.7z", archive)]).password(SECRET),
            &files,
        )
        .await;
        assert_eq!(offered(&graph), [1]);
    }

    // (vi) Taken down and restored after every article.
    #[tokio::test]
    async fn restarted_after_every_article_delivers_every_member() {
        let (archive, files) = sealed(lzma2(), false);
        let whole = restart_row(
            Scenario::new(vec![Post::plain("lantern.7z", archive)]).global_password(SECRET),
        )
        .await;
        assert_members(&whole, &files);
    }

    // A byte of the coded pack wrong as posted, under a PAR2 set over the
    // archive as written, with `candidates` in order. The recovery volume is
    // held until nothing moves, so every run has read the damage first.
    fn damaged_pack(candidates: &[&str]) -> (Scenario, Files) {
        let (archive, files) = sealed(delta(), false);
        let mut posted = archive.clone();
        posted[coded_pack(&files).start + 7] ^= 0x5a;
        let mut posts = with_par2(&archive);
        posts[0].bytes = posted;
        let scenario = candidates
            .iter()
            .fold(Scenario::new(posts), |scenario, candidate| {
                scenario.global_password(candidate)
            });
        (scenario, files)
    }

    async fn repaired_after_the_run_read_it(candidates: &[&str]) {
        let (scenario, files) = damaged_pack(candidates);
        let (scenario, spoken) = hearing(scenario);
        let held = run_holding(&scenario, 2).await;
        assert_eq!(
            held.status,
            Some(JobStatus::Complete),
            "{candidates:?} {:?} {:?}",
            held.files.keys(),
            spoken.lock().unwrap()
        );
        let mut names: Vec<String> = files.iter().map(|(name, _)| delivered(name)).collect();
        names.sort();
        assert_eq!(held.files.keys().cloned().collect::<Vec<_>>(), names);
        for (name, bytes) in &files {
            assert!(held.files[&delivered(name)] == *bytes, "{name} bytes");
        }
        assert_runs_made_only_coded(&spoken);
        assert_eq!(closing(&spoken).len(), 1, "{candidates:?}");
    }

    // (v) The right password, the coded pack repaired after its run read it:
    // the run starts again under the same password.
    #[tokio::test]
    async fn a_damaged_coded_pack_repaired_after_its_run_read_it_delivers_every_member() {
        repaired_after_the_run_read_it(&[SECRET]).await;
    }

    // The right password not first.
    #[tokio::test]
    async fn a_damaged_coded_pack_with_the_right_password_second_delivers_every_member() {
        repaired_after_the_run_read_it(&["ember-wrong-guess", SECRET]).await;
    }

    // The right password first and a wrong one after: the run reads the
    // damage under the right one and takes it for the key, and the wrong
    // one fails too. The repair lands behind what the run read, so the
    // job tries its candidates again from the first, which delivers.
    #[tokio::test]
    async fn a_damaged_coded_pack_with_the_right_password_first_delivers_every_member() {
        repaired_after_the_run_read_it(&[SECRET, "ember-wrong-guess"]).await;
    }

    // The same with the right password alone.
    #[tokio::test]
    async fn a_damaged_coded_pack_with_the_right_password_alone_delivers_every_member() {
        repaired_after_the_run_read_it(&[SECRET]).await;
    }

    // The right password first, restarted after every article.
    #[tokio::test]
    async fn a_damaged_coded_pack_right_first_restarted_after_every_article_delivers() {
        let (scenario, files) = damaged_pack(&[SECRET, "ember-wrong-guess"]);
        let whole = restart_row(scenario).await;
        assert_members(&whole, &files);
    }

    // No damage: each candidate is offered once, as before, whichever
    // order they come in; the right one first is the only one offered.
    #[tokio::test]
    async fn without_damage_each_candidate_is_offered_once() {
        let (archive, files) = sealed(lzma2(), false);
        for (candidates, offers) in [
            (vec![SECRET, "ember-wrong-guess"], vec![1]),
            (vec!["ember-wrong-guess", SECRET], vec![2]),
        ] {
            let scenario = candidates.iter().fold(
                Scenario::new(vec![Post::plain("lantern.7z", archive.clone())]),
                |scenario, candidate| scenario.global_password(candidate),
            );
            let graph = delivers(scenario, &files).await;
            assert_eq!(offered(&graph), offers, "{candidates:?}");
        }
    }

    // A whole 7z coded under AES, which the engine opens alone, the right
    // password first: the repair of the pack its run read sends the
    // candidates back to the first, and the engine delivers every file.
    #[tokio::test]
    async fn a_damaged_all_coded_aes_7z_with_the_right_password_first_delivers_every_file() {
        let (_, files) = sealed(delta(), false);
        let entries: Vec<(&str, Vec<u8>)> = files
            .iter()
            .map(|(name, bytes)| (*name, bytes.clone()))
            .collect();
        let archive = super::nested::sevenz(&[(under_aes(delta()), entries)]);
        for candidates in [[SECRET, "ember-wrong-guess"], ["ember-wrong-guess", SECRET]] {
            let mut posted = archive.clone();
            posted[32 + 3 * ARTICLE + 7] ^= 0x5a;
            let mut posts = with_par2(&archive);
            posts[0].bytes = posted;
            let scenario = candidates
                .iter()
                .fold(Scenario::new(posts), |scenario, candidate| {
                    scenario.global_password(candidate)
                });
            let held = run_holding(&scenario, 2).await;
            assert_eq!(
                held.status,
                Some(JobStatus::Complete),
                "{candidates:?} {:?}",
                held.files.keys()
            );
            for (name, bytes) in &files {
                assert!(held.files[&delivered(name)] == *bytes, "{name} bytes");
            }
        }
    }

    // The stored folders under AES too, a byte of the smallest stored member
    // wrong as posted: the store proves a candidate on that member before it
    // opens the folders, and bytes of the proof coming again are tried under
    // every candidate the store holds. The right password alone, or after a
    // wrong one, delivers every member once the proof is repaired.
    #[tokio::test]
    async fn a_damaged_proof_repaired_delivers_every_member() {
        let (archive, files) = mixed_under(MKV, aes_copy(), under_aes(delta()), false);
        // Each folder enciphered is padded to the cipher's block.
        let pad = |len: usize| len.next_multiple_of(16);
        let lumen = 32 + pad(files[0].1.len()) + pad(files[1].1.len() + files[2].1.len());
        let mut posted = archive.clone();
        posted[lumen + 70] ^= 0x5a;
        for candidates in [vec![SECRET], vec!["ember-wrong-guess", SECRET]] {
            let mut posts = with_par2(&archive);
            posts[0].bytes = posted.clone();
            let (scenario, spoken) = hearing(
                candidates
                    .iter()
                    .fold(Scenario::new(posts), |scenario, candidate| {
                        scenario.global_password(candidate)
                    }),
            );
            let held = run_holding(&scenario, 2).await;
            assert_eq!(
                held.status,
                Some(JobStatus::Complete),
                "{candidates:?} {:?} {:?}",
                held.files.keys(),
                spoken.lock().unwrap()
            );
            for (name, bytes) in &files {
                assert!(held.files[&delivered(name)] == *bytes, "{name} bytes");
            }
        }
    }

    // Restarted after every article, the damage repaired.
    #[tokio::test]
    async fn a_damaged_coded_pack_restarted_after_every_article_delivers_every_member() {
        let (scenario, files) = damaged_pack(&[SECRET]);
        let whole = restart_row(scenario).await;
        assert_members(&whole, &files);
    }

    // No password and the pack damaged: the repair runs once, and the job
    // ends Partial for the password.
    #[tokio::test]
    async fn a_damaged_coded_pack_with_no_password_ends_partial_for_the_password() {
        let (scenario, files) = damaged_pack(&[]);
        let graph = run(&scenario).await;
        assert_eq!(
            graph.status,
            Some(JobStatus::Partial),
            "{:?}",
            graph.failure
        );
        assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Password));
        for (name, bytes) in [&files[0], &files[3]] {
            assert!(graph.files.get(&delivered(name)) == Some(bytes), "{name}");
        }
    }

    // An article of the coded pack lost with no PAR2, under the right
    // password: the stored members are delivered, and the job is Partial
    // for the damage, not for a password.
    #[tokio::test]
    async fn a_coded_pack_beyond_repair_is_partial_for_the_damage() {
        let (archive, files) = sealed(delta(), false);
        let lost = article_in(coded_pack(&files));
        let graph = run(&Scenario::new(vec![Post::plain("lantern.7z", archive)])
            .damaged(0, lost)
            .global_password(SECRET))
        .await;
        assert_eq!(
            graph.status,
            Some(JobStatus::Partial),
            "{:?}",
            graph.failure
        );
        assert_ne!(graph.failure_kind, Some(HistoryFailureKind::Password));
        for (name, bytes) in [&files[0], &files[3]] {
            assert!(graph.files.get(&delivered(name)) == Some(bytes), "{name}");
        }
        assert_demoted(&graph, "unrepairable");
    }

    // An article of a stored member lost, under a PAR2 set: the repair
    // rebuilds it and the store replays it, the stored folder plain or under
    // AES. The coded members are made by one run.
    #[tokio::test]
    async fn a_damaged_stored_member_beside_an_aes_coded_folder_is_repaired_and_replayed() {
        for stored in [copy(), aes_copy()] {
            let (archive, files) = mixed_under(MKV, stored, under_aes(delta()), false);
            let damaged = article_in(32..32 + files[0].1.len());
            let (scenario, spoken) = hearing(
                Scenario::new(with_par2(&archive))
                    .damaged(0, damaged)
                    .global_password(SECRET),
            );
            let graph = run(&scenario).await;
            assert_members(&graph, &files);
            assert_fetched_once(&graph);
            assert_runs_made_only_coded(&spoken);
            assert_eq!(closing(&spoken).len(), 1);
        }
    }

    // The start header's article, or the end header's, lost under a PAR2
    // set, the end header plain or enciphered: the store maps nothing until
    // the repair brings it, then streams and runs.
    #[tokio::test]
    async fn a_lost_header_repaired_by_par2_delivers_every_member() {
        for encrypt_header in [false, true] {
            let (archive, files) = sealed(delta(), encrypt_header);
            let tail = ((archive.len() - 1) / ARTICLE) as u32;
            for lost in [0, tail] {
                let graph = run(&Scenario::new(with_par2(&archive))
                    .damaged(0, lost)
                    .global_password(SECRET))
                .await;
                assert_members(&graph, &files);
                assert_fetched_once(&graph);
            }
        }
    }

    // A header posted naming a stored and a coded member otherwise, repaired
    // after the store mapped it and the run made the coded members under it:
    // the new map moves both, and a run under the new map, and the same
    // password, makes the coded ones again.
    #[tokio::test]
    async fn a_repaired_header_moving_members_restarts_the_run_under_the_same_password() {
        // The header is compressed and carries each archive's own salt and
        // IV, so its length can differ by a byte: draw again until the two
        // archives line up.
        let (before, after, files) = (0..64)
            .map(|_| {
                let (before, _) = mixed_under(
                    ["ember.mkv", "quill.mkv", "cinder.nfo", "lumen.mkv"],
                    copy(),
                    under_aes(delta()),
                    false,
                );
                let (after, files) = mixed_under(
                    ["embex.mkv", "quill.mkv", "cindex.nfo", "lumen.mkv"],
                    copy(),
                    under_aes(delta()),
                    false,
                );
                (before, after, files)
            })
            .find(|(before, after, _)| before.len() == after.len())
            .expect("two archives of one length");
        // Each archive draws its own AES salt and IV, so the coded pack
        // differs as well as the header: enough recovery for both.
        let mut posts = vec![Post::plain("lantern.7z", before)];
        posts.extend(par2_set("lantern", &[("lantern.7z", &after)], 16));
        let (scenario, spoken) = hearing(Scenario::new(posts).global_password(SECRET));
        let held = run_holding(&scenario, 2).await;
        assert_eq!(
            held.status,
            Some(JobStatus::Complete),
            "{:?} {:?}",
            held.files.keys(),
            spoken.lock().unwrap()
        );
        let mut names: Vec<String> = files.iter().map(|(name, _)| delivered(name)).collect();
        names.sort();
        assert_eq!(held.files.keys().cloned().collect::<Vec<_>>(), names);
        for (name, bytes) in &files {
            assert!(held.files[&delivered(name)] == *bytes, "{name} bytes");
        }
        assert_runs_made_only_coded(&spoken);
        assert!(closing(&spoken).len() >= 2, "{:?}", spoken.lock().unwrap());
    }
}

// A mixed 7z posted in numbered parts, `lantern.7z.001` onward. The split
// store places each part in its one member where the lengths before it end,
// and the 7z store below reads that member as it reads a 7z posted whole: it
// streams the stored members, shelves the headers and the coded pack, and
// its run decodes the coded folder. Every case runs with the coded folder
// plain, and under AES with the job's password.
mod split {
    use std::ops::Range;

    use weaver_server_pipeline::pipeline::tests::engines::stall_fixtures::waiting_on_held_articles;

    use super::nested::{assert_delivers, delivery, generations, heard, outer_entries, sevenz};
    use super::*;

    const AES: [bool; 2] = [false, true];

    fn sealed(aes: bool) -> (Vec<u8>, Files) {
        match aes {
            false => mixed(MKV, delta()),
            true => mixed_under(MKV, copy(), under_aes(delta()), false),
        }
    }

    fn job(posts: Vec<Post>, aes: bool) -> Scenario {
        let scenario = Scenario::new(posts);
        match aes {
            false => scenario,
            true => scenario.global_password(SECRET),
        }
    }

    // Where the coded pack, the stored member after it and the end header
    // lie in the archive.
    struct Layout {
        pack: Range<usize>,
        lumen: Range<usize>,
        end: Range<usize>,
    }

    fn layout(archive: &[u8], files: &Files) -> Layout {
        let lumen = files[3].1.as_slice();
        let at = archive
            .windows(lumen.len())
            .position(|window| window == lumen)
            .unwrap();
        Layout {
            pack: 32 + files[0].1.len()..at,
            lumen: at..at + lumen.len(),
            end: at + lumen.len()..archive.len(),
        }
    }

    // The archive cut at `cuts`, posted as `lantern.7z.001` onward.
    fn parts(archive: &[u8], cuts: &[usize]) -> Vec<Post> {
        let mut bounds = vec![0];
        bounds.extend_from_slice(cuts);
        bounds.push(archive.len());
        bounds
            .windows(2)
            .zip(1..)
            .map(|(span, number)| {
                Post::plain(
                    &format!("lantern.7z.{number:03}"),
                    archive[span[0]..span[1]].to_vec(),
                )
            })
            .collect()
    }

    // Three parts: the coded pack across the first boundary, the stored
    // member after it across the second.
    fn three(archive: &[u8], files: &Files) -> Vec<Post> {
        let at = layout(archive, files);
        parts(archive, &[at.pack.start + 100, at.lumen.start + 30])
    }

    fn recovery(parts: &[Post], blocks: u32) -> Vec<Post> {
        let files: Vec<(&str, &[u8])> = parts
            .iter()
            .map(|post| (post.name.as_str(), post.bytes.as_slice()))
            .collect();
        par2_set("lantern", &files, blocks)
    }

    // The part holding byte `at` of the archive, and its article there.
    fn article_at(parts: &[Post], at: usize) -> (u32, u32) {
        let mut start = 0;
        for (index, part) in parts.iter().enumerate() {
            if at < start + part.bytes.len() {
                return (index as u32, ((at - start) / ARTICLE) as u32);
            }
            start += part.bytes.len();
        }
        panic!("{at} past the parts")
    }

    // Every article of part `part` lost.
    fn lose_part(scenario: Scenario, parts: &[Post], part: u32) -> Scenario {
        let articles = parts[part as usize].bytes.len().div_ceil(ARTICLE) as u32;
        (0..articles).fold(scenario, |scenario, article| scenario.lost(part, article))
    }

    fn reordered(posts: &[Post], order: &[usize]) -> Vec<Post> {
        order
            .iter()
            .map(|&index| Post::plain(&posts[index].name, posts[index].bytes.clone()))
            .collect()
    }

    // The generations whose run closed a coded member.
    fn closing(spoken: &Spoken) -> BTreeSet<u64> {
        spoken
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, message)| message.contains("MemberClosed"))
            .map(|(generation, _)| *generation)
            .collect()
    }

    fn refused(spoken: &Spoken) -> bool {
        spoken
            .lock()
            .unwrap()
            .iter()
            .any(|(_, message)| message.contains("Refuse"))
    }

    // Only the 7z store keeps a shelf, and it keeps the headers and the
    // coded pack exactly: no stored byte, and nothing of the split store's.
    fn assert_shelf(graph: &Outcome, archive: &[u8], files: &Files) {
        let kept: Vec<(String, u64)> = graph
            .graph()
            .shelved_by
            .values()
            .filter(|(_, kept)| *kept > 0)
            .cloned()
            .collect();
        assert_eq!(
            kept,
            vec![(
                "sevenz_store".to_owned(),
                archive.len() as u64 - stored_bytes(files)
            )]
        );
        assert_shelf_gone(graph);
    }

    // Every member whole, the stored ones written once by the store, the
    // coded ones made by one run that nothing refused, and the shelf exact.
    async fn delivers(scenario: Scenario, archive: &[u8], files: &Files) -> Outcome {
        let (scenario, spoken) = hearing(scenario);
        let graph = run(&scenario).await;
        assert_members(&graph, files);
        assert_fetched_once(&graph);
        assert!(kinds(&graph).contains(&"split"), "{:?}", kinds(&graph));
        assert_runs_made_only_coded(&spoken);
        assert_eq!(closing(&spoken).len(), 1, "{:?}", spoken.lock().unwrap());
        assert!(!refused(&spoken), "{:?}", spoken.lock().unwrap());
        assert_shelf(&graph, archive, files);
        graph
    }

    // Held until nothing moves, then repaired: every member whole, and no
    // run named a stored member.
    async fn repaired_holding(scenario: Scenario, held: u32, files: &Files) -> Spoken {
        let (scenario, spoken) = hearing(scenario);
        let held = run_holding(&scenario, held).await;
        assert_eq!(
            held.status,
            Some(JobStatus::Complete),
            "{:?} {:?}",
            held.files.keys(),
            spoken.lock().unwrap()
        );
        let mut names: Vec<String> = files.iter().map(|(name, _)| delivered(name)).collect();
        names.sort();
        assert_eq!(held.files.keys().cloned().collect::<Vec<_>>(), names);
        for (name, bytes) in files {
            assert!(held.files[&delivered(name)] == *bytes, "{name} bytes");
        }
        assert_runs_made_only_coded(&spoken);
        spoken
    }

    async fn repaired(scenario: Scenario, files: &Files) {
        let (scenario, spoken) = hearing(scenario);
        let graph = run(&scenario).await;
        assert_members(&graph, files);
        assert_fetched_once(&graph);
        assert_runs_made_only_coded(&spoken);
    }

    // 1. A boundary anywhere: through the coded pack, through a stored
    // member, through the end header with its last bytes alone in the last
    // part, through the start header, and twelve small parts.
    #[tokio::test]
    async fn a_mixed_7z_cut_anywhere_delivers_every_member() {
        for aes in AES {
            let (archive, files) = sealed(aes);
            let at = layout(&archive, &files);
            let cases = [
                ("pack", vec![at.pack.start + 100]),
                ("stored", vec![32 + 100, at.lumen.start + 30]),
                (
                    "end header",
                    vec![at.pack.start + 50, at.end.start + at.end.len() / 2],
                ),
                ("start header", vec![10]),
                ("twelve", {
                    let chunk = archive.len().div_ceil(12);
                    (1..12).map(|part| part * chunk).collect()
                }),
            ];
            for (case, cuts) in cases {
                println!("{case}, aes {aes}");
                delivers(job(parts(&archive, &cuts), aes), &archive, &files).await;
            }
        }
    }

    // 2. The last part posted first, or a middle part last: the store
    // places each where it lies and delivers as in order.
    #[tokio::test]
    async fn parts_out_of_order_deliver_every_member() {
        for aes in AES {
            let (archive, files) = sealed(aes);
            let posted = three(&archive, &files);
            for order in [[2, 1, 0], [0, 2, 1]] {
                println!("{order:?}, aes {aes}");
                delivers(job(reordered(&posted, &order), aes), &archive, &files).await;
            }
        }
    }

    // 2. The part holding the middle of the coded pack out with the server
    // but for its first article, which declares its length and so places
    // every part after it: the store maps, and the run reads and waits on
    // its shelf for the bytes still out, asks nothing and fails nothing,
    // then decodes once they come. (A part none of whose articles came
    // places nothing after it: the split store holds those bytes until one
    // declares the part's length, so the 7z store sees no end header.)
    #[tokio::test]
    async fn a_run_waits_on_a_part_still_out() {
        for aes in AES {
            let (archive, files) = sealed(aes);
            let at = layout(&archive, &files);
            let posts = parts(&archive, &[at.pack.start + 20, at.pack.end - 20]);
            let (scenario, spoken) = hearing(job(posts, aes));
            let held =
                waiting_on_held_articles(&scenario, |(part, article)| part == 1 && article > 0)
                    .await;
            assert_eq!(held.status, Some(JobStatus::Complete), "aes {aes}");
            for (name, bytes) in &files {
                assert!(held.files[&delivered(name)] == *bytes, "{name} bytes");
            }
            assert_runs_made_only_coded(&spoken);
            assert_eq!(closing(&spoken).len(), 1);
            assert!(!refused(&spoken), "{:?}", spoken.lock().unwrap());
        }
    }

    // 3. A byte of the coded pack wrong as posted in the second part,
    // under a PAR2 set over the parts held until nothing moves: the run
    // reads the damage and ends, the set rebuilds the part, and the run
    // starts over and makes the coded members; no run names a stored one.
    #[tokio::test]
    async fn a_coded_pack_damaged_in_one_part_is_repaired_after_its_run_read_it() {
        for aes in AES {
            let (archive, files) = sealed(aes);
            let at = layout(&archive, &files);
            let written = three(&archive, &files);
            let (part, _) = article_at(&written, at.pack.start + 150);
            assert_eq!(part, 1);
            let mut posts = reordered(&written, &[0, 1, 2]);
            posts[1].bytes[50] ^= 0x5a;
            posts.extend(recovery(&written, 4));
            let spoken = repaired_holding(job(posts, aes), 4, &files).await;
            assert_eq!(closing(&spoken).len(), 1, "aes {aes}");
        }
    }

    // 3. Damage the set repairs as the parts come: an article of a stored
    // member, of the end header, the articles either side of a boundary in
    // the coded pack, and a whole part.
    #[tokio::test]
    async fn damage_in_any_part_is_repaired() {
        for aes in AES {
            let (archive, files) = sealed(aes);
            let at = layout(&archive, &files);
            let written = three(&archive, &files);
            let mut posts = reordered(&written, &[0, 1, 2]);
            posts.extend(recovery(&written, 8));
            let boundary = written[0].bytes.len();
            assert!(at.pack.contains(&(boundary - ARTICLE)) && at.pack.contains(&(boundary + 1)));
            let stored = article_at(&written, 32 + 100);
            let header = article_at(&written, at.end.start + 1);
            let before = article_at(&written, boundary - 1);
            let after = article_at(&written, boundary);
            let base = || {
                job(
                    reordered(&posts, &(0..posts.len()).collect::<Vec<_>>()),
                    aes,
                )
            };
            println!("stored, aes {aes}");
            repaired(base().damaged(stored.0, stored.1), &files).await;
            println!("end header, aes {aes}");
            repaired(base().damaged(header.0, header.1), &files).await;
            println!("boundary, aes {aes}");
            repaired(
                base().damaged(before.0, before.1).damaged(after.0, after.1),
                &files,
            )
            .await;
            println!("whole part, aes {aes}");
            repaired(lose_part(base(), &written, 1), &files).await;
        }
    }

    // 4. A whole part lost with no recovery: the set is not a file, as for a
    // split 7z that stores everything. The part holding only the coded pack,
    // and the part holding only the end header: each ends as the same cut of
    // an all-stored 7z of the same files does.
    #[tokio::test]
    async fn a_part_lost_beyond_repair_ends_as_an_all_stored_split_does() {
        for aes in AES {
            let (mixed, files) = sealed(aes);
            // The reference stores every folder plain: a folder of two
            // members under AES and a copy is not the store's, so an
            // all-stored set under AES goes to an engine.
            let (stored, _) = mixed_under(MKV, copy(), copy(), false);
            for (case, part) in [("pack", 1), ("end header", 2)] {
                let mut ended = Vec::new();
                for (archive, aes) in [(&mixed, aes), (&stored, false)] {
                    let at = layout(archive, &files);
                    let cuts = match case {
                        "pack" => vec![at.pack.start + 10, at.pack.end - 10],
                        _ => vec![at.pack.start + 10, at.end.start],
                    };
                    let posts = parts(archive, &cuts);
                    let graph =
                        run(&lose_part(job(parts(archive, &cuts), aes), &posts, part)).await;
                    // Failed for the share lost or for the set: the share
                    // differs with the archive's length, so the kind is
                    // compared, with what was delivered.
                    let kind = match graph.status {
                        Some(JobStatus::Failed { .. }) => "failed",
                        Some(JobStatus::Partial) => "partial",
                        Some(JobStatus::Complete) => "complete",
                        _ => "other",
                    };
                    ended.push((kind, graph.files.keys().cloned().collect::<Vec<_>>()));
                }
                println!("{case}, aes {aes}: {ended:?}");
                assert_eq!(ended[0], ended[1], "{case}, aes {aes}");
            }
        }
    }

    // 5. Parts posted under names that number nothing, the PAR2 set last:
    // the set names each part, and the store places it.
    #[tokio::test]
    async fn obfuscated_parts_named_by_the_set_deliver_every_member() {
        for aes in AES {
            let (archive, files) = sealed(aes);
            let written = three(&archive, &files);
            let mut posts: Vec<Post> = written
                .iter()
                .zip(0u32..)
                .map(|(post, index)| {
                    Post::plain(&format!("{:032x}", 0x5eed_0000 + index), post.bytes.clone())
                })
                .collect();
            posts.swap(0, 1);
            posts.extend(recovery(&written, 4));
            repaired(job(posts, aes), &files).await;
        }
    }

    // 5. The second and third parts posted under each other's numbers, the
    // PAR2 set naming each: the set names them again and the store places
    // them where they lie.
    #[tokio::test]
    async fn misnumbered_parts_named_by_the_set_deliver_every_member() {
        for aes in AES {
            let (archive, files) = sealed(aes);
            let written = three(&archive, &files);
            let mut posts = reordered(&written, &[0, 1, 2]);
            posts[1].name = written[2].name.clone();
            posts[2].name = written[1].name.clone();
            posts.extend(recovery(&written, 4));
            repaired(job(posts, aes), &files).await;
        }
    }

    // 5. Two parts of one length inside the first stored member posted
    // under each other's numbers, the PAR2 index that names them held until
    // nothing moves: the store maps and the run makes the coded members
    // first, and then the index names the parts again and they move.
    #[tokio::test]
    async fn parts_named_again_after_the_run_started_deliver_every_member() {
        for aes in AES {
            let (archive, files) = sealed(aes);
            let written = parts(
                &archive,
                &[32 + ARTICLE, 32 + 2 * ARTICLE, 32 + 3 * ARTICLE],
            );
            let mut posts = reordered(&written, &[0, 1, 2, 3]);
            posts[1].name = written[2].name.clone();
            posts[2].name = written[1].name.clone();
            posts.push(recovery(&written, 1).swap_remove(0));
            let (scenario, spoken) = hearing(job(posts, aes));
            let held = run_holding(&scenario, 4).await;
            assert_eq!(
                held.status,
                Some(JobStatus::Complete),
                "aes {aes}: {:?} {:?}",
                held.files.keys(),
                spoken.lock().unwrap()
            );
            for (name, bytes) in &files {
                assert!(held.files[&delivered(name)] == *bytes, "{name} bytes");
            }
            assert!(!closing(&spoken).is_empty(), "{:?}", spoken.lock().unwrap());
        }
    }

    // 6. Taken down and restored after every article.
    #[tokio::test]
    async fn restarted_after_every_article_delivers_every_member() {
        for aes in AES {
            let (archive, files) = sealed(aes);
            let whole = restart_row(job(three(&archive, &files), aes)).await;
            assert_members(&whole, &files);
        }
    }

    // 7. A zip a coded member of a split mixed 7z, a byte of the coded pack
    // wrong as posted in the second part: the run reads it and ends, the
    // PAR2 set over the parts rebuilds it, and the run starts over, the zip
    // below starting over on what it makes.
    #[tokio::test]
    async fn a_zip_coded_in_a_split_mixed_7z_starts_over_when_the_run_does() {
        for aes in AES {
            let entries = outer_entries();
            let coded = match aes {
                false => delta(),
                true => under_aes(delta()),
            };
            let archive = sevenz(&[
                (copy(), vec![entries[0].clone()]),
                (coded, vec![entries[1].clone(), entries[2].clone()]),
            ]);
            let at = 32 + entries[0].1.len() + 100 + ARTICLE;
            let written = parts(
                &archive,
                &[32 + entries[0].1.len() + 50, archive.len() - 100],
            );
            let (part, _) = article_at(&written, at);
            assert_eq!(part, 1);
            let mut posts = reordered(&written, &[0, 1, 2]);
            posts[1].bytes[at - written[0].bytes.len()] ^= 0x5a;
            posts.extend(recovery(&written, 4));
            let (scenario, heard) = heard(job(posts, aes));
            let held = run_holding(&scenario, 4).await;
            assert_eq!(held.status, Some(JobStatus::Complete), "aes {aes}");
            assert_delivers(&held.files, &delivery());
            let ran = generations(&heard);
            assert_eq!(ran.len(), 1, "only the 7z runs: {ran:?}");
            assert!(
                ran.values().all(|ran| ran.len() >= 2),
                "the run ran again once repaired: {ran:?}"
            );
        }
    }

    // 8. The same files posted whole or in parts, and in parts stored,
    // mixed or coded, deliver alike.
    #[tokio::test]
    async fn whole_or_split_stored_mixed_or_coded_deliver_alike() {
        for aes in AES {
            let (mixed, files) = sealed(aes);
            let (seal, coded) = match aes {
                false => (copy(), lzma2()),
                true => (aes_copy(), under_aes(lzma2())),
            };
            let stored = sevenz(&[
                (seal.clone(), vec![(files[0].0, files[0].1.clone())]),
                (seal.clone(), vec![(files[1].0, files[1].1.clone())]),
                (seal.clone(), vec![(files[2].0, files[2].1.clone())]),
                (seal, vec![(files[3].0, files[3].1.clone())]),
            ]);
            let all: Vec<(&str, Vec<u8>)> = files.iter().map(|(n, b)| (*n, b.clone())).collect();
            let compressed = sevenz(&[(coded, all)]);
            let whole = run(&job(vec![Post::plain("lantern.7z", mixed.clone())], aes)).await;
            assert_members(&whole, &files);
            for (case, archive) in [
                ("stored", stored),
                ("mixed", mixed.clone()),
                ("coded", compressed),
            ] {
                let graph = run(&job(sevenz_fixtures_split(&archive), aes)).await;
                assert_eq!(
                    graph.status, whole.status,
                    "{case}, aes {aes}: {:?}",
                    graph.failure
                );
                assert_eq!(graph.files, whole.files, "{case}, aes {aes}");
            }
        }
    }

    fn sevenz_fixtures_split(archive: &[u8]) -> Vec<Post> {
        super::super::sevenz_fixtures::split(archive, 3)
    }

    // The parts stored in one zip, posted as one file: the zip names its
    // members only when its central directory, its last article, arrives,
    // and then hands every part down in that one step. The 7z store below
    // reads its map from the last part; what it took of the earlier parts
    // before that was held for it, and is handed to it again under the map.
    mod in_zip {
        use std::io::Write;

        use super::*;

        fn zipped(parts: &[Post]) -> Vec<Post> {
            let mut writer = ::zip::ZipWriter::new(Cursor::new(Vec::new()));
            let stored = ::zip::write::SimpleFileOptions::default()
                .compression_method(::zip::CompressionMethod::Stored);
            for part in parts {
                writer.start_file(part.name.as_str(), stored).unwrap();
                writer.write_all(&part.bytes).unwrap();
            }
            vec![Post::plain(
                "Ember.zip",
                writer.finish().unwrap().into_inner(),
            )]
        }

        // A 7z that stores every member in one folder, in 2, 3 and 12
        // parts: the store places every member, and no engine reads it.
        #[tokio::test]
        async fn an_all_stored_7z_in_parts_inside_a_stored_zip_delivers_every_member() {
            let files = mixed(MKV, delta()).1;
            let archive = sevenz(&[(copy(), files.clone())]);
            for count in [2, 3, 12] {
                let parts = super::super::super::sevenz_fixtures::split(&archive, count);
                let graph = run(&Scenario::new(zipped(&parts))).await;
                assert_members(&graph, &files);
                assert_fetched_once(&graph);
                for kind in ["zip", "split", "sevenz_store"] {
                    assert!(
                        kinds(&graph).contains(&kind),
                        "{count} parts: {:?}",
                        kinds(&graph)
                    );
                }
            }
        }

        // The mixed 7z, plain or AES-coded, in three parts cut through its
        // coded pack and its stored member: every member, the stored ones
        // written once by the store, the coded ones by one run that nothing
        // refused. The shelves are exact: the zip's keeps the whole post
        // until its central directory names the parts, the 7z store's its
        // headers and coded pack, and the split keeps none.
        #[tokio::test]
        async fn a_mixed_7z_in_parts_inside_a_stored_zip_delivers_every_member() {
            for aes in AES {
                let (archive, files) = sealed(aes);
                let posts = zipped(&three(&archive, &files));
                let zip = posts[0].bytes.len() as u64;
                let (scenario, spoken) = hearing(job(posts, aes));
                let graph = run(&scenario).await;
                assert_members(&graph, &files);
                assert_fetched_once(&graph);
                assert!(kinds(&graph).contains(&"split"), "{:?}", kinds(&graph));
                assert_runs_made_only_coded(&spoken);
                assert_eq!(closing(&spoken).len(), 1, "{:?}", spoken.lock().unwrap());
                assert!(!refused(&spoken), "{:?}", spoken.lock().unwrap());
                let kept: Vec<(String, u64)> = graph
                    .graph()
                    .shelved_by
                    .values()
                    .filter(|(_, kept)| *kept > 0)
                    .cloned()
                    .collect();
                assert_eq!(
                    kept,
                    vec![
                        ("zip".to_owned(), zip),
                        (
                            "sevenz_store".to_owned(),
                            archive.len() as u64 - stored_bytes(&files)
                        ),
                    ],
                    "aes {aes}"
                );
                assert_shelf_gone(&graph);
            }
        }

        // A RAR's later volume stored in the zip before its first: the RAR
        // store takes it before the first volume's headers say where its
        // bytes go, in the same step, and every file is still delivered.
        #[tokio::test]
        async fn a_rar_whose_later_volume_reaches_it_first_delivers_every_file() {
            let files = mixed(MKV, delta()).1;
            let volumes = crate::pipeline::tests::direct_store::multi_member_store_set(&files, 3);
            let parts: Vec<Post> = volumes
                .iter()
                .rev()
                .map(|(name, bytes)| Post::plain(name, bytes.clone()))
                .collect();
            let graph = run(&Scenario::new(zipped(&parts))).await;
            assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
            assert_eq!(graph.files.len(), files.len(), "{:?}", graph.files.keys());
            for (name, bytes) in &files {
                assert!(
                    graph
                        .files
                        .iter()
                        .any(|(delivered, made)| delivered.ends_with(name) && made == bytes),
                    "{name}: {:?}",
                    graph.files.keys()
                );
            }
            assert_fetched_once(&graph);
        }
    }
}
