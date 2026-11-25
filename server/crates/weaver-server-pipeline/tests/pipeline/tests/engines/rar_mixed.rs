// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR set that is not solid and stores a file before it compresses one.
// The store streams the stored members as they arrive and keeps only the
// headers and the compressed member's packed data on its shelf; its run
// reads that shelf and decodes the compressed member alone. A repair of
// the packed data restarts the run, which makes the member again and leaves
// the stored ones as the store wrote them; a repair of a stored member
// replays it through the store. Packed data nothing can rebuild costs the
// compressed member only. Every case runs for RAR5 and RAR4.
//
// Nothing in the tree compresses RAR: a compressed member is packed data
// RAR itself wrote, lifted from a fixture and written again under the test
// writer's headers.

use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use super::harness::{
    ARTICLE, Outcome, Post, Scenario, assert_fetched_once, kinds, par2_set, payload, run,
    run_holding,
};
use super::rar_fixtures::delivered;
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use weaver_objects::rar_store::writer::{Item, Options, Version, lifted, literal, write};
use weaver_server_pipeline::pipeline::tests::engines::restart_fixtures::restart_row;

type Files = Vec<(&'static str, Vec<u8>)>;

const VERSIONS: [Version; 2] = [Version::Rar5, Version::Rar4];

// The three members, in the archive's order: a stored file, the compressed
// one, and a stored file.
const MKV: [&str; 3] = ["alpha.mkv", "notes.nfo", "beta.mkv"];
const BIN: [&str; 3] = ["alpha.bin", "notes.nfo", "beta.bin"];

// The store numbers members as the archive does: the compressed one is the
// second.
const MADE: u32 = 1;

const SECRET: &str = "ember-harbor-secret";

macro_rules! profile {
    ($name:literal) => {
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../weaver-server-core/tests/fixtures/extraction_profiles/",
            $name
        ))
    };
}

// What RAR packed: a small member (64 KiB of zeros in a few dozen bytes),
// or a large one (a text in some 41 KB, hundreds of articles of packed
// data).
#[derive(Clone, Copy, Debug)]
enum Size {
    Small,
    Large,
}

fn packed(version: Version, size: Size, name: &'static str) -> (Item, Vec<u8>) {
    let (fixture, member): (&[u8], &str) = match (version, size) {
        (Version::Rar5, Size::Small) => (profile!("rar5_multifile_lz.rar"), "zeros_64k.bin"),
        (Version::Rar4, Size::Small) => (profile!("rar4_multifile_lz.rar"), "zeros_64k.bin"),
        (Version::Rar5, Size::Large) => (profile!("rar5_lz.rar"), "compressible.txt"),
        (Version::Rar4, Size::Large) => (profile!("rar4_lz.rar"), "compressible.txt"),
    };
    let (plain, packed, codec) = lifted(fixture, member);
    (
        Item::Packed {
            name,
            plain: plain.clone(),
            packed,
            codec,
        },
        plain,
    )
}

// `names[0]` stored, `names[1]` compressed, `names[2]` stored.
fn mixed_items(version: Version, size: Size, names: [&'static str; 3]) -> (Vec<Item>, Files) {
    let (made, plain) = packed(version, size, names[1]);
    let files: Files = vec![
        (names[0], payload(5 * ARTICLE + 11, 41)),
        (names[1], plain),
        (names[2], payload(4 * ARTICLE, 44)),
    ];
    let items = vec![
        Item::File(names[0], files[0].1.clone()),
        made,
        Item::File(names[2], files[2].1.clone()),
    ];
    (items, files)
}

fn named_volumes(volumes: Vec<Vec<u8>>) -> Vec<(String, Vec<u8>)> {
    let many = volumes.len() > 1;
    volumes
        .into_iter()
        .enumerate()
        .map(|(index, bytes)| {
            let name = if many {
                format!("silver.horizon.part{:02}.rar", index + 1)
            } else {
                "silver.horizon.rar".to_owned()
            };
            (name, bytes)
        })
        .collect()
}

fn mixed(version: Version, size: Size, names: [&'static str; 3], options: &Options) -> Set {
    let (items, files) = mixed_items(version, size, names);
    assert_eq!(options.version, version);
    Set {
        volumes: named_volumes(write(&items, options)),
        files,
    }
}

struct Set {
    volumes: Vec<(String, Vec<u8>)>,
    files: Files,
}

impl Set {
    fn posts(&self) -> Vec<Post> {
        self.volumes
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect()
    }

    // The volumes as posted, then a PAR2 set over `source`'s volumes.
    fn with_par2_of(&self, source: &Set) -> Vec<Post> {
        let files: Vec<(&str, &[u8])> = source
            .volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let mut posts = self.posts();
        posts.extend(par2_set("silver.horizon", &files, 4));
        posts
    }

    fn with_par2(&self) -> Vec<Post> {
        self.with_par2_of(self)
    }

    // The post that carries the PAR2 set's recovery blocks.
    fn recovery_post(&self) -> u32 {
        self.volumes.len() as u32 + 1
    }

    fn total(&self) -> u64 {
        self.volumes
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum()
    }

    fn stored_bytes(&self) -> u64 {
        (self.files[0].1.len() + self.files[2].1.len()) as u64
    }

    // Where `name`'s data lies in each volume that holds a part of it, as
    // its headers say, read under `password`.
    fn data_of(&self, name: &str, password: Option<&str>) -> Vec<(u32, Range<usize>)> {
        self.volumes
            .iter()
            .enumerate()
            .filter_map(|(index, (_, bytes))| {
                let facts = unrar_rs::RarArchive::parse_volume_facts_walk_with_shared_kdf_cache(
                    std::io::Cursor::new(bytes.clone()),
                    password,
                    Arc::new(unrar_rs::KdfCache::new()),
                )
                .expect("a RAR volume")
                .facts;
                let part = facts.members.iter().find(|part| part.name == name)?;
                let start = part.data_offset as usize;
                Some((index as u32, start..start + part.data_size as usize))
            })
            .collect()
    }
}

fn options(version: Version) -> Options {
    Options::new(version)
}

fn split(version: Version, budget: usize) -> Options {
    let mut options = Options::new(version);
    options.split = Some(budget);
    options.numbered = true;
    options
}

// Every file enciphered under `SECRET` but the first, which stays in the
// clear: a stored file in the clear, the compressed one enciphered, a
// stored file enciphered.
fn sealed(version: Version) -> Options {
    let mut options = Options::new(version);
    options.password = Some(SECRET);
    options.clear = vec![0];
    options
}

// Every file and every header enciphered.
fn sealed_headers(version: Version) -> Options {
    let mut options = Options::new(version);
    options.password = Some(SECRET);
    options.encrypt_headers = true;
    options
}

// The first article wholly inside `range`.
fn article_in(range: Range<usize>) -> u32 {
    let article = range.start.div_ceil(ARTICLE);
    assert!((article + 1) * ARTICLE <= range.end, "{range:?}");
    article as u32
}

// Every member, every byte as written, and nothing else; no whole engine.
fn assert_members(label: &str, outcome: &Outcome, files: &Files) {
    assert!(
        outcome.completed(),
        "{label}: {:?} {:?} {:?}",
        outcome.status,
        outcome.failure,
        outcome.files.keys()
    );
    assert_files(label, &outcome.files, files);
    assert!(
        !kinds(outcome).contains(&"rar_engine"),
        "{label}: the store kept the set: {:?}",
        kinds(outcome)
    );
}

fn assert_files(
    label: &str,
    got: &std::collections::BTreeMap<String, Vec<u8>>,
    files: &[(&str, Vec<u8>)],
) {
    let mut names: Vec<String> = files.iter().map(|(name, _)| delivered(name)).collect();
    names.sort();
    assert_eq!(
        got.keys().cloned().collect::<Vec<_>>(),
        names,
        "{label}: delivered names"
    );
    for (name, bytes) in files {
        assert!(got[&delivered(name)] == *bytes, "{label}: {name} bytes");
    }
}

// What the job's runs said, by generation.
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

// No run named, made, wrote or retracted a stored member, and no run
// announced one: the store wrote each once, as it streamed. The harness
// hears a run's mail only while the job waits on it (`run`), so what it
// heard is a part of what the runs said: this asserts on what was heard,
// never on how many runs there were.
fn assert_runs_named_no_stored_member(label: &str, spoken: &Spoken) {
    let (members, _) = named(spoken);
    assert!(
        members.is_subset(&BTreeSet::from([MADE])),
        "{label}: the members runs named: {members:?}"
    );
    assert!(
        !spoken
            .lock()
            .unwrap()
            .iter()
            .any(|(_, message)| message.starts_with("Emit(Member {")),
        "{label}: a scoped run announces nothing"
    );
}

// The RAR store kept a shelf on disk during the job, and no file of it is
// left once the job ended.
fn assert_shelf_gone(label: &str, graph: &Outcome) {
    let shelves: Vec<String> = graph
        .graph()
        .shelf_files
        .iter()
        .filter(|(node, _)| {
            graph
                .graph()
                .shelved_by
                .get(node)
                .is_some_and(|(kind, _)| kind == "rar_store")
        })
        .flat_map(|(_, files)| files)
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            format!("{}/{name}", weaver_graph::names::INTERNAL_DIR)
        })
        .collect();
    assert!(!shelves.is_empty(), "{label}: the store kept a shelf");
    for shelf in &shelves {
        assert!(
            !graph
                .files
                .keys()
                .chain(graph.retained_files.keys())
                .any(|name| name.ends_with(shelf.as_str())),
            "{label}: {shelf} left"
        );
    }
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

fn assert_demoted(label: &str, graph: &Outcome, first: &str, reason: &str) {
    assert_eq!(
        demoted(graph),
        vec![(first.to_owned(), "rar_store".to_owned(), reason.to_owned())],
        "{label}: {:?}",
        graph.events
    );
}

// One volume; the packed data across a volume boundary; ten volumes, every
// file spanning some; and the sets RAR itself wrote. The store streams the
// stored members and shelves the headers and the packed data alone.
#[tokio::test]
async fn a_mixed_rar_streams_its_stored_members_and_shelves_only_the_rest() {
    for version in VERSIONS {
        for (label, size, options) in [
            ("one volume", Size::Small, options(version)),
            (
                "packed across a boundary",
                Size::Large,
                split(version, 30_000),
            ),
            ("ten volumes", Size::Large, split(version, 4_400)),
        ] {
            let label = format!("{version:?} {label}");
            let set = mixed(version, size, MKV, &options);
            if label.ends_with("ten volumes") {
                assert!(set.volumes.len() >= 10, "{}", set.volumes.len());
            }
            let (scenario, spoken) = hearing(Scenario::new(set.posts()));
            let graph = run(&scenario).await;
            assert_members(&label, &graph, &set.files);
            assert_fetched_once(&graph);
            assert_runs_named_no_stored_member(&label, &spoken);
            assert_eq!(
                graph.graph().shelved,
                set.total() - set.stored_bytes(),
                "{label}: the shelf keeps the headers and the packed data, no stored byte"
            );
            assert_shelf_gone(&label, &graph);
        }
    }
    for (name, fixture) in [
        (
            "rar5_multifile_lz.rar",
            &profile!("rar5_multifile_lz.rar")[..],
        ),
        (
            "rar4_multifile_lz.rar",
            &profile!("rar4_multifile_lz.rar")[..],
        ),
    ] {
        let graph = run(&Scenario::new(vec![Post::plain(name, fixture.to_vec())])).await;
        assert!(graph.completed(), "{name}: {:?}", graph.failure);
        assert!(!kinds(&graph).contains(&"rar_engine"), "{name}");
        assert_eq!(graph.files[&delivered("zeros_64k.bin")], vec![0; 65_536]);
        assert_eq!(
            graph.graph().shelved,
            fixture.len() as u64 - 14 - 25,
            "{name}: hello.txt and second.txt are streamed"
        );
    }
}

// A set whose last volume has no end record, in one volume and in several:
// the store streams the stored members, and its run starts once nothing
// more can join, as the whole engine's run ends then, and makes the
// compressed member.
#[tokio::test]
async fn a_mixed_rar_with_no_end_record_runs_once_nothing_more_can_join() {
    for version in VERSIONS {
        no_end_record(
            &format!("{version:?} one volume"),
            version,
            Size::Small,
            options(version),
        )
        .await;
    }
}

// The same set in volumes: the face appears only when the job goes quiet
// with every volume closed, and a quiet that changes nothing but the face
// steps no run.
#[tokio::test]
async fn a_mixed_rar_in_volumes_with_no_end_record_runs_once_nothing_more_can_join() {
    for version in VERSIONS {
        no_end_record(
            &format!("{version:?} in volumes"),
            version,
            Size::Large,
            split(version, 30_000),
        )
        .await;
    }
}

async fn no_end_record(label: &str, version: Version, size: Size, mut options: Options) {
    options.ended = false;
    let set = mixed(version, size, MKV, &options);
    let (scenario, spoken) = hearing(Scenario::new(set.posts()));
    let graph = run(&scenario).await;
    assert_members(label, &graph, &set.files);
    assert_fetched_once(&graph);
    assert_runs_named_no_stored_member(label, &spoken);
    assert!(!kinds(&graph).contains(&"rar_engine"), "{label}");
    assert_shelf_gone(label, &graph);
}

// A set with no end record, in one volume and in numbered volumes of up to
// `budget` bytes: what each case runs under.
fn no_end(version: Version, mut options: Options, budget: usize) -> [(&'static str, Options); 2] {
    options.ended = false;
    let mut volumes = split(version, budget);
    volumes.password = options.password;
    volumes.clear = options.clone().clear;
    volumes.ended = false;
    [("one volume", options), ("volumes", volumes)]
}

// The set with a byte of the compressed member's packed data wrong in the
// post itself, and the post its first part lies in.
fn packed_wrong(version: Version, options: &Options, password: Option<&str>) -> (Set, Vec<Post>) {
    let set = mixed(version, Size::Large, MKV, options);
    let (volume, at) = set.data_of(MKV[1], password).remove(0);
    let mut posts = set.posts();
    posts[volume as usize].bytes[at.start + at.len() / 2] ^= 0x5a;
    (set, posts)
}

// The stored members delivered whole, the set kept beside them as a
// Partial job keeps it, and the compressed member not delivered.
fn assert_stored_delivered(label: &str, graph: &Outcome, set: &Set) {
    for (name, bytes) in [&set.files[0], &set.files[2]] {
        assert!(
            graph.files.get(&delivered(name)) == Some(bytes),
            "{label}: {name} delivered whole: {:?}",
            graph.files.keys()
        );
    }
    assert!(
        !graph.files.contains_key(&delivered(MKV[1])),
        "{label}: {:?}",
        graph.files.keys()
    );
}

// No end record, a byte of the packed data wrong as posted and no PAR2: the
// run the store owes once nothing more can join reads it and ends, and the
// job ends Partial with the stored members delivered whole.
#[tokio::test]
async fn no_end_record_packed_data_wrong_without_par2_delivers_the_stored_members() {
    for version in VERSIONS {
        for (case, options) in no_end(version, options(version), 30_000) {
            let label = format!("{version:?} {case}");
            let (set, posts) = packed_wrong(version, &options, None);
            assert_eq!(set.volumes.len() > 1, case == "volumes", "{label}");
            let (scenario, spoken) = hearing(Scenario::new(posts));
            let graph = run(&scenario).await;
            assert_eq!(
                graph.status,
                Some(JobStatus::Partial),
                "{label}: {:?} {:?}",
                graph.failure,
                graph.files.keys()
            );
            assert_stored_delivered(&label, &graph, &set);
            assert_fetched_once(&graph);
            assert_runs_named_no_stored_member(&label, &spoken);
            assert_shelf_gone(&label, &graph);
        }
    }
}

// The same under a PAR2 set over the set as written: the repair rebuilds
// the packed data, the run makes the member, and the job is Complete.
#[tokio::test]
async fn no_end_record_packed_data_wrong_under_par2_is_repaired_complete() {
    for version in VERSIONS {
        for (case, options) in no_end(version, options(version), 30_000) {
            let label = format!("{version:?} {case}");
            let (set, wrong) = packed_wrong(version, &options, None);
            let mut posts = set.with_par2();
            for (post, bytes) in posts.iter_mut().zip(wrong) {
                *post = bytes;
            }
            let (scenario, spoken) = hearing(Scenario::new(posts));
            let graph = run(&scenario).await;
            assert_members(&label, &graph, &set.files);
            assert_eq!(graph.status, Some(JobStatus::Complete), "{label}");
            assert_fetched_once(&graph);
            assert_runs_named_no_stored_member(&label, &spoken);
        }
    }
}

// No end record, the compressed member enciphered under `-p` and the stored
// ones in the clear, with no candidate that opens it: the run the store
// owes asks, nothing answers, and the job ends Partial for the password
// with the stored members delivered whole.
#[tokio::test]
async fn no_end_record_with_no_right_password_delivers_the_stored_members() {
    for version in VERSIONS {
        let mut sealed = options(version);
        sealed.password = Some(SECRET);
        sealed.clear = vec![0, 2];
        for (case, options) in no_end(version, sealed, 3 * ARTICLE) {
            let label = format!("{version:?} {case}");
            let set = mixed(version, Size::Small, MKV, &options);
            assert_eq!(set.volumes.len() > 1, case == "volumes", "{label}");
            let (scenario, spoken) = hearing(Scenario::new(set.posts()).password("ember-harbour"));
            let graph = run(&scenario).await;
            assert_eq!(
                graph.status,
                Some(JobStatus::Partial),
                "{label}: {:?} {:?}",
                graph.failure,
                graph.files.keys()
            );
            assert_eq!(
                graph.failure_kind,
                Some(HistoryFailureKind::Password),
                "{label}: {:?}",
                graph.failure
            );
            assert_stored_delivered(&label, &graph, &set);
            assert_fetched_once(&graph);
            assert_runs_named_no_stored_member(&label, &spoken);
        }
    }
}

// Two compressed members, each after a stored one: the store runs only once
// its layout holds every header, so its run makes both, and no run names a
// stored member. One volume, which is no set and says so in its main header
// before the headers after it, and numbered volumes.
#[tokio::test]
async fn a_mixed_rar_with_two_compressed_members_makes_both() {
    for version in VERSIONS {
        for (case, options) in [
            ("one volume", options(version)),
            ("volumes", split(version, 3 * ARTICLE)),
        ] {
            let label = format!("{version:?} {case}");
            let files: Files = vec![
                ("alpha.mkv", payload(4 * ARTICLE + 5, 41)),
                ("notes.nfo", payload(3 * ARTICLE + 1, 45)),
                ("beta.mkv", payload(3 * ARTICLE, 44)),
                ("extra.nfo", payload(2 * ARTICLE + 7, 46)),
            ];
            let items = vec![
                Item::File(files[0].0, files[0].1.clone()),
                literal(version, files[1].0, &files[1].1),
                Item::File(files[2].0, files[2].1.clone()),
                literal(version, files[3].0, &files[3].1),
            ];
            let volumes = named_volumes(write(&items, &options));
            assert_eq!(volumes.len() > 1, case == "volumes", "{label}");
            let posts = volumes
                .iter()
                .map(|(name, bytes)| Post::plain(name, bytes.clone()))
                .collect();
            let (scenario, spoken) = hearing(Scenario::new(posts));
            let graph = run(&scenario).await;
            assert_members(&label, &graph, &files);
            assert_fetched_once(&graph);
            let (members, _) = named(&spoken);
            assert!(
                members.is_subset(&BTreeSet::from([1, 3])),
                "{label}: the members runs named: {members:?}"
            );
        }
    }
}

// A byte of the packed data wrong in the post itself, under a PAR2 set over
// the set as written: every article arrives intact, so only the member's
// own checksum or its decode finds it. The recovery volume is held until
// nothing moves, so the run has read the damage and ended before the
// repair. Once it comes, the repair rebuilds the packed data and the run
// decodes the member again; the stored members are the store's, and no run
// names them.
#[tokio::test]
async fn packed_data_repaired_by_par2_after_its_run_read_it_restarts_the_run_alone() {
    for version in VERSIONS {
        let label = format!("{version:?}");
        let set = mixed(version, Size::Small, MKV, &options(version));
        let mut posted = set.posts();
        let (_, at) = set.data_of(MKV[1], None).remove(0);
        posted[0].bytes[at.start + at.len() / 2] ^= 0x5a;
        let mut posts = set.with_par2();
        posts[0] = posted.remove(0);
        let (scenario, spoken) = hearing(Scenario::new(posts));
        let held = run_holding(&scenario, set.recovery_post()).await;
        assert_eq!(held.status, Some(JobStatus::Complete), "{label}");
        assert_files(&label, &held.files, &set.files);
        // The member was wrong as posted and the run read it before the
        // repair: only a run after the repair makes it whole.
        assert_runs_named_no_stored_member(&label, &spoken);
    }
}

// An article of a stored member lost, under a PAR2 set: the repair
// rebuilds it and the store replays it through its map. The compressed
// member comes out of its run whole, once.
#[tokio::test]
async fn a_damaged_stored_member_is_repaired_and_replayed_beside_the_compressed_one() {
    for version in VERSIONS {
        let label = format!("{version:?}");
        let set = mixed(version, Size::Small, MKV, &options(version));
        let (_, stored) = set.data_of(MKV[0], None).remove(0);
        let (scenario, spoken) =
            hearing(Scenario::new(set.with_par2()).damaged(0, article_in(stored)));
        let graph = run(&scenario).await;
        assert_members(&label, &graph, &set.files);
        assert_fetched_once(&graph);
        assert_runs_named_no_stored_member(&label, &spoken);
    }
}

// An article of the packed data lost with no PAR2: the stored members are
// delivered whole and the compressed one is not.
async fn packed_data_lost(version: Version, names: [&'static str; 3]) -> (Outcome, Set) {
    let label = format!("{version:?}");
    let set = mixed(version, Size::Large, names, &options(version));
    let (_, at) = set.data_of(names[1], None).remove(0);
    let (scenario, spoken) = hearing(Scenario::new(set.posts()).damaged(0, article_in(at)));
    let graph = run(&scenario).await;
    let (_, generations) = named(&spoken);
    assert!(generations.len() <= 1, "{label}: {generations:?}");
    for (name, bytes) in [&set.files[0], &set.files[2]] {
        assert!(
            graph.files.get(&delivered(name)) == Some(bytes)
                || graph.retained_files.values().any(|kept| kept == bytes),
            "{label}: {name} kept whole: {:?} {:?}",
            graph.files.keys(),
            graph.retained_files.keys()
        );
    }
    assert!(
        graph.files.get(&delivered(names[1])) != Some(&set.files[1].1),
        "{label}: the compressed member cannot be whole"
    );
    (graph, set)
}

// A byte of the packed data wrong in the post itself, with no PAR2: every
// article arrives intact, so only the run's decode finds the damage. The
// stored members arrived whole and are delivered; the compressed one is
// not, and the job is Partial for it.
#[tokio::test]
async fn packed_data_wrong_as_posted_without_par2_delivers_the_stored_members() {
    for version in VERSIONS {
        let label = format!("{version:?}");
        let set = mixed(version, Size::Large, MKV, &options(version));
        let (_, at) = set.data_of(MKV[1], None).remove(0);
        let mut posts = set.posts();
        posts[0].bytes[at.start + at.len() / 2] ^= 0x5a;
        let (scenario, spoken) = hearing(Scenario::new(posts));
        let graph = run(&scenario).await;
        assert_eq!(
            graph.status,
            Some(JobStatus::Partial),
            "{label}: {:?}",
            graph.failure
        );
        for (name, bytes) in [&set.files[0], &set.files[2]] {
            assert!(
                graph.files.get(&delivered(name)) == Some(bytes),
                "{label}: {name} delivered whole: {:?}",
                graph.files.keys()
            );
        }
        assert!(!graph.files.contains_key(&delivered(MKV[1])), "{label}");
        assert_fetched_once(&graph);
        assert_runs_named_no_stored_member(&label, &spoken);
    }
}

#[tokio::test]
async fn packed_data_beyond_repair_delivers_the_stored_members_partial() {
    for version in VERSIONS {
        let (graph, _) = packed_data_lost(version, MKV).await;
        assert_eq!(
            graph.status,
            Some(JobStatus::Partial),
            "{version:?}: {:?} {:?}",
            graph.failure,
            graph.files.keys()
        );
        assert_demoted(
            &format!("{version:?}"),
            &graph,
            "silver.horizon.rar",
            "unrepairable",
        );
    }
}

// The same with nothing usable stored: whole `.bin` files alone are no
// delivery, and the job fails.
#[tokio::test]
async fn packed_data_beyond_repair_with_no_usable_stored_member_fails() {
    for version in VERSIONS {
        let label = format!("{version:?}");
        let (graph, _) = packed_data_lost(version, BIN).await;
        assert!(
            matches!(graph.status, Some(JobStatus::Failed { .. })),
            "{label}: {:?} {:?}",
            graph.status,
            graph.files.keys()
        );
        assert_demoted(&label, &graph, "silver.horizon.rar", "unrepairable");
        assert_shelf_gone(&label, &graph);
    }
}

// No decode pool can hold the compressed member's run: its reservation is
// refused before it starts. The run ends where it stands and is not asked
// again, and the stored members are delivered. The set is withheld for
// what the run's reservation said, and the job's events say so.
async fn no_decode_pool(version: Version, names: [&'static str; 3]) -> (Outcome, Set) {
    no_decode_pool_under(&format!("{version:?}"), version, names, &options(version)).await
}

async fn no_decode_pool_under(
    label: &str,
    version: Version,
    names: [&'static str; 3],
    options: &Options,
) -> (Outcome, Set) {
    use weaver_graph::Why;
    use weaver_server_pipeline::pipeline::graph::engine::{DecodePool, RUN_OVERHEAD};
    let set = mixed(version, Size::Small, names, options);
    let first = set.volumes[0].0.clone();
    let (scenario, spoken) = hearing(
        Scenario::new(set.posts())
            .setup(|pipeline| pipeline.graph_decode = Arc::new(DecodePool::new(RUN_OVERHEAD - 1))),
    );
    let graph = run(&scenario).await;
    assert_eq!(
        named(&spoken),
        Default::default(),
        "{label}: no run emitted"
    );
    assert!(
        !graph.files.contains_key(&delivered(names[1])),
        "{label}: {:?}",
        graph.files.keys()
    );
    // Every volume of the set is withheld for the memory.
    let withheld = &graph.graph().delivery.withheld;
    assert_eq!(
        withheld
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        set.volumes
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        "{label}: {withheld:?}"
    );
    assert!(
        withheld.iter().all(|(_, why)| matches!(
            why,
            Why::TooLarge {
                needed: Some(_),
                allowed: Some(_)
            }
        )),
        "{label}: {withheld:?}"
    );
    assert_demoted(label, &graph, &first, "too_large");
    (graph, set)
}

#[tokio::test]
async fn a_compressed_member_no_decode_pool_holds_ends_partial_beside_the_stored_members() {
    for version in VERSIONS {
        let (graph, set) = no_decode_pool(version, MKV).await;
        assert_eq!(
            graph.status,
            Some(JobStatus::Partial),
            "{version:?}: {:?}",
            graph.failure
        );
        for (name, bytes) in [&set.files[0], &set.files[2]] {
            assert!(graph.files.get(&delivered(name)) == Some(bytes), "{name}");
        }
    }
}

#[tokio::test]
async fn a_compressed_member_no_decode_pool_holds_with_no_usable_stored_member_fails_for_its_memory()
 {
    for version in VERSIONS {
        let label = format!("{version:?}");
        let (graph, _) = no_decode_pool(version, BIN).await;
        let Some(JobStatus::Failed { error }) = &graph.status else {
            panic!("{label}: {:?}", graph.status);
        };
        assert!(
            error.starts_with("silver.horizon.rar needs ") && error.contains(" of decode memory, "),
            "{label}: {error}"
        );
        assert_shelf_gone(&label, &graph);
    }
}

// The same with no end record, in one volume and in numbered volumes: the
// quiet that lets the set run steps a run whose reservation is refused, and
// the job ends as with an end record, the set withheld for the memory.
#[tokio::test]
async fn no_end_record_no_decode_pool_holds_ends_partial_beside_the_stored_members() {
    for version in VERSIONS {
        for (case, options) in no_end(version, options(version), 3 * ARTICLE) {
            let label = format!("{version:?} {case}");
            let (graph, set) = no_decode_pool_under(&label, version, MKV, &options).await;
            assert_eq!(set.volumes.len() > 1, case == "volumes", "{label}");
            assert_eq!(
                graph.status,
                Some(JobStatus::Partial),
                "{label}: {:?}",
                graph.failure
            );
            for (name, bytes) in [&set.files[0], &set.files[2]] {
                assert!(
                    graph.files.get(&delivered(name)) == Some(bytes),
                    "{label}: {name}"
                );
            }
            assert_fetched_once(&graph);
        }
    }
}

// The set's volume names, as a failure lists them.
fn names_of(set: &Set) -> String {
    set.volumes
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

#[tokio::test]
async fn no_end_record_no_decode_pool_holds_with_no_usable_stored_member_fails_for_its_memory() {
    for version in VERSIONS {
        for (case, options) in no_end(version, options(version), 3 * ARTICLE) {
            let label = format!("{version:?} {case}");
            let (graph, set) = no_decode_pool_under(&label, version, BIN, &options).await;
            let Some(JobStatus::Failed { error }) = &graph.status else {
                panic!("{label}: {:?}", graph.status);
            };
            assert!(
                error.starts_with(&format!("{} needs ", names_of(&set)))
                    && error.contains(" of decode memory, "),
                "{label}: {error}"
            );
            assert_fetched_once(&graph);
            assert_shelf_gone(&label, &graph);
        }
    }
}

// Taken down and restored after every article: the layout, the shelf and
// the run come back from disk, and the job ends as the uninterrupted one
// does.
#[tokio::test]
async fn a_mixed_rar_restarted_after_every_article_delivers_its_members() {
    for version in VERSIONS {
        let set = mixed(version, Size::Small, MKV, &split(version, 4 * ARTICLE));
        assert!(set.volumes.len() > 1);
        let whole = restart_row(Scenario::new(set.posts())).await;
        assert_members(&format!("{version:?}"), &whole, &set.files);
    }
}

// A RAR4 set whose volumes state no number, posted under names that say
// nothing, so the store lays each volume on trust where it fits. The run
// reads every volume where it was laid; none moves, and the run is not
// restarted.
#[tokio::test]
async fn a_rar4_set_laid_on_trust_runs_once_where_it_was_laid() {
    use crate::pipeline::tests::direct_store::obfuscate_volumes;
    let mut options = split(Version::Rar4, 4 * ARTICLE);
    options.numbered = false;
    let set = mixed(Version::Rar4, Size::Small, MKV, &options);
    assert!(set.volumes.len() > 2, "{}", set.volumes.len());
    let posts: Vec<Post> = obfuscate_volumes(&set.volumes)
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    let (scenario, spoken) = hearing(Scenario::new(posts));
    let graph = run(&scenario).await;
    assert_members("on trust", &graph, &set.files);
    assert_fetched_once(&graph);
    assert_runs_named_no_stored_member("on trust", &spoken);
}

// A recovery record in every volume is the set's own: the store leaves it,
// and the compressed member is made as without one.
#[tokio::test]
async fn a_mixed_rar_with_a_recovery_record_delivers_every_member() {
    for version in VERSIONS {
        let mut options = split(version, 4 * ARTICLE);
        options.recovery = true;
        let set = mixed(version, Size::Small, MKV, &options);
        let (scenario, spoken) = hearing(Scenario::new(set.posts()));
        let graph = run(&scenario).await;
        let label = format!("{version:?}");
        assert_members(&label, &graph, &set.files);
        assert_runs_named_no_stored_member(&label, &spoken);
    }
}

// A header posted naming the compressed member otherwise, under a PAR2
// set over the set as it should be. The compressed member is the set's
// last, after two stored ones. The run decodes under the posted name; the
// recovery volume is held until nothing moves, so it ends first. The repair
// rewrites the header, the re-lay renames the made member where it stands,
// and the run, started again behind the repair, makes it again under the
// repaired name; the stored members before it are never retracted.
async fn renamed(version: Version, options: &Options, password: Option<&str>) {
    let label = format!("{version:?} {:?}", options.password);
    let build = |name: &'static str| {
        let (made, plain) = packed(version, Size::Small, name);
        let files: Files = vec![
            ("alpha.mkv", payload(5 * ARTICLE + 11, 41)),
            ("beta.mkv", payload(4 * ARTICLE, 44)),
            (name, plain),
        ];
        let items = vec![
            Item::File(files[0].0, files[0].1.clone()),
            Item::File(files[1].0, files[1].1.clone()),
            made,
        ];
        Set {
            volumes: named_volumes(write(&items, options)),
            files,
        }
    };
    let before = build("notex.nfo");
    let after = build("notes.nfo");
    assert_eq!(before.total(), after.total(), "{label}");
    let mut scenario = Scenario::new(before.with_par2_of(&after));
    if let Some(password) = password {
        scenario = scenario.password(password);
    }
    let (scenario, spoken) = hearing(scenario);
    let held = run_holding(&scenario, after.recovery_post()).await;
    assert_eq!(
        held.status,
        Some(JobStatus::Complete),
        "{label}: {:?} {:?}",
        held.files.keys(),
        held.after
    );
    assert_files(&label, &held.files, &after.files);
    // Under `-hp` the store gives a set whose repaired headers differ from
    // what it mapped to the whole engine, as it does a set that stores
    // everything: the engine's run makes every member. Otherwise no run
    // named a stored member.
    if !options.encrypt_headers {
        let (members, _) = named(&spoken);
        assert!(
            members.is_subset(&BTreeSet::from([2])),
            "{label}: the members runs named: {members:?}"
        );
    }
}

#[tokio::test]
async fn a_repaired_header_renaming_the_compressed_member_makes_it_again() {
    for version in VERSIONS {
        renamed(version, &options(version), None).await;
    }
}

// A header posted naming a member otherwise, under a PAR2 set over the set
// as it should be: the stored members stream under the posted names, and
// the repair rewrites the header. A stored member whose pieces the repair
// leaves as they were is renamed where it stands: its file keeps the bytes
// already written and is not written again, nor is any member after it. A
// made member renamed is renamed where it stands too, and its run, started
// again behind the repair, makes it again under its new name: the stored
// members beside it are written once.
//
// `which` is the member renamed: 0 the first, 1 the middle one. A mixed set
// is a stored file, the compressed one and a stored file; otherwise all
// three are stored.
async fn renamed_member(version: Version, options: &Options, mixed: bool, which: usize) {
    let label = format!(
        "{version:?} mixed {mixed} renamed {which} password {:?} headers {}",
        options.password, options.encrypt_headers
    );
    let build = |rename: bool| {
        let mut names = renamed_names(mixed);
        if rename {
            names[which] = match (mixed, which) {
                (_, 0) => "alphx.mkv",
                (true, _) => "notez.nfo",
                (false, _) => "betx.mkv",
            };
        }
        let stored = |index: usize| payload((5 - index) * ARTICLE + 11 * index, 41 + index as u8);
        let mut files: Files = Vec::new();
        let mut items = Vec::new();
        for (index, name) in names.into_iter().enumerate() {
            if mixed && index == 1 {
                let (made, plain) = packed(version, Size::Small, name);
                files.push((name, plain));
                items.push(made);
            } else {
                let bytes = stored(index);
                files.push((name, bytes.clone()));
                items.push(Item::File(name, bytes));
            }
        }
        Set {
            volumes: named_volumes(write(&items, options)),
            files,
        }
    };
    let posted = build(true);
    let after = build(false);
    assert_eq!(posted.total(), after.total(), "{label}");
    let mut scenario = Scenario::new(posted.with_par2_of(&after));
    if let Some(password) = options.password {
        scenario = scenario.password(password);
    }
    let graph = run(&scenario).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Complete),
        "{label}: {:?} {:?}",
        graph.failure,
        graph.files.keys()
    );
    // Every member under its repaired name, with its exact bytes, and no
    // file under the name posted.
    assert_files(&label, &graph.files, &after.files);
    assert_fetched_once(&graph);
    // Under `-hp` the repaired volume's headers are walked again and the
    // store gives the set to the whole engine, which makes every member
    // anew.
    if options.encrypt_headers {
        assert!(graph.graph().kinds.contains("rar_engine"), "{label}");
        return;
    }
    // Each stored member's file was written once: one file, never cut back.
    // The compressed member of a mixed set is its run's: a repair that
    // replaces bytes its run read restarts the run, which makes it again.
    let written_once = (0..renamed_names(mixed).len()).filter(|&member| !(mixed && member == 1));
    let files = &graph.graph().member_files;
    for member in written_once {
        let seen = files
            .iter()
            .filter(|((node, id), _)| *node != weaver_graph::NodeId::ROOT && id.0 == member as u32)
            .map(|(_, seen)| seen)
            .collect::<Vec<_>>();
        assert_eq!(seen.len(), 1, "{label}: member {member} files {files:?}");
        assert!(
            seen[0].inodes.len() == 1 && !seen[0].shrank,
            "{label}: member {member} was written again: {:?}",
            seen[0]
        );
    }
}

fn renamed_names(mixed: bool) -> [&'static str; 3] {
    match mixed {
        true => ["alpha.mkv", "notes.nfo", "beta.mkv"],
        false => ["alpha.mkv", "beta.mkv", "gamma.mkv"],
    }
}

// The stored members renamed: the first and the middle one of a set that
// stores every file, and the stored first one of a mixed set, as
// `(mixed, which)`.
const STORED_RENAMES: [(bool, usize); 3] = [(false, 0), (false, 1), (true, 0)];

#[tokio::test]
async fn a_repaired_header_renaming_a_stored_member_renames_it_where_it_stands() {
    for version in VERSIONS {
        for (mixed, which) in STORED_RENAMES {
            renamed_member(version, &options(version), mixed, which).await;
        }
    }
}

// The same with every file enciphered under `-p`.
#[tokio::test]
async fn a_repaired_header_renaming_an_enciphered_stored_member_renames_it_where_it_stands() {
    for version in VERSIONS {
        let mut sealed = options(version);
        sealed.password = Some(SECRET);
        for (mixed, which) in STORED_RENAMES {
            renamed_member(version, &sealed, mixed, which).await;
        }
    }
}

// The same under `-hp`: the store gives the set to the whole engine.
#[tokio::test]
async fn a_repaired_enciphered_header_renaming_a_member_gives_the_set_to_the_engine() {
    for version in VERSIONS {
        for (mixed, which) in STORED_RENAMES {
            if (version, mixed, which) == (Version::Rar5, false, 1) {
                continue;
            }
            renamed_member(version, &sealed_headers(version), mixed, which).await;
        }
    }
}

// A RAR5 set that stores every file under `-hp`, its middle file renamed.
#[tokio::test]
#[ignore = "blocked: the whole engine given the set ends Partial without the renamed member, as it did before members were renamed in place"]
async fn a_repaired_enciphered_header_renaming_the_middle_member_gives_the_set_to_the_engine() {
    renamed_member(Version::Rar5, &sealed_headers(Version::Rar5), false, 1).await;
}

// The compressed member of a mixed set renamed: it is renamed where it
// stands, its run makes it again under its new name, and the stored
// members beside it are written once.
#[tokio::test]
async fn a_repaired_header_renaming_the_made_member_makes_it_again_and_writes_the_stored_ones_once()
{
    for version in VERSIONS {
        let mut sealed = options(version);
        sealed.password = Some(SECRET);
        for options in [options(version), sealed] {
            renamed_member(version, &options, true, 1).await;
        }
    }
}

// Enciphered sets: the compressed member under `-p` is the run's, decoded
// under the job's password; the stored ones are the store's.
mod sealed {
    use super::*;

    // The password offered as the job's: every member delivered, under
    // `-p` with a stored file in the clear, and under `-hp`.
    #[tokio::test]
    async fn a_sealed_mixed_rar_delivers_every_member_under_the_job_password() {
        for version in VERSIONS {
            for (case, options) in [("-p", sealed(version)), ("-hp", sealed_headers(version))] {
                let label = format!("{version:?} {case}");
                let set = mixed(version, Size::Small, MKV, &options);
                let (scenario, spoken) = hearing(Scenario::new(set.posts()).password(SECRET));
                let graph = run(&scenario).await;
                assert_members(&label, &graph, &set.files);
                assert_fetched_once(&graph);
                assert_runs_named_no_stored_member(&label, &spoken);
            }
        }
    }

    // Two wrong candidates before the right one: the run asks again after
    // each, and the last decodes the member.
    // Held back: an offer made while the store is out for its work waits
    // for it (`Tree::offer_password`), and the job restarts the run only
    // when the node shows an engine face at the offer (`offer_password` in
    // the job's passwords), which a lent object does not. The run that
    // asked is not started again under the candidate that opens it.
    #[tokio::test]
    async fn a_sealed_mixed_rar_delivers_every_member_under_the_last_of_several_candidates() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Small, MKV, &sealed(version));
            let (scenario, spoken) = hearing(
                Scenario::new(set.posts())
                    .global_password("ember-harbour")
                    .global_password("EMBER-HARBOR-SECRET")
                    .global_password(SECRET),
            );
            let graph = run(&scenario).await;
            assert_members(&label, &graph, &set.files);
            assert_runs_named_no_stored_member(&label, &spoken);
        }
    }

    // A wrong job password first, then the right one from the settings.
    // Held back: an offer made while the store is out for its work waits
    // for it (`Tree::offer_password`), and the job restarts the run only
    // when the node shows an engine face at the offer (`offer_password` in
    // the job's passwords), which a lent object does not. The run that
    // asked is not started again under the candidate that opens it.
    #[tokio::test]
    async fn a_sealed_mixed_rar_delivers_every_member_after_a_wrong_password() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Small, MKV, &sealed(version));
            let (scenario, spoken) = hearing(
                Scenario::new(set.posts())
                    .password("ember-harbour")
                    .global_password(SECRET),
            );
            let graph = run(&scenario).await;
            assert!(
                graph.completed(),
                "{label}: {:?} {:?} {:?}",
                graph.events,
                graph.failure_kind,
                spoken.lock().unwrap()
            );
            assert_members(&label, &graph, &set.files);
        }
    }

    // No password: the stored file in the clear is delivered, and the job
    // is Partial for the password.
    #[tokio::test]
    async fn a_sealed_mixed_rar_with_no_password_is_partial_for_the_password() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Small, MKV, &sealed(version));
            let graph = run(&Scenario::new(set.posts())).await;
            assert_eq!(
                graph.status,
                Some(JobStatus::Partial),
                "{label}: {:?} {:?}",
                graph.failure,
                graph.files.keys()
            );
            assert_eq!(
                graph.failure_kind,
                Some(HistoryFailureKind::Password),
                "{label}"
            );
            assert!(graph.files.get(&delivered(MKV[0])) == Some(&set.files[0].1));
            for name in [MKV[1], MKV[2]] {
                assert!(
                    !graph.files.contains_key(&delivered(name)),
                    "{label}: {name}"
                );
            }
        }
    }

    // Repair item 1a: the right password, a byte of the enciphered packed
    // data wrong as posted under PAR2. The run reads it and fails; the
    // repair rebuilds it and the run decodes the member.
    #[tokio::test]
    async fn sealed_packed_data_repaired_after_its_run_read_it_delivers_every_member() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Small, MKV, &sealed(version));
            let (_, at) = set.data_of(MKV[1], Some(SECRET)).remove(0);
            let mut posts = set.with_par2();
            posts[0].bytes[at.start + 3] ^= 0x5a;
            let (scenario, spoken) = hearing(Scenario::new(posts).password(SECRET));
            let held = run_holding(&scenario, set.recovery_post()).await;
            assert_eq!(held.status, Some(JobStatus::Complete), "{label}");
            assert_files(&label, &held.files, &set.files);
            assert_runs_named_no_stored_member(&label, &spoken);
        }
    }

    // Repair item 1b: the same with the right password not the first.
    #[tokio::test]
    async fn sealed_packed_data_repaired_under_a_later_candidate_delivers_every_member() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Small, MKV, &sealed(version));
            let (_, at) = set.data_of(MKV[1], Some(SECRET)).remove(0);
            let mut posts = set.with_par2();
            posts[0].bytes[at.start + 3] ^= 0x5a;
            let scenario = Scenario::new(posts)
                .global_password("ember-harbour")
                .global_password(SECRET);
            let held = run_holding(&scenario, set.recovery_post()).await;
            assert_eq!(held.status, Some(JobStatus::Complete), "{label}");
            assert_files(&label, &held.files, &set.files);
        }
    }

    // Repair item 1c: no working password and the packed data damaged: the
    // job ends by the Partial rule for the password, with no repair loop.
    #[tokio::test]
    async fn sealed_damaged_packed_data_with_no_working_password_is_partial_for_the_password() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Small, MKV, &sealed(version));
            let (_, at) = set.data_of(MKV[1], Some(SECRET)).remove(0);
            let mut posts = set.with_par2();
            posts[0].bytes[at.start + 3] ^= 0x5a;
            let graph = run(&Scenario::new(posts).password("ember-harbour")).await;
            assert_eq!(
                graph.status,
                Some(JobStatus::Partial),
                "{label}: {:?}",
                graph.failure
            );
            assert_eq!(
                graph.failure_kind,
                Some(HistoryFailureKind::Password),
                "{label}"
            );
            assert!(graph.files.get(&delivered(MKV[0])) == Some(&set.files[0].1));
        }
    }

    // Repair item 2: an article of the enciphered stored member lost under
    // PAR2, beside the enciphered compressed one: the store replays the
    // repaired member, and the run makes the compressed one once.
    #[tokio::test]
    async fn a_sealed_stored_member_is_repaired_beside_the_compressed_one() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Small, MKV, &sealed(version));
            let (_, stored) = set.data_of(MKV[2], Some(SECRET)).remove(0);
            let (scenario, spoken) = hearing(
                Scenario::new(set.with_par2())
                    .damaged(0, article_in(stored))
                    .password(SECRET),
            );
            let graph = run(&scenario).await;
            assert_members(&label, &graph, &set.files);
            assert_runs_named_no_stored_member(&label, &spoken);
        }
    }

    // Repair item 3, before the run: the headers' first article lost under
    // PAR2, `-p` and `-hp`. Nothing is laid until the repair brings it.
    #[tokio::test]
    async fn a_sealed_mixed_rar_whose_headers_are_repaired_delivers_every_member() {
        for version in VERSIONS {
            for (case, options) in [("-p", sealed(version)), ("-hp", sealed_headers(version))] {
                let label = format!("{version:?} {case}");
                let set = mixed(version, Size::Small, MKV, &options);
                let graph = run(&Scenario::new(set.with_par2())
                    .damaged(0, 0)
                    .password(SECRET))
                .await;
                assert_members(&label, &graph, &set.files);
                assert_fetched_once(&graph);
            }
        }
    }

    // Repair item 3, after the run, `-p` and `-hp`.
    #[tokio::test]
    async fn a_repaired_header_renaming_the_sealed_compressed_member_makes_it_again() {
        for version in VERSIONS {
            renamed(version, &sealed(version), Some(SECRET)).await;
            renamed(version, &sealed_headers(version), Some(SECRET)).await;
        }
    }

    // Repair item 4: an article of the enciphered packed data lost with no
    // PAR2: the member is beyond repair, and the reason is the damage, not
    // the password.
    #[tokio::test]
    async fn sealed_packed_data_beyond_repair_is_partial_for_the_damage() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Large, MKV, &sealed(version));
            let (_, at) = set.data_of(MKV[1], Some(SECRET)).remove(0);
            let graph = run(&Scenario::new(set.posts())
                .damaged(0, article_in(at))
                .password(SECRET))
            .await;
            assert_eq!(
                graph.status,
                Some(JobStatus::Partial),
                "{label}: {:?}",
                graph.failure
            );
            assert_ne!(
                graph.failure_kind,
                Some(HistoryFailureKind::Password),
                "{label}"
            );
            assert_demoted(&label, &graph, "silver.horizon.rar", "unrepairable");
        }
    }

    // A RAR5 header that checks the password proves the job's password
    // before a byte is decrypted, so a byte of the enciphered packed data
    // wrong as posted is damage, not a sign of another password. With no
    // PAR2 the job ends for the damage, never for the password.
    #[tokio::test]
    async fn sealed_rar5_packed_data_wrong_under_a_proven_password_is_partial_for_the_damage() {
        let set = mixed(Version::Rar5, Size::Large, MKV, &sealed(Version::Rar5));
        let (_, at) = set.data_of(MKV[1], Some(SECRET)).remove(0);
        let mut posts = set.posts();
        // Past the coded stream's first block header, so the decode runs
        // on and the member's checksum fails.
        posts[0].bytes[at.start + at.len() / 2] ^= 0x5a;
        let graph = run(&Scenario::new(posts).password(SECRET)).await;
        assert_eq!(
            graph.status,
            Some(JobStatus::Partial),
            "{:?}",
            graph.failure
        );
        assert_ne!(graph.failure_kind, Some(HistoryFailureKind::Password));
        assert!(!graph.files.contains_key(&delivered(MKV[1])));
        assert_demoted("RAR5", &graph, "silver.horizon.rar", "unrepairable");
    }

    // The same under PAR2, the right password the first of two candidates:
    // the run's failure asks for no other candidate, the repair rebuilds the
    // byte, and the run decodes the member under the first.
    #[tokio::test]
    async fn sealed_rar5_packed_data_wrong_under_a_proven_password_is_repaired_without_another_candidate()
     {
        let set = mixed(Version::Rar5, Size::Large, MKV, &sealed(Version::Rar5));
        let (_, at) = set.data_of(MKV[1], Some(SECRET)).remove(0);
        let mut posts = set.with_par2();
        posts[0].bytes[at.start + at.len() / 2] ^= 0x5a;
        let scenario = Scenario::new(posts)
            .global_password(SECRET)
            .global_password("ember-harbour");
        let held = run_holding(&scenario, set.recovery_post()).await;
        assert_eq!(held.status, Some(JobStatus::Complete));
        assert_files("RAR5", &held.files, &set.files);
    }

    // Repair item 5: the enciphered packed data spans volumes; a byte of
    // its part in the second volume is wrong as posted, under PAR2.
    #[tokio::test]
    async fn sealed_packed_data_spanning_volumes_is_repaired_in_one_part() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let mut options = sealed(version);
            options.split = Some(30_000);
            options.numbered = true;
            let set = mixed(version, Size::Large, MKV, &options);
            let parts = set.data_of(MKV[1], Some(SECRET));
            assert!(parts.len() >= 2, "{label}: {parts:?}");
            let (volume, at) = parts[1].clone();
            let mut posts = set.with_par2();
            posts[volume as usize].bytes[at.start + 100] ^= 0x5a;
            let held =
                run_holding(&Scenario::new(posts).password(SECRET), set.recovery_post()).await;
            assert_eq!(held.status, Some(JobStatus::Complete), "{label}");
            assert_files(&label, &held.files, &set.files);
        }
    }
}

// A mixed RAR below another object, and archives below a mixed RAR. Below
// the root only release layers open; what they hold lands in the job's
// folder.
mod nested {
    use std::collections::BTreeMap;
    use std::io::{Cursor, Write};

    use ::zip::ZipWriter;
    use ::zip::write::SimpleFileOptions;
    use weaver_graph::NodeId;

    use super::*;

    type Heard = Arc<Mutex<Vec<(NodeId, u64, String)>>>;

    fn heard(scenario: Scenario) -> (Scenario, Heard) {
        let heard = Heard::default();
        let kept = Arc::clone(&heard);
        let scenario = scenario.on_run_message(move |node, generation, message| {
            kept.lock().unwrap().push((node, generation, message));
        });
        (scenario, heard)
    }

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

    // The mixed RAR a stored member of a zip: the zip streams it, and the
    // RAR store below streams its stored members and shelves only the
    // headers and the packed data, as at the root.
    #[tokio::test]
    async fn a_mixed_rar_inside_a_stored_zip_streams_as_at_the_root() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Small, MKV, &options(version));
            let rar = set.volumes[0].1.clone();
            let outer = zip_of(stored_zip(), &[("lantern.rar", rar.clone())]);
            let (scenario, heard) = heard(Scenario::new(vec![Post::plain("Ember.zip", outer)]));
            let graph = run(&scenario).await;
            assert!(graph.completed(), "{label}: {:?}", graph.failure);
            assert_files(&label, &graph.files, &set.files);
            assert_fetched_once(&graph);
            assert!(kinds(&graph).contains(&"rar_store"), "{label}");
            assert!(!kinds(&graph).contains(&"rar_engine"), "{label}");
            assert!(generations(&heard).len() <= 1, "{label}: only the RAR runs");
            let shelved: Vec<u64> = graph
                .graph()
                .shelved_by
                .values()
                .filter(|(kind, _)| kind == "rar_store")
                .map(|(_, kept)| *kept)
                .collect();
            assert_eq!(
                shelved,
                vec![rar.len() as u64 - set.stored_bytes()],
                "{label}"
            );
        }
    }

    // The mixed RAR a deflated member of a zip, which the zip's run makes.
    // A byte of the deflate stream is wrong as posted; the PAR2 set over the
    // zip rebuilds it, the zip's run makes the RAR again on a new node, and
    // the RAR store and its run below start over on the new bytes.
    #[tokio::test]
    async fn a_mixed_rar_a_zip_run_makes_again_after_a_repair_starts_over_below() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Small, MKV, &options(version));
            let outer = zip_of(deflated_zip(), &[("lantern.rar", set.volumes[0].1.clone())]);
            let mut posted = outer.clone();
            posted[outer.len() / 3] ^= 0x5a;
            let mut posts = vec![Post::plain("Ember.zip", posted)];
            posts.extend(par2_set("Ember", &[("Ember.zip", &outer)], 4));
            let held = run_holding(&Scenario::new(posts), 2).await;
            assert_eq!(held.status, Some(JobStatus::Complete), "{label}");
            assert_files(&label, &held.files, &set.files);
        }
    }

    // The mixed RAR cut into numbered parts: the split store joins them,
    // and the RAR store below reads the join as one volume.
    #[tokio::test]
    async fn a_mixed_rar_in_numbered_parts_streams_as_one_volume() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = mixed(version, Size::Small, MKV, &options(version));
            let rar = &set.volumes[0].1;
            let cut = (rar.len() / 2).next_multiple_of(ARTICLE);
            let graph = run(&Scenario::new(vec![
                Post::plain("lantern.rar.001", rar[..cut].to_vec()),
                Post::plain("lantern.rar.002", rar[cut..].to_vec()),
            ]))
            .await;
            assert!(graph.completed(), "{label}: {:?}", graph.failure);
            assert_files(&label, &graph.files, &set.files);
            assert!(!kinds(&graph).contains(&"rar_engine"), "{label}");
        }
    }

    fn inner_files() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("spark.mkv", payload(4 * ARTICLE + 3, 61)),
            ("flint.nfo", payload(ARTICLE / 2, 62)),
        ]
    }

    // A zip a stored member of the mixed RAR, before the compressed member,
    // whose packed data is wrong as posted under PAR2. The run reads the
    // damage and fails, and once repaired it starts over; the zip below,
    // which the store streamed, does not.
    #[tokio::test]
    async fn a_zip_stored_in_a_mixed_rar_stands_when_the_run_starts_over() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let zip = zip_of(stored_zip(), &inner_files());
            let (made, plain) = packed(version, Size::Small, "notes.nfo");
            let items = vec![
                Item::File("inner.zip", zip),
                made,
                Item::File("beta.mkv", payload(4 * ARTICLE, 44)),
            ];
            let set = Set {
                volumes: named_volumes(write(&items, &options(version))),
                files: Vec::new(),
            };
            let (_, at) = set.data_of("notes.nfo", None).remove(0);
            let mut posts = set.with_par2();
            posts[0].bytes[at.start + at.len() / 2] ^= 0x5a;
            let (scenario, heard) = heard(Scenario::new(posts));
            let held = run_holding(&scenario, set.recovery_post()).await;
            assert_eq!(held.status, Some(JobStatus::Complete), "{label}");
            let mut expect = inner_files();
            expect.push(("notes.nfo", plain));
            expect.push(("beta.mkv", payload(4 * ARTICLE, 44)));
            assert_files(&label, &held.files, &expect);
            // The zip below, which the store streamed, runs nothing.
            let ran = generations(&heard);
            assert!(ran.len() <= 1, "{label}: only the RAR runs: {ran:?}");
        }
    }

    // A compressed member whose packed data spans volumes, with the
    // volumes of a nested RAR set stored around it. The nested set opens
    // below, and the run makes the compressed member across the volumes.
    #[tokio::test]
    async fn a_compressed_member_spanning_volumes_beside_a_stored_nested_set_delivers_both() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let inner_body = payload(3 * ARTICLE + 7, 71);
            let mut inner_options = split(version, 2 * ARTICLE);
            inner_options.numbered = true;
            let inner = write(
                &[Item::File("spark.mkv", inner_body.clone())],
                &inner_options,
            );
            assert_eq!(inner.len(), 2, "{label}");
            let (made, plain) = packed(version, Size::Large, "notes.nfo");
            let names = ["inner.part1.rar", "inner.part2.rar"];
            let items = vec![
                Item::File(names[0], inner[0].clone()),
                made,
                Item::File(names[1], inner[1].clone()),
            ];
            let set = Set {
                volumes: named_volumes(write(&items, &split(version, 30_000))),
                files: Vec::new(),
            };
            assert!(set.data_of("notes.nfo", None).len() >= 2, "{label}");
            let graph = run(&Scenario::new(set.posts())).await;
            assert!(graph.completed(), "{label}: {:?}", graph.failure);
            assert_files(
                &label,
                &graph.files,
                &[("notes.nfo", plain), ("spark.mkv", inner_body)],
            );
            assert!(!kinds(&graph).contains(&"rar_engine"), "{label}");
        }
    }

    // A stored RAR and a stored 7z, as compressed members of the mixed set
    // (`inners` names which): literal-coded, so the set's run makes them,
    // and each is a layer of the release that opens below as any nested
    // archive does.
    fn nested_items(version: Version, inners: &[&str]) -> (Vec<Item>, Files) {
        let rar_files = inner_files();
        let sevenz_files: Files = vec![
            ("cinder.mkv", payload(5 * ARTICLE + 9, 63)),
            ("ash.nfo", payload(ARTICLE / 3, 64)),
        ];
        let alpha = payload(4 * ARTICLE + 5, 41);
        let beta = payload(3 * ARTICLE, 44);
        let mut items = vec![
            Item::File("alpha.mkv", alpha.clone()),
            Item::File("beta.mkv", beta.clone()),
        ];
        let mut files: Files = vec![("alpha.mkv", alpha), ("beta.mkv", beta)];
        if inners.contains(&"inner.rar") {
            let stored: Vec<Item> = rar_files
                .iter()
                .map(|(name, bytes)| Item::File(name, bytes.clone()))
                .collect();
            let inner = write(&stored, &options(version)).remove(0);
            items.insert(1, literal(version, "inner.rar", &inner));
            files.extend(rar_files);
        }
        if inners.contains(&"inner.7z") {
            let inner =
                weaver_server_pipeline::pipeline::tests::engines::depth::sevenz(&sevenz_files);
            items.push(literal(version, "inner.7z", &inner));
            files.extend(sevenz_files);
        }
        (items, files)
    }

    // The set's store and, below it, a store for each nested archive: each
    // RAR store keeps a shelf of its own.
    fn assert_nested_opened(label: &str, graph: &Outcome, inners: &[&str]) {
        let kinds = kinds(graph);
        let rar_stores = graph
            .graph()
            .shelved_by
            .values()
            .filter(|(kind, _)| kind == "rar_store")
            .count();
        assert_eq!(
            rar_stores,
            1 + usize::from(inners.contains(&"inner.rar")),
            "{label}: {:?}",
            graph.graph().shelved_by
        );
        assert_eq!(
            kinds.contains(&"sevenz_store"),
            inners.contains(&"inner.7z"),
            "{label}: {kinds:?}"
        );
        assert!(!kinds.contains(&"rar_engine"), "{label}: {kinds:?}");
    }

    async fn nested_made_by_the_run(inners: &[&str]) {
        for version in VERSIONS {
            for (case, split) in [("one volume", None), ("volumes", Some(3 * ARTICLE))] {
                let label = format!("{version:?} {inners:?} {case}");
                let (items, files) = nested_items(version, inners);
                let mut options = options(version);
                options.split = split;
                options.numbered = split.is_some();
                let set = Set {
                    volumes: named_volumes(write(&items, &options)),
                    files: Vec::new(),
                };
                assert_eq!(set.volumes.len() > 1, split.is_some(), "{label}");
                let graph = run(&Scenario::new(set.posts())).await;
                assert_eq!(
                    graph.status,
                    Some(JobStatus::Complete),
                    "{label}: {:?}",
                    graph.failure
                );
                assert_files(&label, &graph.files, &files);
                assert_fetched_once(&graph);
                assert_nested_opened(&label, &graph, inners);
            }
        }
    }

    #[tokio::test]
    async fn a_nested_rar_the_run_makes_opens_below() {
        nested_made_by_the_run(&["inner.rar"]).await;
    }

    #[tokio::test]
    async fn a_nested_7z_the_run_makes_opens_below() {
        nested_made_by_the_run(&["inner.7z"]).await;
    }

    #[tokio::test]
    async fn a_nested_rar_and_7z_the_run_makes_both_open_below() {
        nested_made_by_the_run(&["inner.rar", "inner.7z"]).await;
    }

    // Repair item 6: the nested archive enciphered in the set, a byte of
    // its packed data wrong as posted, under PAR2. The run makes the nested
    // archive with the damage in it and the archive below reads those
    // bytes; the checksum fails, the repair rebuilds the byte, the run makes
    // it again, and the archive below starts over on the new bytes.
    #[tokio::test]
    async fn a_nested_archive_in_an_enciphered_made_member_starts_over_after_a_repair() {
        for version in VERSIONS {
            for inner in ["inner.rar", "inner.7z"] {
                let label = format!("{version:?} {inner}");
                let (items, files) = nested_items(version, &[inner]);
                let set = Set {
                    volumes: named_volumes(write(&items, &sealed(version))),
                    files: Vec::new(),
                };
                let (_, at) = set.data_of(inner, Some(SECRET)).remove(0);
                let mut posts = set.with_par2();
                posts[0].bytes[at.start + at.len() / 2] ^= 0x5a;
                let held =
                    run_holding(&Scenario::new(posts).password(SECRET), set.recovery_post()).await;
                assert_eq!(held.status, Some(JobStatus::Complete), "{label}");
                assert_files(&label, &held.files, &files);
            }
        }
    }

    // The same content packed all stored, mixed and all compressed delivers
    // the same files.
    #[tokio::test]
    async fn the_same_content_stored_mixed_or_compressed_delivers_alike() {
        for version in VERSIONS {
            let (zeros, zeros_plain) = packed(version, Size::Small, "alpha.mkv");
            let (notes, notes_plain) = packed(version, Size::Large, "notes.nfo");
            let files = vec![
                ("alpha.mkv", zeros_plain.clone()),
                ("notes.nfo", notes_plain.clone()),
            ];
            let cases = [
                (
                    "stored",
                    vec![
                        Item::File("alpha.mkv", zeros_plain.clone()),
                        Item::File("notes.nfo", notes_plain.clone()),
                    ],
                ),
                (
                    "mixed",
                    vec![Item::File("alpha.mkv", zeros_plain.clone()), notes.clone()],
                ),
                ("compressed", vec![zeros, notes]),
            ];
            for (case, items) in cases {
                let label = format!("{version:?} {case}");
                let volumes = named_volumes(write(&items, &options(version)));
                let posts: Vec<Post> = volumes
                    .iter()
                    .map(|(name, bytes)| Post::plain(name, bytes.clone()))
                    .collect();
                // Stored, the text is some 227 KB: posted in larger
                // articles, as a real post is.
                let graph = run(&Scenario::new(posts).framed(16 * 1024)).await;
                assert_eq!(graph.status, Some(JobStatus::Complete), "{label}");
                assert_files(&label, &graph.files, &files);
                assert_fetched_once(&graph);
            }
        }
    }
}

// A part of a compressed member's packed data, read off the headers of the
// volume it lies in: the volume, where in it, and the member it is a part
// of, by the name its first part gives it.
struct PackedPart {
    volume: u32,
    data: Range<u64>,
    name: String,
}

fn packed_parts(volumes: &[(String, Vec<u8>)], password: Option<&str>) -> Vec<PackedPart> {
    let mut parts = Vec::new();
    let mut name = String::new();
    for (volume, (_, bytes)) in volumes.iter().enumerate() {
        let facts = unrar_rs::RarArchive::parse_volume_facts_walk_with_shared_kdf_cache(
            std::io::Cursor::new(bytes.clone()),
            password,
            Arc::new(unrar_rs::KdfCache::new()),
        )
        .expect("a RAR volume")
        .facts;
        let mut members: Vec<_> = facts.members.iter().collect();
        members.sort_by_key(|member| member.order);
        for member in members {
            if !member.split_before || name.is_empty() {
                name.clone_from(&member.name);
            }
            if member.compression_method != 0 && member.data_size > 0 {
                parts.push(PackedPart {
                    volume: volume as u32,
                    data: member.data_offset..member.data_offset + member.data_size,
                    name: name.clone(),
                });
            }
        }
    }
    parts
}

// Every look at which a verified byte of a compressed member's packed data
// stood on a shelf, where a run could read it, while no member an object
// announced carried that member's name: `name_at` gives the name the shelf's headers gave it
// at that look. Also how many looks had packed data on a shelf while some
// compressed member was still unannounced, so the race was there to lose.
fn packed_ahead_of_its_member(
    graph: &Outcome,
    parts: &[PackedPart],
    name_at: impl Fn(&super::harness::ShelfLook, &PackedPart) -> String,
) -> (Vec<String>, usize) {
    let mut early = Vec::new();
    let mut raced = 0;
    for (at, look) in graph.graph().shelf_looks.iter().enumerate() {
        let shelved = |part: &PackedPart| {
            look.kept.iter().any(|((_, volume), runs)| {
                *volume == part.volume
                    && runs.iter().any(|(range, provenance)| {
                        provenance.is_verified()
                            && range.start < part.data.end
                            && part.data.start < range.end
                    })
            })
        };
        let mut any = false;
        let mut unannounced = false;
        for part in parts {
            let name = name_at(look, part);
            let announced = look.names.contains(&name);
            unannounced |= !announced;
            if shelved(part) {
                any = true;
                if !announced {
                    early.push(format!(
                        "look {at}: {name} in volume {} at {:?}, names {:?}",
                        part.volume, part.data, look.names
                    ));
                }
            }
        }
        if any && unannounced {
            raced += 1;
        }
    }
    (early, raced)
}

// A stored file, a small compressed one, a stored file, a large compressed
// one across many volumes, and a stored file.
fn two_packed(version: Version) -> (Vec<Item>, Files) {
    let (small, small_plain) = packed(version, Size::Small, "notes.nfo");
    let (large, large_plain) = packed(version, Size::Large, "inner.txt");
    let files: Files = vec![
        ("alpha.mkv", payload(5 * ARTICLE + 11, 41)),
        ("notes.nfo", small_plain),
        ("beta.mkv", payload(4 * ARTICLE, 44)),
        ("inner.txt", large_plain),
        ("gamma.srt", payload(2 * ARTICLE + 3, 47)),
    ];
    let items = vec![
        Item::File(files[0].0, files[0].1.clone()),
        small,
        Item::File(files[2].0, files[2].1.clone()),
        large,
        Item::File(files[4].0, files[4].1.clone()),
    ];
    (items, files)
}

// No byte of a compressed member's packed data stands on the store's shelf,
// where a run could read it, before the store has announced that member:
// at every look, posted in order, last article first, and with every
// volume's articles landing out of order at once; plain, under `-p` and
// under `-hp`; for RAR5 and RAR4.
#[tokio::test]
async fn packed_data_reaches_the_shelf_only_after_its_member_is_announced() {
    let mut raced = 0;
    for version in VERSIONS {
        for (case, password, headers) in [
            ("plain", None, false),
            ("-p", Some(SECRET), false),
            ("-hp", Some(SECRET), true),
        ] {
            let mut options = split(version, 4_400);
            options.password = password;
            options.encrypt_headers = headers;
            let (items, files) = two_packed(version);
            let volumes = named_volumes(write(&items, &options));
            assert!(volumes.len() >= 10, "{}", volumes.len());
            let parts = packed_parts(&volumes, password);
            assert!(parts.iter().any(|part| part.name == "notes.nfo"));
            assert!(parts.iter().any(|part| part.name == "inner.txt"));
            let set = Set { volumes, files };
            let every: Vec<u32> = (0..set.volumes.len() as u32).collect();
            for order in ["in order", "backwards", "reversed"] {
                let label = format!("{version:?} {case} {order}");
                let mut scenario = Scenario::new(set.posts());
                if let Some(password) = password {
                    scenario = scenario.password(password);
                }
                scenario = match order {
                    "backwards" => scenario.backwards(),
                    "reversed" => scenario.reversed(&every),
                    _ => scenario,
                };
                let graph = run(&scenario).await;
                assert_files(&label, &graph.files, &set.files);
                assert_fetched_once(&graph);
                let (early, looks) =
                    packed_ahead_of_its_member(&graph, &parts, |_, part| part.name.clone());
                assert!(early.is_empty(), "{label}: {early:#?}");
                raced += looks;
            }
        }
    }
    assert!(
        raced > 0,
        "no look had packed data shelved ahead of a member"
    );
}

// Headers posted naming the large compressed member otherwise, under a PAR2
// set over the set as it should be: until the repair, the shelf's headers
// name it as posted and the store announces it so; once repaired, under the
// repaired name. At no look is its packed data on the shelf ahead of its
// announcement under the name the shelf's headers give it then.
#[tokio::test]
async fn packed_data_under_a_repaired_header_reaches_the_shelf_only_after_its_member_is_announced()
{
    for version in VERSIONS {
        let label = format!("{version:?}");
        let options = split(version, 4_400);
        let build = |name: &'static str| {
            // No stored member after the renamed one: the re-lay retracts
            // every member from it on, and the store does not stream a
            // retracted stored member again (the ignored renaming tests).
            let (mut items, mut files) = two_packed(version);
            items.truncate(4);
            files.truncate(4);
            let Item::Packed { name: packed, .. } = &mut items[3] else {
                unreachable!()
            };
            *packed = name;
            files[3].0 = name;
            Set {
                volumes: named_volumes(write(&items, &options)),
                files,
            }
        };
        let before = build("inner.txz");
        let after = build("inner.txt");
        assert_eq!(before.total(), after.total(), "{label}");
        // Where the headers differ, by volume.
        let renamed: Vec<(u32, Range<u64>)> = before
            .volumes
            .iter()
            .zip(&after.volumes)
            .enumerate()
            .filter_map(|(volume, ((_, posted), (_, right)))| {
                let first = posted.iter().zip(right).position(|(a, b)| a != b)?;
                let last = posted.iter().zip(right).rposition(|(a, b)| a != b)?;
                Some((volume as u32, first as u64..last as u64 + 1))
            })
            .collect();
        assert!(!renamed.is_empty(), "{label}");
        let parts = packed_parts(&after.volumes, None);
        // The header repeats in every volume the member spans, so the
        // recovery set holds a block for each.
        let mut posts = before.posts();
        let volumes: Vec<(&str, &[u8])> = after
            .volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        posts.extend(par2_set(
            "silver.horizon",
            &volumes,
            2 * renamed.len() as u32,
        ));
        let graph = run(&Scenario::new(posts)).await;
        assert!(
            graph.completed(),
            "{label}: {:?} {:?} {:?}",
            graph.status,
            graph.failure,
            graph.files.keys()
        );
        assert_files(&label, &graph.files, &after.files);
        assert_fetched_once(&graph);
        // The member's first part names it: the shelf's headers give the
        // repaired name once that part's header is repaired, whatever the
        // headers that carry it on say.
        let repaired = |look: &super::harness::ShelfLook| {
            renamed[..1].iter().all(|(volume, range)| {
                look.kept.iter().any(|((_, at), runs)| {
                    at == volume
                        && runs.iter().any(|(run, provenance)| {
                            *provenance == weaver_graph::Provenance::Repaired
                                && run.start <= range.start
                                && range.end <= run.end
                        })
                })
            })
        };
        let (early, _) = packed_ahead_of_its_member(&graph, &parts, |look, part| {
            if part.name == "inner.txt" && !repaired(look) {
                "inner.txz".to_owned()
            } else {
                part.name.clone()
            }
        });
        // Some look saw the headers as posted and some as repaired.
        let looks = &graph.graph().shelf_looks;
        assert!(looks.iter().any(repaired), "{label}");
        assert!(looks.iter().any(|look| !repaired(look)), "{label}");
        assert!(early.is_empty(), "{label}: {early:#?}");
    }
}

// The store's run chases the layout: it starts at the first member the
// store makes, and makes each member it meets that the store makes, laid
// before it started or after.
mod chase {
    use super::*;
    use crate::pipeline::tests::direct_store::obfuscate_volumes;
    use weaver_objects::rar_store::writer::Codec;

    // Whether a run closed member `id` in what was heard.
    fn closed(spoken: &Spoken, id: u32) -> bool {
        let closed = format!("MemberClosed(MemberId({id}))");
        spoken
            .lock()
            .unwrap()
            .iter()
            .any(|(_, message)| message.starts_with("Emit(") && message.contains(&closed))
    }

    // The volumes that hold a part of `name`'s data.
    fn volumes_of(set: &Set, name: &str, password: Option<&str>) -> BTreeSet<u32> {
        set.data_of(name, password)
            .into_iter()
            .map(|(volume, _)| volume)
            .collect()
    }

    // A compressed file first, then a stored one.
    fn compressed_first(version: Version, size: Size) -> (Vec<Item>, Files) {
        let (first, plain) = packed(version, size, "first.nfo");
        let files: Files = vec![
            ("first.nfo", plain),
            ("alpha.mkv", payload(5 * ARTICLE + 11, 41)),
        ];
        let items = vec![first, Item::File(files[1].0, files[1].1.clone())];
        (items, files)
    }

    // Two compressed files and nothing stored.
    fn compressed_only(version: Version) -> (Vec<Item>, Files) {
        let (first, first_plain) = packed(version, Size::Small, "first.nfo");
        let (second, second_plain) = packed(version, Size::Large, "second.txt");
        let files: Files = vec![("first.nfo", first_plain), ("second.txt", second_plain)];
        (vec![first, second], files)
    }

    // Two compressed members a stored file between them keeps apart: it
    // fills volumes of its own, so the second's header lies past the
    // first's last volume.
    fn apart(version: Version) -> Set {
        let (mut items, mut files) = two_packed(version);
        let between = payload(3 * 4_400, 44);
        items[2] = Item::File("beta.mkv", between.clone());
        files[2].1 = between;
        set_of((items, files), &split(version, 4_400))
    }

    fn set_of((items, files): (Vec<Item>, Files), options: &Options) -> Set {
        Set {
            volumes: named_volumes(write(&items, options)),
            files,
        }
    }

    // The set held at its last volume: every article of it is out with the
    // server until nothing moves. By then the run has closed the first
    // member it makes, from the volumes before; once the last comes, the
    // job ends Complete with every member as written, from one generation
    // of runs. A mixed set, and one of compressed files alone.
    #[tokio::test]
    async fn a_made_member_is_delivered_before_the_last_volume_arrives() {
        for version in VERSIONS {
            for (case, set, first) in [
                (
                    "mixed",
                    mixed(version, Size::Small, MKV, &split(version, 3 * ARTICLE)),
                    (MADE, MKV[1]),
                ),
                (
                    "compressed only",
                    set_of(compressed_only(version), &split(version, 3 * ARTICLE)),
                    (0, "first.nfo"),
                ),
            ] {
                let label = format!("{version:?} {case}");
                let last = set.volumes.len() as u32 - 1;
                assert!(last >= 2, "{label}");
                assert!(
                    !volumes_of(&set, first.1, None).contains(&last),
                    "{label}: the first made member ends before the last volume"
                );
                let (scenario, spoken) = hearing(Scenario::new(set.posts()));
                let at_hold = Arc::new(Mutex::new(None));
                let seen = Arc::clone(&at_hold);
                let heard = Arc::clone(&spoken);
                let held = super::super::harness::run_holding_where(
                    &scenario,
                    |article| article.0 == last,
                    move |_| *seen.lock().unwrap() = Some(closed(&heard, first.0)),
                )
                .await;
                assert_eq!(
                    *at_hold.lock().unwrap(),
                    Some(true),
                    "{label}: {} closed before the last volume came",
                    first.1
                );
                assert_eq!(held.status, Some(JobStatus::Complete), "{label}");
                assert_files(&label, &held.files, &set.files);
                assert_eq!(named(&spoken).1.len(), 1, "{label}: one generation of runs");
            }
        }
    }

    // Two compressed members, the second's header in a volume held until
    // nothing moves: by then the run has started and closed the first. The
    // same run, one generation, makes the second once its volumes come.
    #[tokio::test]
    async fn a_member_laid_after_the_run_started_is_made_by_the_same_run() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = apart(version);
            let late = *volumes_of(&set, "inner.txt", None)
                .first()
                .expect("the large member's volumes");
            assert!(
                volumes_of(&set, "notes.nfo", None)
                    .iter()
                    .all(|volume| *volume < late),
                "{label}"
            );
            let (scenario, spoken) = hearing(Scenario::new(set.posts()));
            let at_hold = Arc::new(Mutex::new(None));
            let seen = Arc::clone(&at_hold);
            let heard = Arc::clone(&spoken);
            let held = super::super::harness::run_holding_where(
                &scenario,
                |article| article.0 == late,
                move |_| *seen.lock().unwrap() = Some((closed(&heard, 1), closed(&heard, 3))),
            )
            .await;
            assert_eq!(
                *at_hold.lock().unwrap(),
                Some((true, false)),
                "{label}: the run closed notes.nfo and waits on the held volume"
            );
            assert_eq!(held.status, Some(JobStatus::Complete), "{label}");
            assert_files(&label, &held.files, &set.files);
            let (members, generations) = named(&spoken);
            assert_eq!(members, BTreeSet::from([1, 3]), "{label}");
            assert_eq!(generations.len(), 1, "{label}: one start, one generation");
            assert!(closed(&spoken, 3), "{label}");
        }
    }

    // Not solid, of compressed files alone: the store keeps the set and its
    // run makes every member, as the whole engine does.
    #[tokio::test]
    async fn a_set_of_compressed_files_alone_stays_with_the_store() {
        for version in VERSIONS {
            for (case, options) in [
                ("one volume", options(version)),
                ("volumes", split(version, 4 * ARTICLE)),
            ] {
                let label = format!("{version:?} {case}");
                let set = set_of(compressed_only(version), &options);
                let graph = run(&Scenario::new(set.posts())).await;
                assert_members(&label, &graph, &set.files);
                assert_fetched_once(&graph);
            }
        }
    }

    // A compressed file first, then a stored one: the stored one is
    // streamed, the compressed one made.
    #[tokio::test]
    async fn a_set_whose_first_file_is_compressed_streams_the_stored_one_after_it() {
        for version in VERSIONS {
            for (case, options) in [
                ("one volume", options(version)),
                ("volumes", split(version, 4 * ARTICLE)),
            ] {
                let label = format!("{version:?} {case}");
                let set = set_of(compressed_first(version, Size::Small), &options);
                let (scenario, spoken) = hearing(Scenario::new(set.posts()));
                let graph = run(&scenario).await;
                assert_members(&label, &graph, &set.files);
                assert_fetched_once(&graph);
                let (members, _) = named(&spoken);
                assert_eq!(
                    members,
                    BTreeSet::from([0]),
                    "{label}: runs name the first only"
                );
            }
        }
    }

    // Solid, its first file compressed: the whole engine takes the set.
    #[tokio::test]
    async fn a_solid_set_whose_first_file_is_compressed_goes_to_the_engine() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let mut options = options(version);
            options.solid = true;
            let set = set_of(compressed_first(version, Size::Small), &options);
            let graph = run(&Scenario::new(set.posts())).await;
            assert!(graph.completed(), "{label}: {:?}", graph.failure);
            assert_files(&label, &graph.files, &set.files);
            assert!(
                kinds(&graph).contains(&"rar_engine"),
                "{label}: {:?}",
                kinds(&graph)
            );
        }
    }

    // A second compressed member whose window is wider than the run
    // reserved at its start. A lent window is scratch, not RAM, so the
    // decode pool's ceiling does not bound it: with no ceiling or one that
    // fits the wide window alone, the run borrows the window from scratch
    // and decodes in one generation, never restarted, each member
    // delivered and each article fetched once.
    #[tokio::test]
    async fn a_later_wider_window_is_lent_from_scratch_and_the_run_is_not_restarted() {
        use weaver_server_pipeline::pipeline::graph::engine::{DecodePool, RUN_OVERHEAD};
        let version = Version::Rar5;
        let wide: u64 = 64 << 20;
        let (mut items, files) = two_packed(version);
        let Item::Packed { codec, .. } = &mut items[3] else {
            unreachable!()
        };
        *codec = Codec {
            dict: wide,
            ..*codec
        };
        let set = set_of((items, files), &split(version, 4_400));
        for (case, ceiling) in [
            ("lent", None),
            ("held to the ceiling", Some(RUN_OVERHEAD + wide)),
        ] {
            let label = format!("{version:?} {case}");
            let mut scenario = Scenario::new(set.posts());
            if let Some(ceiling) = ceiling {
                scenario = scenario.setup(move |pipeline| {
                    pipeline.graph_decode = Arc::new(DecodePool::new(ceiling))
                });
            }
            let (scenario, spoken) = hearing(scenario);
            let graph = run(&scenario).await;
            assert_members(&label, &graph, &set.files);
            assert_fetched_once(&graph);
            let (_, generations) = named(&spoken);
            assert_eq!(
                generations.len(),
                1,
                "{label}: lent, one generation: {generations:?}"
            );
        }
    }

    // A volume in the middle of the second made member, laid after the run
    // started, is posted as another format's volume under the set's name:
    // the store refuses the set while its run is live. The late member
    // fails with the set and is not delivered, and the job does not end
    // Complete.
    #[tokio::test]
    async fn a_refusal_mid_run_fails_late_members_too() {
        let version = Version::Rar5;
        let label = format!("{version:?}");
        let mut set = apart(version);
        let inner = volumes_of(&set, "inner.txt", None);
        assert!(
            volumes_of(&set, "notes.nfo", None)
                .iter()
                .all(|volume| volume < inner.first().unwrap()),
            "{label}"
        );
        let middle = *inner
            .iter()
            .nth(inner.len() / 2)
            .expect("inner.txt's volumes") as usize;
        let other = write(
            &[Item::File("gamma.srt", payload(2 * ARTICLE + 3, 47))],
            &Options::new(Version::Rar4),
        );
        set.volumes[middle].1 = other[0].clone();
        let (scenario, spoken) = hearing(Scenario::new(set.posts()));
        let at_hold = Arc::new(Mutex::new(None));
        let seen = Arc::clone(&at_hold);
        let heard = Arc::clone(&spoken);
        let held = super::super::harness::run_holding_where(
            &scenario,
            |article| article.0 == middle as u32,
            move |_| *seen.lock().unwrap() = Some(closed(&heard, 1)),
        )
        .await;
        assert_eq!(
            *at_hold.lock().unwrap(),
            Some(true),
            "{label}: the run is live, notes.nfo closed, when the volume comes"
        );
        assert_eq!(held.status, Some(JobStatus::Partial), "{label}");
        assert!(
            !held.files.contains_key(&delivered("inner.txt")),
            "{label}: {:?}",
            held.files.keys()
        );
        assert!(held.files.contains_key(&delivered("notes.nfo")), "{label}");
        assert!(!closed(&spoken, 3), "{label}");
    }

    // A RAR4 header posted naming a stored member compressed, under a PAR2
    // set over the set as written: the store makes the member, and its run
    // reads the stored bytes as packed ones. Once the repair puts the
    // header right, the member is the store's to stream: the run restarts
    // without it, in a new generation, and the member is delivered whole.
    #[tokio::test]
    async fn a_repaired_header_turning_a_made_member_stored_mid_run_streams_it() {
        let version = Version::Rar4;
        let (made, plain) = packed(version, Size::Small, "notes.nfo");
        let files: Files = vec![
            ("alpha.mkv", payload(5 * ARTICLE + 11, 41)),
            ("notes.nfo", plain),
            ("beta.mkv", payload(4 * ARTICLE, 44)),
        ];
        let items = vec![
            Item::File(files[0].0, files[0].1.clone()),
            made,
            Item::File(files[2].0, files[2].1.clone()),
        ];
        let set = set_of((items, files), &split(version, 4 * ARTICLE));
        // beta.mkv's first header, its method byte set to a compressed one.
        let mut posted = set.posts();
        let (volume, data) = set.data_of("beta.mkv", None).remove(0);
        let bytes = &mut posted[volume as usize].bytes;
        let name = bytes[..data.start]
            .windows(8)
            .rposition(|window| window == b"beta.mkv")
            .expect("beta.mkv's header");
        let header = name - 32;
        assert_eq!(bytes[header + 2], 0x74, "{name}: a file header");
        assert_eq!(bytes[header + 25], 0x30, "stored");
        bytes[header + 25] = 0x33;
        let size = u16::from_le_bytes([bytes[header + 5], bytes[header + 6]]) as usize;
        let crc = (par2_rs::checksum::crc32(&bytes[header + 2..header + size]) & 0xffff) as u16;
        bytes[header..header + 2].copy_from_slice(&crc.to_le_bytes());
        let mut posts = posted;
        let volumes: Vec<(&str, &[u8])> = set
            .volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        posts.extend(par2_set("silver.horizon", &volumes, 4));
        let (scenario, spoken) = hearing(Scenario::new(posts));
        let graph = run(&scenario).await;
        assert_members("RAR4", &graph, &set.files);
        let (_, generations) = named(&spoken);
        assert!(generations.len() > 1, "the run restarted: {generations:?}");
    }

    // A RAR4 set whose volumes state no number, as `rar` writes it without
    // `-vn`: four volumes or more of a stored file, a compressed one and a
    // stored file.
    fn on_trust() -> Set {
        let mut options = split(Version::Rar4, 2 * ARTICLE);
        options.numbered = false;
        let set = mixed(Version::Rar4, Size::Small, MKV, &options);
        assert!(set.volumes.len() > 3, "{}", set.volumes.len());
        set
    }

    // Posted under names that say nothing, every volume's articles landing
    // out of order at once, so the store holds each volume on trust where
    // its links put it; and posted under its names, last article first.
    // The run reads a volume only once its place is proven, and the job
    // ends Complete with each article fetched once and no stored member
    // named by a run.
    #[tokio::test]
    async fn a_rar4_set_held_on_trust_out_of_order_is_read_where_it_is_placed() {
        let set = on_trust();
        let every: Vec<u32> = (0..set.volumes.len() as u32).collect();
        let nameless: Vec<Post> = obfuscate_volumes(&set.volumes)
            .into_iter()
            .map(|(name, bytes)| Post::plain(&name, bytes))
            .collect();
        for (case, scenario) in [
            (
                "nameless, landing out of order",
                Scenario::new(nameless).reversed(&every),
            ),
            ("posted backwards", Scenario::new(set.posts()).backwards()),
        ] {
            let (scenario, spoken) = hearing(scenario);
            let graph = run(&scenario).await;
            assert_members(case, &graph, &set.files);
            assert_fetched_once(&graph);
            assert_runs_named_no_stored_member(case, &spoken);
        }
    }

    // The same set posted with two neighbours' names swapped, under a PAR2
    // set that names them rightly: the names hold each volume where it is
    // not, and the repair's renames move them behind what the run read. The
    // run is restarted on the moved volumes, and the job ends Complete.
    #[tokio::test]
    #[ignore = "blocked: a RAR4 set held on trust and renamed by PAR2 ends Failed, lost proven bytes"]
    async fn a_rar4_set_held_on_trust_whose_volumes_par2_moves_is_read_again() {
        let set = on_trust();
        let count = set.volumes.len();
        let mut swapped = set.posts();
        let (a, b) = (count - 3, count - 2);
        let (name_a, name_b) = (swapped[a].name.clone(), swapped[b].name.clone());
        swapped[a].name = name_b;
        swapped[b].name = name_a;
        let truth: Vec<(&str, &[u8])> = set
            .volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        swapped.extend(par2_set("silver.horizon", &truth, 4));
        let graph = run(&Scenario::new(swapped)).await;
        assert_members("swapped", &graph, &set.files);
        assert_fetched_once(&graph);
    }

    // `-hp`, the first file compressed: the store keeps the set and its run
    // makes the first file under the job's password.
    #[tokio::test]
    async fn a_set_under_hp_whose_first_file_is_compressed_stays_with_the_store() {
        for version in VERSIONS {
            for (case, mut options) in [
                ("one volume", sealed_headers(version)),
                ("volumes", sealed_headers(version)),
            ] {
                if case == "volumes" {
                    options.split = Some(4 * ARTICLE);
                    options.numbered = true;
                }
                let label = format!("{version:?} {case}");
                let set = set_of(compressed_first(version, Size::Small), &options);
                let graph = run(&Scenario::new(set.posts()).password(SECRET)).await;
                assert_members(&label, &graph, &set.files);
                assert_fetched_once(&graph);
            }
        }
    }

    // Compressed files alone, an article of the second's packed data lost,
    // under a PAR2 set: the repair rebuilds it and the job ends Complete.
    #[tokio::test]
    async fn a_set_of_compressed_files_alone_damaged_in_its_packed_data_is_repaired() {
        for version in VERSIONS {
            let label = format!("{version:?}");
            let set = set_of(compressed_only(version), &split(version, 30_000));
            let (volume, at) = set.data_of("second.txt", None).remove(0);
            let scenario = Scenario::new(set.with_par2()).damaged(volume, article_in(at));
            let graph = run(&scenario).await;
            assert_members(&label, &graph, &set.files);
            assert_fetched_once(&graph);
        }
    }
}
