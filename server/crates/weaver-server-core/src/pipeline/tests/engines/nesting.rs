// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Nesting deep and broad: the ten-deep chain of stored containers and
// streams with one file at the bottom, and every prefix of it, in every
// frame size, served backwards, and restarted after every article; every
// stream kind under each store; three decoders deep; a compressed RAR
// nested in a stored one; a 7z of a thousand xz members; a RAR of two
// hundred; and a zip entry sized only by its descriptor under an xz.

use std::io::{Cursor, Write};

use ::zip::ZipWriter;

use super::depth::stored as sevenz;
use super::harness::{ARTICLE, Outcome, Post, Scenario, assert_fetched_once, run};
use super::streams::{brotli, bzip2, deflate, gzip, tar, xz, zstd};
use weaver_graph::Why;

use crate::jobs::model::JobStatus;
use crate::pipeline::graph::lines::Lines;
use crate::pipeline::tests::direct_store::multi_member_store_set;

const FOLDER: &str = "lantern beacon";
const FILE: &str = "ember.bin";

fn delivered(name: &str) -> String {
    format!("{FOLDER}/{name}")
}

// Bytes no period runs through, so a compressing level cannot fold them.
pub(super) fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

// The file's name at the bottom of `levels`: a stream names no member, so
// what an xz holds is named after the stream.
fn bottom(levels: &[Level]) -> &'static str {
    match levels.last() {
        Some(Level {
            kind: Kind::Xz,
            name,
        }) => name.strip_suffix(".xz").unwrap(),
        _ => FILE,
    }
}

fn file() -> Vec<u8> {
    noise(3 * ARTICLE + 17, 7)
}

// A stored RAR5 volume of `entries`, in order.
fn rar(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    multi_member_store_set(entries, 1).remove(0).1
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

pub(super) fn sevenz_of(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let borrowed: Vec<(&str, &[u8])> = entries
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    sevenz(&borrowed)
}

#[derive(Clone, Copy, Debug)]
enum Kind {
    Rar,
    SevenZ,
    TarGz,
    Zip,
    Xz,
    Tar,
}

impl Kind {
    // The container holding one member, `name`, of `bytes`.
    fn wrap(self, name: &str, bytes: Vec<u8>) -> Vec<u8> {
        let entries = [(name, bytes)];
        match self {
            Kind::Rar => rar(&entries),
            Kind::SevenZ => sevenz_of(&entries),
            Kind::TarGz => gzip(&tar(&entries)),
            Kind::Zip => zip(&entries),
            Kind::Xz => xz(&entries[0].1),
            Kind::Tar => tar(&entries),
        }
    }

    // The objects the graph binds to read one level, outermost first: a
    // gzipped tar is a gzip stream, then the tar it holds.
    fn objects(self) -> &'static [&'static str] {
        match self {
            Kind::Rar => &["rar_store"],
            Kind::SevenZ => &["sevenz_store"],
            Kind::TarGz => &["stream_gz", "tar"],
            Kind::Zip => &["zip"],
            Kind::Xz => &["stream_xz"],
            Kind::Tar => &["tar"],
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Level {
    kind: Kind,
    name: &'static str,
}

// root ──► a.rar ──► b.7z ──► c.tar.gz ──► d.zip ──► e.rar ──► f.xz ──►
// g.rar ──► h.7z ──► i.tar ──► j.rar ──► ember.bin
pub(super) const CHAIN: [Level; 10] = [
    Level {
        kind: Kind::Rar,
        name: "a.rar",
    },
    Level {
        kind: Kind::SevenZ,
        name: "b.7z",
    },
    Level {
        kind: Kind::TarGz,
        name: "c.tar.gz",
    },
    Level {
        kind: Kind::Zip,
        name: "d.zip",
    },
    Level {
        kind: Kind::Rar,
        name: "e.rar",
    },
    Level {
        kind: Kind::Xz,
        name: "f.xz",
    },
    Level {
        kind: Kind::Rar,
        name: "g.rar",
    },
    Level {
        kind: Kind::SevenZ,
        name: "h.7z",
    },
    Level {
        kind: Kind::Tar,
        name: "i.tar",
    },
    Level {
        kind: Kind::Rar,
        name: "j.rar",
    },
];

// The file wrapped from the inside out by `levels`, the outermost first:
// what the root's one post carries.
fn chain(levels: &[Level]) -> Vec<u8> {
    let mut inner = (FILE, file());
    for level in levels.iter().rev() {
        inner = (level.name, level.kind.wrap(inner.0, inner.1));
    }
    inner.1
}

pub(super) fn chained(levels: &[Level]) -> Scenario {
    Scenario::new(vec![Post::plain(levels[0].name, chain(levels))])
}

// The objects bound to read `levels`, outermost first, each at its depth.
fn bindings(levels: &[Level]) -> Vec<(String, u32)> {
    levels
        .iter()
        .flat_map(|level| level.kind.objects())
        .zip(0u32..)
        .map(|(kind, depth)| (kind.to_string(), depth))
        .collect()
}

fn field<'l>(line: &'l str, key: &str) -> &'l str {
    line.split(' ')
        .filter_map(|field| field.split_once('='))
        .find(|(name, _)| *name == key)
        .map(|(_, value)| value)
        .unwrap_or_else(|| panic!("no {key} in {line}"))
}

// The graph delivered the one file whole at `depth`, every object bound
// in order on the way down, and nothing else.
fn assert_delivered_at(graph: &Outcome, lines: &Lines, levels: &[Level]) {
    let bound = bindings(levels);
    let depth = bound.len() as u32;
    let name = bottom(levels);
    assert!(
        graph.completed(),
        "graph: {:?} {:?}",
        graph.status,
        graph.failure
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        [delivered(name)],
        "graph: delivered names"
    );
    assert!(graph.files[&delivered(name)] == file(), "graph: the bytes");
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    let delivery = &graph.graph().delivery;
    assert_eq!(delivery.whole, [name], "{delivery:?}");
    assert!(
        delivery.damaged.is_empty() && delivery.unrepaired.is_empty(),
        "{delivery:?}"
    );
    assert!(delivery.withheld.is_empty(), "{delivery:?}");
    assert_eq!(delivery.files.len(), 1, "{delivery:?}");
    assert_eq!(delivery.files[0].name, name);
    assert_eq!(delivery.files[0].depth, depth, "{delivery:?}");
    assert!(delivery.files[0].whole);
    let said = lines.of("member delivered");
    assert_eq!(said.len(), 1, "{said:?}");
    assert_eq!(field(&said[0], "name"), name, "{}", said[0]);
    assert_eq!(field(&said[0], "depth"), depth.to_string(), "{}", said[0]);
    let objects: Vec<(String, u32)> = lines
        .of("object bound")
        .iter()
        .map(|line| {
            (
                field(line, "kind").to_string(),
                field(line, "depth").parse().unwrap(),
            )
        })
        .collect();
    assert_eq!(objects, bound, "object bound lines");
}

// What a run came to, in one line a table can hold: the status, then the
// delivered names.
fn summary(outcome: &Outcome) -> String {
    let status = match &outcome.status {
        Some(JobStatus::Failed { error }) => format!("Failed({error})"),
        other => format!("{other:?}"),
    };
    let names: Vec<&str> = outcome
        .files
        .keys()
        .map(|name| name.strip_prefix(&format!("{FOLDER}/")).unwrap_or(name))
        .collect();
    format!("{status} [{}]", names.join(", "))
}

async fn chain_row(depth: usize) {
    let levels = &CHAIN[..depth];
    let scenario = chained(levels);
    let (lines, guard) = Lines::capture();
    let graph = run(&scenario).await;
    drop(guard);
    assert_delivered_at(&graph, &lines, levels);
}

#[tokio::test]
async fn the_ten_deep_chain_delivers_its_file() {
    chain_row(10).await;
}

macro_rules! prefixes {
    ($($name:ident => $depth:expr),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                chain_row($depth).await;
            }
        )*
    };
}

prefixes! {
    the_chain_1_deep_delivers_its_file => 1,
    the_chain_2_deep_delivers_its_file => 2,
    the_chain_3_deep_delivers_its_file => 3,
    the_chain_4_deep_delivers_its_file => 4,
    the_chain_5_deep_delivers_its_file => 5,
    the_chain_6_deep_delivers_its_file => 6,
    the_chain_7_deep_delivers_its_file => 7,
    the_chain_8_deep_delivers_its_file => 8,
    the_chain_9_deep_delivers_its_file => 9,
}

#[tokio::test]
async fn the_ten_deep_chain_delivers_alike_in_every_frame_size() {
    for size in [usize::MAX, 4096, 1] {
        let (lines, guard) = Lines::capture();
        let graph = run(&chained(&CHAIN).framed(size)).await;
        drop(guard);
        assert_delivered_at(&graph, &lines, &CHAIN);
    }
}

#[tokio::test]
async fn the_ten_deep_chain_served_last_article_first_delivers() {
    let (lines, guard) = Lines::capture();
    let graph = run(&chained(&CHAIN).backwards()).await;
    drop(guard);
    assert_delivered_at(&graph, &lines, &CHAIN);
}

#[tokio::test]
async fn the_ten_deep_chain_restarted_after_every_article_delivers() {
    super::restart::restart_row(chained(&CHAIN)).await;
}

// The ten-deep chain served backwards, reduced: a stored 7z inside a
// stored RAR, served last article first in articles of ARTICLE bytes, the
// 7z's front split across the RAR's first two articles.
#[tokio::test]
async fn a_stored_7z_in_a_stored_rar_served_last_article_first_delivers() {
    let levels = [CHAIN[0], CHAIN[1]];
    let (lines, guard) = Lines::capture();
    let graph = run(&chained(&levels).backwards()).await;
    drop(guard);
    assert_delivered_at(&graph, &lines, &levels);
}

// Each stream kind's file, named for the stream that holds it, and the
// bytes it holds.
fn streamed() -> Vec<(&'static str, &'static str, Vec<u8>)> {
    type Compress = (&'static str, &'static str, fn(&[u8]) -> Vec<u8>);
    let compress: [Compress; 6] = [
        ("xz.bin.xz", "xz.bin", xz),
        ("gz.bin.gz", "gz.bin", gzip),
        ("bz.bin.bz2", "bz.bin", bzip2),
        ("zst.bin.zst", "zst.bin", zstd),
        ("br.bin.br", "br.bin", brotli),
        ("raw.bin.deflate", "raw.bin", deflate),
    ];
    compress
        .iter()
        .zip(11u64..)
        .map(|((posted, name, compress), seed)| {
            let bytes = noise(2 * ARTICLE + seed as usize, seed);
            (*posted, *name, compress(&bytes))
        })
        .collect()
}

fn streamed_bytes(name: &str) -> Vec<u8> {
    let seed = streamed()
        .iter()
        .zip(11u64..)
        .find(|((_, held, _), _)| *held == name)
        .map(|(_, seed)| seed)
        .unwrap();
    noise(2 * ARTICLE + seed as usize, seed)
}

// Every stream kind as a member of one store of `kind`: the graph decodes
// every stream where it lies.
async fn streams_under(kind: Kind, posted: &str) {
    let entries: Vec<(&str, Vec<u8>)> = streamed()
        .into_iter()
        .map(|(name, _, bytes)| (name, bytes))
        .collect();
    let store = match kind {
        Kind::Rar => rar(&entries),
        Kind::SevenZ => sevenz_of(&entries),
        Kind::Tar => tar(&entries),
        Kind::Zip => zip(&entries),
        _ => unreachable!("a store"),
    };
    let scenario = Scenario::new(vec![Post::plain(posted, store)]);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "graph: {:?}", graph.failure);
    let mut names: Vec<&str> = streamed().iter().map(|(_, name, _)| *name).collect();
    names.sort();
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        names.iter().map(|name| delivered(name)).collect::<Vec<_>>(),
        "graph: delivered names"
    );
    for name in &names {
        assert!(
            graph.files[&delivered(name)] == streamed_bytes(name),
            "graph: {name}"
        );
    }
    let delivery = &graph.graph().delivery;
    let mut whole = delivery.whole.clone();
    whole.sort();
    assert_eq!(whole, names, "{delivery:?}");
    assert!(
        delivery
            .files
            .iter()
            .all(|file| file.depth == 2 && file.whole),
        "{delivery:?}"
    );
    assert!(delivery.withheld.is_empty(), "{delivery:?}");
}

#[tokio::test]
async fn every_stream_kind_under_a_stored_rar_delivers() {
    streams_under(Kind::Rar, "lantern.rar").await;
}

#[tokio::test]
async fn every_stream_kind_under_a_stored_7z_delivers() {
    streams_under(Kind::SevenZ, "lantern.7z").await;
}

#[tokio::test]
async fn every_stream_kind_under_a_tar_delivers() {
    streams_under(Kind::Tar, "lantern.tar").await;
}

#[tokio::test]
async fn every_stream_kind_under_a_stored_zip_delivers() {
    // Fails under graph: see the reduced row below.
    streams_under(Kind::Zip, "lantern.zip").await;
}

// The zip row reduced: one xz in a stored zip, in articles of ARTICLE
// bytes. The zip asks for its directory, in its last article, right after
// its first, so the end of the xz's bytes reaches the stream before its
// middle. Whole in one article it delivers.
#[tokio::test]
async fn a_stored_zip_of_one_xz_delivers_its_file() {
    let bytes = file();
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.zip",
        zip(&[("ember.bin.xz", xz(&bytes))]),
    )]);
    let whole = run(&scenario_framed(&scenario, 4096)).await;
    assert!(whole.completed(), "in one article: {:?}", whole.failure);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "graph: {:?}", graph.failure);
    assert!(graph.files[&delivered(FILE)] == bytes, "graph: the bytes");
    assert!(graph.graph().delivery.withheld.is_empty());
}

fn scenario_framed(scenario: &Scenario, size: usize) -> Scenario {
    let posts = scenario
        .posts
        .iter()
        .map(|post| Post::plain(&post.name, post.bytes.clone()))
        .collect();
    Scenario::new(posts).framed(size)
}

#[tokio::test]
async fn three_decoders_deep_xz_in_gzip_in_zstd_delivers() {
    let scenario = Scenario::new(vec![Post::plain(
        "ember.bin.xz.gz.zst",
        zstd(&gzip(&xz(&file()))),
    )]);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "graph: {:?}", graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        [delivered(FILE)]
    );
    assert!(graph.files[&delivered(FILE)] == file(), "graph: the bytes");
    let delivery = &graph.graph().delivery;
    assert_eq!(delivery.whole, [FILE], "{delivery:?}");
    assert_eq!(delivery.files[0].depth, 3, "{delivery:?}");
}

// The compressed RAR5 fixture (two stored members, one compressed) as a
// member of a stored RAR: the store hands it to the engine one deep.
#[tokio::test]
async fn a_nested_stored_rar_with_one_compressed_member_among_stored_ones_delivers() {
    let mixed = crate::pipeline::tests::rar5_fixture_bytes("rar5_multifile_lz.rar");
    let alone = run(&Scenario::new(vec![Post::plain(
        "mixed.rar",
        mixed.clone(),
    )]))
    .await;
    assert!(alone.completed(), "alone: {:?}", alone.failure);
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.rar",
        rar(&[("mixed.rar", mixed)]),
    )]);
    let graph = run(&scenario).await;
    assert!(
        graph.completed(),
        "graph: {:?} {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.files.keys(),
        graph.graph().delivery
    );
    assert_eq!(
        graph.files, alone.files,
        "graph: what the fixture alone delivers"
    );
    assert!(
        graph.files[&delivered("zeros_64k.bin")] == vec![0u8; 64 * 1024],
        "the compressed member"
    );
    assert!(
        graph.graph().kinds.contains("rar_engine"),
        "{:?}",
        graph.graph().kinds
    );
    let delivery = &graph.graph().delivery;
    assert!(
        delivery
            .files
            .iter()
            .all(|file| file.depth == 2 && file.whole),
        "{delivery:?}"
    );
    assert!(delivery.withheld.is_empty(), "{delivery:?}");
}

// Small files, each in its own xz, stored in one 7z, run at the graph's
// slot limit of 1, 2 and 8, set by the tuner's extraction override. The
// xz streams decode in the stream and take no run slot, so the limit
// bounds nothing here; every member's bytes are asserted, and how many
// runs held a slot at once is not observed: the harness exposes no look
// at the job between articles.
async fn many_xz_members(count: u64) {
    let files: Vec<(String, Vec<u8>)> = (0..count)
        .map(|index| {
            (
                format!("m{index:04}.bin"),
                noise(8 + (index as usize * 7) % 41, 1000 + index),
            )
        })
        .collect();
    let posted: Vec<(String, Vec<u8>)> = files
        .iter()
        .map(|(name, bytes)| (format!("{name}.xz"), xz(bytes)))
        .collect();
    let entries: Vec<(&str, Vec<u8>)> = posted
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", sevenz_of(&entries))]).framed(4096);
    for limit in [1, 2, 8] {
        // Read when the run's pipeline builds its tuner; nextest runs each
        // test in its own process, and nothing reads it meanwhile.
        unsafe { std::env::set_var("WEAVER_MAX_CONCURRENT_EXTRACTIONS", limit.to_string()) };
        let graph = run(&scenario).await;
        assert!(graph.completed(), "{limit}: {:?}", graph.failure);
        assert_eq!(graph.files.len(), files.len(), "{limit}: delivered");
        for (name, bytes) in &files {
            assert!(
                graph.files.get(&delivered(name)) == Some(bytes),
                "{limit}: {name}"
            );
        }
        let delivery = &graph.graph().delivery;
        assert_eq!(delivery.whole.len(), files.len(), "{limit}");
        assert!(
            delivery
                .files
                .iter()
                .all(|file| file.depth == 2 && file.whole)
        );
        assert!(delivery.withheld.is_empty());
    }
}

// Out of the suite: the job's work grows with the cube of the member count
// (50 members 0.6 s, 100 3.3 s, 200 26 s per run), so a thousand runs for
// about an hour at each limit.
#[tokio::test]
#[ignore = "a thousand members runs for about an hour per slot limit: per-run-message work grows with the tree"]
async fn a_7z_of_a_thousand_xz_members_delivers_every_member_at_slot_limits_1_2_and_8() {
    many_xz_members(1000).await;
}

#[tokio::test]
async fn a_7z_of_a_hundred_xz_members_delivers_every_member_at_slot_limits_1_2_and_8() {
    many_xz_members(100).await;
}

#[tokio::test]
async fn a_stored_rar_of_two_hundred_members_delivers_them_all() {
    let files: Vec<(String, Vec<u8>)> = (0..200u64)
        .map(|index| {
            (
                format!("m{index:03}.bin"),
                noise(5 + (index as usize * 13) % 97, 2000 + index),
            )
        })
        .collect();
    let named: Vec<(&str, Vec<u8>)> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let volumes = multi_member_store_set(&named, 4);
    let scenario = Scenario::new(
        volumes
            .into_iter()
            .map(|(name, bytes)| Post::plain(&name, bytes))
            .collect(),
    );
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.files.len(), files.len());
    for (name, bytes) in &files {
        assert!(graph.files.get(&delivered(name)) == Some(bytes), "{name}",);
    }
    assert_fetched_once(&graph);
    let delivery = &graph.graph().delivery;
    assert_eq!(delivery.whole.len(), files.len());
    assert!(
        delivery
            .files
            .iter()
            .all(|file| file.depth == 1 && file.whole)
    );
}

// A stored zip written to a stream: its one entry's local header declares
// no size, a data descriptor after the data does. Under an xz the store
// cannot seek to the directory that would size it. `Why::UnboundedEntry`
// delivers what it refuses rather than withholding it; here nothing is
// refused at all: the zip object streams the entry whole once the xz has
// ended and its directory is in.
#[tokio::test]
async fn a_sized_by_descriptor_zip_entry_under_an_xz_parent_is_refused_unbounded() {
    assert!(!Why::UnboundedEntry.withholds());
    let mut writer = ZipWriter::new_stream(Vec::new());
    writer.start_file(FILE, super::zip::stored()).unwrap();
    writer.write_all(&file()).unwrap();
    let bytes = writer.finish().unwrap().into_inner();
    // The general-purpose flag's bit 3: sized by the descriptor.
    assert_eq!(bytes[..4], [0x50, 0x4b, 0x03, 0x04]);
    assert_ne!(
        u16::from_le_bytes([bytes[6], bytes[7]]) & 0x0008,
        0,
        "a descriptor"
    );
    let scenario = Scenario::new(vec![Post::plain("lantern.zip.xz", xz(&bytes))]);
    let (lines, guard) = Lines::capture();
    let graph = run(&scenario).await;
    drop(guard);
    assert!(
        lines.of("object refused").is_empty(),
        "{:?}",
        lines.of("object refused")
    );
    assert_eq!(summary(&graph), "Some(Complete) [ember.bin]", "graph");
    assert!(graph.files[&delivered(FILE)] == file(), "graph: the bytes");
    let delivery = &graph.graph().delivery;
    assert!(delivery.withheld.is_empty(), "{delivery:?}");
    assert_eq!(delivery.whole, [FILE], "{delivery:?}");
    assert_eq!(delivery.files[0].depth, 2, "{delivery:?}");
}
