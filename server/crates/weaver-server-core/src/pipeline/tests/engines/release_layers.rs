// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Release layers below the root (`Tree::opens_as_layer`). Inside what an
// archive held, a container opens when its own name says an archive, or
// when its folder holds only pieces of archive sets and furniture; any
// other container there is a file of the release, delivered as posted.
// What a layer opened below the root holds lands in the folder the layer
// sat in. Every byte arrives unless a test says otherwise.

use std::io::{Cursor, Write};

use ::zip::ZipWriter;
use weaver_graph::Why;

use super::harness::{ARTICLE, Outcome, Post, Scenario, payload, run};
use super::restart::{assert_restarted, run_restarting};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::lines::Lines;
use crate::pipeline::tests::direct_store::scenarios::encrypted_store_set;
use crate::pipeline::tests::direct_store::{multi_member_store_set, single_member_store_set};

const FOLDER: &str = "lantern beacon";
const SECRET: &str = "lantern-layer-secret";

fn delivered(name: &str) -> String {
    format!("{FOLDER}/{name}")
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

// A zip of one member enciphered with AES, the password nowhere offered.
fn sealed_zip(name: &str, bytes: &[u8]) -> Vec<u8> {
    let options = super::zip::stored().with_aes_encryption(::zip::AesMode::Aes256, SECRET);
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer.start_file(name, options).unwrap();
    writer.write_all(bytes).unwrap();
    writer.finish().unwrap().into_inner()
}

// A single-volume stored RAR5 of `entries`.
fn rar(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    multi_member_store_set(entries, 1).remove(0).1
}

// `entries` as an outer stored RAR5 set of `volumes` volumes, posted at the
// root under part names.
fn outer(entries: &[(&str, Vec<u8>)], volumes: usize) -> Scenario {
    Scenario::new(
        multi_member_store_set(entries, volumes)
            .into_iter()
            .enumerate()
            .map(|(index, (_, bytes))| {
                Post::plain(&format!("Ember.part{:02}.rar", index + 1), bytes)
            })
            .collect(),
    )
}

async fn ran(scenario: &Scenario, case: &str) -> Outcome {
    let (lines, guard) = Lines::capture();
    let graph = run(scenario).await;
    drop(guard);
    eprintln!(
        "{case}: {:?} {:?} files {:?} refused {:#?}",
        graph.status,
        graph.failure,
        graph.files.keys().collect::<Vec<_>>(),
        lines.of("object refused"),
    );
    graph
}

// The names delivered, and each one's bytes, against what is expected.
fn assert_delivers(graph: &Outcome, expected: &[(&str, Vec<u8>)], case: &str) {
    let names: Vec<String> = expected.iter().map(|(name, _)| delivered(name)).collect();
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        {
            let mut sorted = names.clone();
            sorted.sort();
            sorted
        },
        "{case}: delivered names"
    );
    for (name, bytes) in expected {
        assert!(
            graph.files.get(&delivered(name)) == Some(bytes),
            "{case}: bytes of {name}"
        );
    }
}

fn tool() -> Vec<u8> {
    payload(4 * ARTICLE + 7, 11)
}
fn readme() -> Vec<u8> {
    b"patch notes\r\n".to_vec()
}
fn patch() -> Vec<u8> {
    payload(2 * ARTICLE + 3, 12)
}
fn skin(seed: u8) -> Vec<u8> {
    payload(ARTICLE + 9, seed)
}
fn table() -> Vec<u8> {
    payload(3 * ARTICLE + 1, 14)
}

// An application's tree, inside an inner RAR inside the outer one: ordinary
// files, data files built on a zip under names of their own, a patch RAR
// beside its notes, and two zips, each in its own folder, that hold a
// member of the same name.
#[tokio::test]
async fn an_application_tree_opens_only_its_named_archives_each_in_its_own_folder() {
    let a_box = zip(&[("cards.xml", payload(ARTICLE, 21))]);
    let supercab = zip(&[("cabinet.bin", payload(ARTICLE, 22))]);
    let lib = zip(&[("codec.bin", payload(ARTICLE, 23))]);
    let patch_rar = rar(&[("patch.bin", patch())]);
    let blue = zip(&[("skin.ini", skin(24))]);
    let red = zip(&[("skin.ini", skin(25))]);
    let app = rar(&[
        ("App/bin/tool.bin", tool()),
        ("App/data/a.box", a_box.clone()),
        ("App/data/b.supercab", supercab.clone()),
        ("App/data/libde265.dat", lib.clone()),
        ("App/Patch/readme.txt", readme()),
        ("App/Patch/Patch.rar", patch_rar),
        ("App/skins/blue/skin.zip", blue),
        ("App/skins/red/skin.zip", red),
    ]);
    let scenario = outer(&[("App.rar", app)], 3);
    let graph = ran(&scenario, "application tree").await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_delivers(
        &graph,
        &[
            ("App/bin/tool.bin", tool()),
            ("App/data/a.box", a_box),
            ("App/data/b.supercab", supercab),
            ("App/data/libde265.dat", lib),
            ("App/Patch/readme.txt", readme()),
            ("App/Patch/patch.bin", patch()),
            ("App/skins/blue/skin.ini", skin(24)),
            ("App/skins/red/skin.ini", skin(25)),
        ],
        "application tree",
    );
}

// A zip enciphered under a name of its own, beside an ordinary file, is a
// file of the release: delivered as posted, with no password asked.
#[tokio::test]
async fn an_enciphered_zip_under_a_name_of_its_own_in_a_tree_is_delivered_as_posted() {
    let vault = sealed_zip("secret.bin", &payload(2 * ARTICLE, 31));
    let app = rar(&[
        ("App/data/tool.bin", tool()),
        ("App/data/vault.dat", vault.clone()),
    ]);
    let graph = ran(&outer(&[("App.rar", app)], 2), "enciphered data").await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert!(graph.failure.is_none(), "{:?}", graph.failure);
    assert_delivers(
        &graph,
        &[("App/data/tool.bin", tool()), ("App/data/vault.dat", vault)],
        "enciphered data",
    );
}

// A gzip beside ordinary files is a compressed file of the release, not a
// layer: it stays packed.
#[tokio::test]
async fn a_gzip_beside_ordinary_files_below_the_root_stays_packed() {
    let gz = super::streams::gzip(&table());
    let app = rar(&[
        ("App/data/table.txt", readme()),
        ("App/data/table.gz", gz.clone()),
    ]);
    let graph = ran(&outer(&[("App.rar", app)], 2), "gzip in a tree").await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_delivers(
        &graph,
        &[("App/data/table.txt", readme()), ("App/data/table.gz", gz)],
        "gzip in a tree",
    );
}

fn book() -> Vec<u8> {
    payload(9 * ARTICLE + 29, 61)
}
fn nfo() -> Vec<u8> {
    b"ember release notes\r\n".to_vec()
}
fn diz() -> Vec<u8> {
    b"ember volume\r\n".to_vec()
}

// The volumes of a RAR set of the book under names that say nothing, beside
// an nfo and a diz: a layer by what it holds alone.
fn nameless_layer() -> Vec<(String, Vec<u8>)> {
    let mut entries: Vec<(String, Vec<u8>)> = single_member_store_set("ember.pdf", &book(), 2)
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| (format!("a9f3c1e{index}"), bytes))
        .collect();
    entries.push(("ember.nfo".to_owned(), nfo()));
    entries.push(("file_id.diz".to_owned(), diz()));
    entries
}

fn borrowed(entries: &[(String, Vec<u8>)]) -> Vec<(&str, Vec<u8>)> {
    entries
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect()
}

#[tokio::test]
async fn a_set_named_nothing_beside_only_furniture_is_a_layer_and_opens() {
    let entries = nameless_layer();
    let graph = ran(&outer(&borrowed(&entries), 2), "nameless layer").await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(graph.files.get(&delivered("ember.pdf")), Some(&book()));
    for (name, _) in entries.iter().filter(|(name, _)| name.starts_with("a9f3")) {
        assert!(!graph.files.contains_key(&delivered(name)), "{name}");
    }
}

// Each order the articles come in: the layer is decided once nothing more
// can come, never on the members that came first.
#[tokio::test]
async fn a_layer_and_a_packed_container_are_decided_the_same_in_either_order() {
    let layer = outer(&borrowed(&nameless_layer()), 3);
    // The container arrives in the first outer volume and the ordinary file
    // beside it in the last, so a decision on what came first would open it.
    let lib = zip(&[("codec.bin", payload(ARTICLE, 41))]);
    let packed = outer(
        &[
            ("App/libde265.dat", lib.clone()),
            ("App/tool.bin", payload(8 * ARTICLE, 42)),
        ],
        3,
    );
    for (scenario, case) in [(layer, "layer"), (packed, "packed")] {
        let forward = ran(&scenario, case).await;
        let backward = ran(&scenario.backwards(), case).await;
        assert!(forward.completed(), "{case}: {:?}", forward.failure);
        assert_eq!(forward.status, backward.status, "{case}");
        assert_eq!(forward.files, backward.files, "{case}");
    }
    let packed = outer(
        &[
            ("App/libde265.dat", lib.clone()),
            ("App/tool.bin", payload(8 * ARTICLE, 42)),
        ],
        3,
    );
    let graph = run(&packed).await;
    assert_eq!(graph.files.get(&delivered("App/libde265.dat")), Some(&lib));
    assert!(!graph.files.contains_key(&delivered("codec.bin")));
    assert!(!graph.files.contains_key(&delivered("App/codec.bin")));
}

// Two sets, each in a folder of its own with its listing, beside an nfo, a
// sample and extras in a folder of their own: each set's folder holds only
// pieces and furniture, so each opens, and its book lands in its folder.
#[tokio::test]
async fn sets_in_folders_of_their_own_open_and_land_in_those_folders() {
    let first = payload(6 * ARTICLE + 5, 71);
    let second = payload(5 * ARTICLE + 9, 72);
    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    for (folder, book) in [("CD1", &first), ("CD2", &second)] {
        for (index, (_, bytes)) in single_member_store_set("ember.pdf", book, 2)
            .into_iter()
            .enumerate()
        {
            entries.push((format!("{folder}/{}{index}", folder.to_lowercase()), bytes));
        }
        entries.push((format!("{folder}/ember.nfo"), nfo()));
    }
    entries.push(("ember.nfo".to_owned(), nfo()));
    entries.push(("Sample/ember-sample.mkv".to_owned(), payload(ARTICLE, 73)));
    entries.push(("Extras/interview.mkv".to_owned(), payload(2 * ARTICLE, 74)));
    let graph = ran(&outer(&borrowed(&entries), 2), "folders").await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(graph.files.get(&delivered("CD1/ember.pdf")), Some(&first));
    assert_eq!(graph.files.get(&delivered("CD2/ember.pdf")), Some(&second));
    assert!(!graph.files.contains_key(&delivered("ember.pdf")));
    assert!(graph.files.contains_key(&delivered("Extras/interview.mkv")));
    assert!(
        graph
            .files
            .contains_key(&delivered("Sample/ember-sample.mkv"))
    );
}

// A RAR named as one, beside a large ordinary file, opens by its name, and
// what it holds lands in its folder.
#[tokio::test]
async fn a_named_rar_beside_an_ordinary_file_opens_in_its_folder() {
    let movie = payload(5 * ARTICLE + 3, 81);
    let bonus = payload(12 * ARTICLE, 82);
    let scenario = outer(
        &[
            ("Movie/bonus.mkv", bonus.clone()),
            ("Movie/movie.rar", rar(&[("movie.mkv", movie.clone())])),
        ],
        3,
    );
    let graph = ran(&scenario, "named rar").await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_delivers(
        &graph,
        &[("Movie/bonus.mkv", bonus), ("Movie/movie.mkv", movie)],
        "named rar",
    );
}

// Restarted after every article, a layer opens and a packed container stays
// packed, as in the run that never restarted.
#[tokio::test]
async fn a_layer_and_a_packed_container_restarted_after_every_article_end_as_they_did() {
    let lib = zip(&[("codec.bin", payload(ARTICLE, 91))]);
    for scenario in [
        outer(&borrowed(&nameless_layer()), 2),
        outer(
            &[
                ("App/libde265.dat", lib.clone()),
                ("App/tool.bin", payload(4 * ARTICLE, 92)),
            ],
            2,
        ),
    ] {
        let whole = run(&scenario).await;
        assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
        let restarted = run_restarting(&scenario, &mut |_| {}).await;
        assert_restarted(&restarted, &whole);
    }
}

// An enciphered set that is a layer by what it holds ends as the same set
// does under a name that says a RAR, which opens as it streams: with the
// password and without it.
#[tokio::test]
async fn an_enciphered_layer_ends_as_the_same_set_named_as_a_rar() {
    let volumes = encrypted_store_set("ember.pdf", &book(), 1, SECRET, Some(SECRET), false);
    let sealed = volumes[0].1.clone();
    let layer = |name: &str| {
        outer(
            &[
                (name, sealed.clone()),
                ("ember.nfo", nfo()),
                ("file_id.diz", diz()),
            ],
            2,
        )
    };
    for password in [Some(SECRET), None] {
        let with = |scenario: Scenario| match password {
            Some(password) => scenario.password(password),
            None => scenario,
        };
        let nameless = ran(&with(layer("a9f3c1e0")), "enciphered layer").await;
        let named = ran(&with(layer("ember.rar")), "enciphered named").await;
        // The same end, each naming its own set where a reason names one.
        assert_eq!(
            format!("{:?}", nameless.status),
            format!("{:?}", named.status).replace("ember.rar", "a9f3c1e0"),
            "{password:?}"
        );
        assert_eq!(
            nameless.files.get(&delivered("ember.pdf")),
            named.files.get(&delivered("ember.pdf")),
            "{password:?}"
        );
        if password.is_some() {
            assert_eq!(nameless.status, Some(JobStatus::Complete));
            assert_eq!(nameless.files.get(&delivered("ember.pdf")), Some(&book()));
        }
    }
}

// The reason a packed container is refused for is a file's: it disclaims
// the member and withholds nothing.
#[test]
fn a_packed_container_is_disclaimed_and_delivered() {
    assert!(Why::Packed.disclaims());
    assert!(!Why::Packed.withholds());
    assert!(Why::Packed.gives_back());
}
