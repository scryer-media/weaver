// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Deep and broad at once: one stored container two deep holding an
// enciphered RAR5 set, an AES 7z, a plain file and the PAR2 set over both
// archives, damaged inside each archive and repaired at depth; the
// extraction caps breached by a member two deep and by entries counted
// across levels; and the rows where an object gives a member up, to the
// engine or as a leaf, restarted after every article. Two deep as well: a
// PAR3 set repairing the archive beside it, an AES zip, one output name at
// two depths, a RAR set rebuilt from its `.rev`, a compressed RAR whose
// password never comes, and the recovery volumes and index a restart cannot
// read back. Below the root a namespace binds, claims, moves and gives back
// as the root's does, a locked 7z ends as at the root, a repair survives a
// crash, and an archive posted again under another name is that archive.

use sevenz_turbo::encoder_options::Lzma2Options;
use sevenz_turbo::{EncoderConfiguration, EncoderMethod};
use weaver_graph::Why;
use weaver_graph::recovery::RepairOutcome;

use super::caps::{FLOOR, assert_refused, limits, zeros_zstd};
use super::depth::stored as sevenz_stored;
use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, kinds, par2_set, par3_set, payload, run,
};
use super::rar::posts as rar_posts;
use super::rar_rev::{body as rev_body, rar5_revs};
use super::restart::{
    Down, assert_restarted, repairs_and_end, restart_row, run_restarted_while_repairing,
    run_restarting, run_restarting_at,
};
use super::sevenz::{SECRET, container, members as sevenz_members};
use super::streams::{tar, xz};
use super::zip::{archive as zip_archive, members as zip_members, stored as zip_stored};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::scenarios::encrypted_store_set;
use crate::pipeline::tests::direct_store::{
    multi_member_store_set, obfuscate_volumes, protected_store_set, single_member_store_set,
};

pub(super) fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// A stored RAR5 volume of `entries`, in order.
fn rar(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    multi_member_store_set(entries, 1).remove(0).1
}

fn sevenz(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let borrowed: Vec<(&str, &[u8])> = entries.iter().map(|(n, b)| (*n, b.as_slice())).collect();
    sevenz_stored(&borrowed)
}

// `entries` in a stored RAR, `level.rar`, in a stored 7z posted as
// `lantern.7z`: what they hold sits two deep.
pub(super) fn two_deep(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    sevenz(&[("level.rar", rar(entries))])
}

// Every article of `outer` lying wholly inside the bytes of `inner`, past
// its front, so what the member is can still be told from its first bytes.
pub(super) fn articles_inside(outer: &[u8], inner: &[u8]) -> Vec<u32> {
    let at = outer
        .windows(inner.len())
        .position(|window| window == inner)
        .expect("a stored container holds its member's bytes as they are");
    (at / ARTICLE + 1..)
        .take_while(|ordinal| (ordinal + 1) * ARTICLE <= at + inner.len())
        .map(|ordinal| ordinal as u32)
        .collect()
}

fn beacon() -> Vec<u8> {
    payload(4 * ARTICLE + 7, 91)
}

fn candle() -> Vec<u8> {
    payload(2 * ARTICLE + 5, 92)
}

// The broad set: an enciphered two-volume RAR5 set holding `beacon.bin`, an
// AES stored 7z holding `ember.bin` and `quill.bin`, `candle.bin`, and
// `inner.par2` with four recovery blocks over both archives.
fn broad() -> Vec<(String, Vec<u8>)> {
    let mut entries = encrypted_store_set("beacon.bin", &beacon(), 2, SECRET, Some(SECRET), false);
    entries.push((
        "cinder.7z".to_string(),
        container(EncoderConfiguration::new(EncoderMethod::COPY), Some(SECRET)),
    ));
    let covered: Vec<(&str, &[u8])> = entries
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let set: Vec<(String, Vec<u8>)> = par2_set("inner", &covered, 4)
        .into_iter()
        .map(|post| (post.name, post.bytes))
        .collect();
    entries.push(("candle.bin".to_string(), candle()));
    entries.extend(set);
    entries
}

// The broad set two deep, under the job's password, with one article
// served damaged inside the second RAR volume and one inside the 7z.
fn broad_two_deep() -> Scenario {
    let entries = broad();
    let named: Vec<(&str, Vec<u8>)> = entries
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let outer = two_deep(&named);
    let inside = |wanted: fn(&str) -> bool| {
        let (_, bytes) = entries.iter().find(|(name, _)| wanted(name)).unwrap();
        articles_inside(&outer, bytes)[0]
    };
    let (part, cinder) = (
        inside(|name| name.ends_with("part02.rar")),
        inside(|name| name == "cinder.7z"),
    );
    Scenario::new(vec![Post::plain("lantern.7z", outer)])
        .password(SECRET)
        .damaged(0, part)
        .damaged(0, cinder)
}

fn assert_broad_whole(graph: &Outcome) {
    assert!(
        graph.completed(),
        "graph: {:?} {:?}",
        graph.status,
        graph.failure
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        [
            "beacon.bin",
            "candle.bin",
            "ember.bin",
            "inner.par2",
            "inner.vol0+4.par2",
            "quill.bin"
        ]
        .map(delivered),
        "graph: delivered names"
    );
    assert!(
        graph.files[&delivered("beacon.bin")] == beacon(),
        "beacon.bin"
    );
    assert!(
        graph.files[&delivered("candle.bin")] == candle(),
        "candle.bin"
    );
    for (name, bytes) in sevenz_members() {
        assert!(graph.files[&delivered(name)] == bytes, "{name}");
    }
}

// Both archives are repaired at depth by the set beside them, one slice
// each, and opened on the job's password; nothing is asked for twice.
#[tokio::test]
async fn an_enciphered_rar_and_an_aes_7z_two_deep_are_repaired_by_the_set_beside_them() {
    let graph = run(&broad_two_deep()).await;
    assert_broad_whole(&graph);
    assert_eq!(kinds(&graph), ["par2", "rar_store", "sevenz_store"]);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 2 })]
    );
    assert_eq!(graph.graph().claims["lantern.7z"], Claim::Damaged);
}

#[tokio::test]
async fn the_broad_set_two_deep_restarted_after_every_article_is_repaired_with_no_refetch() {
    restart_row(broad_two_deep()).await;
}

// Two sets two deep: `ember.bin` under one recovery block with two of its
// articles damaged, `quill.bin` under two with one; an article two deep
// spans two slices of its member. The short set's file is listed
// unrepaired, the other is rebuilt, and the job is Partial for a Repair
// failure.
#[tokio::test]
async fn two_sets_two_deep_one_short_of_blocks_end_partial_with_the_other_sets_file_whole() {
    let ember = payload(6 * ARTICLE + 5, 93);
    let quill = payload(5 * ARTICLE + 3, 94);
    let mut entries: Vec<(String, Vec<u8>)> = vec![("ember.bin".into(), ember.clone())];
    entries.extend(
        par2_set("ember", &[("ember.bin", ember.as_slice())], 1)
            .into_iter()
            .map(|post| (post.name, post.bytes)),
    );
    entries.push(("quill.bin".into(), quill.clone()));
    entries.extend(
        par2_set("quill", &[("quill.bin", quill.as_slice())], 2)
            .into_iter()
            .map(|post| (post.name, post.bytes)),
    );
    let named: Vec<(&str, Vec<u8>)> = entries
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let outer = two_deep(&named);
    let short = articles_inside(&outer, &ember);
    let whole = articles_inside(&outer, &quill);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)])
        .damaged(0, short[0])
        .damaged(0, short[2])
        .damaged(0, whole[1]);
    let graph = run(&scenario).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(
        graph.failure_kind,
        Some(HistoryFailureKind::Repair),
        "{:?}",
        graph.graph().delivery
    );
    assert!(
        graph.files[&delivered("quill.bin")] == quill,
        "quill.bin rebuilt"
    );
    assert_eq!(graph.graph().delivery.unrepaired, ["ember.bin"]);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A zstd of zeros two deep inflates past the ratio: the job fails at
// extraction naming the ratio, as it does with the stream at the root.
#[tokio::test]
async fn a_member_two_deep_inflating_past_the_ratio_fails_the_job_naming_ratio() {
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.7z",
        two_deep(&[("ember.bin.zst", zeros_zstd(4))]),
    )])
    .limits(limits(|limits| {
        limits.max_ratio = 4;
        limits.ratio_floor_bytes = FLOOR;
    }));
    let graph = run(&scenario).await;
    assert_refused(&graph, "ratio");
}

// An xz two deep writes past the job's byte cap: one budget counts every
// byte an object produces at any depth.
#[tokio::test]
async fn a_member_two_deep_writing_past_the_job_byte_cap_fails_naming_job_bytes() {
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.7z",
        two_deep(&[("ember.bin.xz", xz(&payload(40 * ARTICLE, 95)))]),
    )])
    .limits(limits(|limits| limits.max_job_bytes = 1000));
    let graph = run(&scenario).await;
    assert_refused(&graph, "job_bytes");
}

// Three members beside a tar of three, two deep: no namespace holds more
// than four, and the job's one count of entries passes five.
#[tokio::test]
async fn entries_counted_across_levels_fail_the_job_naming_entries() {
    let leaves: Vec<(String, Vec<u8>)> = (0..3)
        .map(|index| (format!("ember{index}.bin"), payload(ARTICLE + index, 96)))
        .collect();
    let borrowed: Vec<(&str, Vec<u8>)> = leaves
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let mut entries: Vec<(&str, Vec<u8>)> = vec![
        ("quill0.bin", payload(ARTICLE, 97)),
        ("quill1.bin", payload(ARTICLE, 98)),
        ("quill2.bin", payload(ARTICLE, 99)),
    ];
    entries.push(("cinder.tar", tar(&borrowed)));
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", two_deep(&entries))])
        .limits(limits(|limits| limits.max_entries = 5));
    let graph = run(&scenario).await;
    assert_refused(&graph, "entries");
}

// An LZMA2 7z two deep beside a plain file: the store gives it up
// compressed and the engine delivers its members, restarted after every
// article, from what is on disk.
#[tokio::test]
async fn an_lzma2_7z_two_deep_handed_to_the_engine_restarted_after_every_article_delivers_its_members()
 {
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.7z",
        two_deep(&[
            (
                "cinder.7z",
                container(Lzma2Options::from_level(1).into(), None),
            ),
            ("candle.bin", candle()),
        ]),
    )]);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(graph.files[&delivered("candle.bin")] == candle());
    for (name, bytes) in sevenz_members() {
        assert!(graph.files[&delivered(name)] == bytes, "{name}");
    }
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    restart_row(scenario).await;
}

// A stored 7z two deep with a damaged member no set covers: the archive is
// refused and lives on as a leaf beside the member it closed whole, and the
// job is Partial; restarted after every article it ends the same.
#[tokio::test]
async fn an_uncovered_damaged_archive_two_deep_refused_to_a_leaf_restarted_after_every_article_ends_partial_alike()
 {
    let members = sevenz_members();
    let inner = sevenz(&members);
    let outer = two_deep(&[("cinder.7z", inner)]);
    let ordinal = articles_inside(&outer, &members[0].1)[0];
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]).damaged(0, ordinal);
    let whole = run(&scenario).await;
    assert_eq!(
        whole.status,
        Some(JobStatus::Partial),
        "{:?}",
        whole.failure
    );
    assert_eq!(
        whole.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("cinder.7z"), delivered("quill.bin")]
    );
    assert!(whole.files[&delivered("quill.bin")] == members[1].1);
    assert!(whole.refetched().is_empty(), "{:?}", whole.refetched());
    let restarted = run_restarting(&scenario, &mut |_| {}).await;
    assert_restarted(&restarted, &whole);
}

// An xz two deep inflates to 64 KiB under a member cap of 16 KiB; the RARs
// and the xz around it are a few hundred bytes each, under it. The cap
// holds for what an object produces at any depth.
#[tokio::test]
async fn a_member_two_deep_writing_past_the_member_byte_cap_fails_naming_member_bytes() {
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.7z",
        two_deep(&[("ember.bin.xz", xz(&[0x5a; 64 * 1024]))]),
    )])
    .limits(limits(|limits| limits.max_member_bytes = 16 * 1024));
    let graph = run(&scenario).await;
    assert_refused(&graph, "member_bytes");
    assert!(
        graph
            .failure
            .as_deref()
            .is_some_and(|failure| failure.ends_with(" bytes, limit is 16384")),
        "{:?}",
        graph.failure
    );
}

// A stored 7z two deep beside the PAR3 set over it, one article inside it
// served damaged: the set repairs it at depth and nothing is asked for
// twice.
#[tokio::test]
async fn a_damaged_7z_two_deep_is_repaired_by_the_par3_set_beside_it() {
    let cinder = container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    let set = par3_set("inner", &[("cinder.7z", cinder.as_slice())], 2);
    let mut entries: Vec<(&str, Vec<u8>)> = vec![("cinder.7z", cinder.clone())];
    entries.extend(
        set.iter()
            .map(|post| (post.name.as_str(), post.bytes.clone())),
    );
    let outer = two_deep(&entries);
    let damaged = articles_inside(&outer, &cinder)[0];
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]).damaged(0, damaged);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    let mut names: Vec<String> = sevenz_members()
        .iter()
        .map(|(name, _)| delivered(name))
        .chain(set.iter().map(|post| delivered(&post.name)))
        .collect();
    names.sort();
    assert_eq!(graph.files.keys().cloned().collect::<Vec<_>>(), names);
    for (name, bytes) in sevenz_members() {
        assert!(graph.files[&delivered(name)] == bytes, "{name}");
    }
    assert!(
        matches!(
            graph.graph().outcomes[..],
            [Some(RepairOutcome::Repaired { .. })]
        ),
        "{:?}",
        graph.graph().outcomes
    );
    assert!(kinds(&graph).contains(&"par3"), "{:?}", kinds(&graph));
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A RAR5 volume carrying its recovery record, two deep, one article inside
// its member served damaged: the record repairs the volume at depth, its
// member is delivered whole and nothing is asked for twice.
#[tokio::test]
async fn a_rar5_volume_two_deep_is_repaired_from_its_own_recovery_record() {
    let body = beacon();
    let volume = protected_store_set("beacon.bin", &body, 1, 1).remove(0).1;
    let outer = two_deep(&[("cinder.rar", volume)]);
    let damaged = articles_inside(&outer, &body)[0];
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]).damaged(0, damaged);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(graph.files.get(&delivered("beacon.bin")), Some(&body));
    assert_eq!(
        graph.graph().rulings,
        vec![(
            "rar_record".to_string(),
            Some(RepairOutcome::Repaired { slices: 1 })
        )]
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// An AES zip two deep, opened on the job's password by the engine's run.
#[tokio::test]
async fn an_aes_zip_two_deep_delivers_its_members() {
    let enciphered = zip_stored().with_aes_encryption(::zip::AesMode::Aes256, SECRET);
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.7z",
        two_deep(&[("cinder.zip", zip_archive(enciphered))]),
    )])
    .password(SECRET);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        ["ember.bin", "quill.bin"].map(delivered)
    );
    for (name, bytes) in zip_members() {
        assert!(graph.files[&delivered(name)] == bytes, "{name}");
    }
    assert_eq!(
        kinds(&graph),
        ["rar_store", "sevenz_store", "zip", "zip_engine"]
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// `quill.bin` one deep and two deep in stored RARs, served in order: the one
// in `level.rar` is announced first and holds the name until the shallower
// one is announced, which takes it; the holder moves to `quill (1).bin`
// with its file, and nothing is written over.
#[tokio::test]
async fn one_name_one_and_two_deep_goes_to_the_shallower_and_the_deeper_holder_moves() {
    let shallow = payload(3 * ARTICLE + 1, 102);
    let deeper = payload(2 * ARTICLE + 9, 103);
    let outer = rar(&[
        ("level.rar", rar(&[("quill.bin", deeper.clone())])),
        ("quill.bin", shallow.clone()),
    ]);
    let scenario = Scenario::new(vec![Post::plain("lantern.rar", outer)]);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        ["quill (1).bin", "quill.bin"].map(delivered)
    );
    assert!(graph.files[&delivered("quill.bin")] == shallow, "quill.bin");
    assert!(
        graph.files[&delivered("quill (1).bin")] == deeper,
        "quill (1).bin"
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// Every article of `outer` holding a byte of `inner`.
fn articles_holding(outer: &[u8], inner: &[u8]) -> Vec<(u32, u32)> {
    let at = outer
        .windows(inner.len())
        .position(|window| window == inner)
        .expect("a stored container holds its member's bytes as they are");
    (at / ARTICLE..=(at + inner.len() - 1) / ARTICLE)
        .map(|ordinal| (0, ordinal as u32))
        .collect()
}

// `ember.bin` after `set` in a stored RAR, `level.rar`, in a stored RAR
// posted as `lantern.rar`, served in order, one article of `ember.bin`
// damaged. The pipeline goes down after the article `down` picks of those
// holding `lost`, before the damaged article comes, and the file of `lost`
// is gone: the restore cannot read back the articles holding it, so what
// they carried was never said at any depth. They come again, the set is
// found once and counts their blocks once, and it repairs as the run that
// never restarted does.
async fn assert_lost_at_restart_repairs(
    ember: &[u8],
    set: &[Post],
    lost: &Post,
    down: fn(&[(u32, u32)]) -> u32,
) {
    let mut entries: Vec<(&str, Vec<u8>)> = set
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.clone()))
        .collect();
    entries.push(("ember.bin", ember.to_vec()));
    let outer = rar(&[("level.rar", rar(&entries))]);
    let held = articles_holding(&outer, &lost.bytes);
    let at = down(&held);
    let damaged = articles_inside(&outer, ember)[1];
    let order: Vec<(u32, u32)> = (0..outer.len().div_ceil(ARTICLE) as u32)
        .map(|ordinal| (0, ordinal))
        .collect();
    let scenario = Scenario::new(vec![Post::plain("lantern.rar", outer)])
        .damaged(0, damaged)
        .first(&order);
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    assert!(whole.files[&delivered("ember.bin")] == ember);
    assert!(
        matches!(
            whole.graph().outcomes[..],
            [Some(RepairOutcome::Repaired { .. })]
        ),
        "{:?}",
        whole.graph().outcomes
    );
    assert!(damaged > at, "the damage comes after the restart");
    let restarted = run_restarting_at(&scenario, &mut |_| {}, Down::Losing(&lost.name), &|nth| {
        nth == at as usize + 1
    })
    .await;
    assert_eq!(restarted.restarts, 1);
    assert_eq!(restarted.status, whole.status, "status");
    assert_eq!(
        restarted.files.keys().collect::<Vec<_>>(),
        whole.files.keys().collect::<Vec<_>>()
    );
    for (name, bytes) in &whole.files {
        assert!(restarted.files[name] == *bytes, "{name}: delivered bytes");
    }
    let unread: std::collections::BTreeSet<(u32, u32)> = held
        .iter()
        .filter(|(_, ordinal)| *ordinal <= at)
        .filter(|article| !restarted.in_memory.contains(article))
        .copied()
        .collect();
    assert!(
        !unread.is_empty(),
        "the lost file is on disk at the restart"
    );
    assert_eq!(
        restarted.from_disk, unread,
        "the articles holding the lost file on disk, and no other, come again"
    );
    assert_eq!(restarted.graph.outcomes, whole.graph().outcomes);
    assert_eq!(
        restarted.graph.blocks_reachable,
        whole.graph().blocks_reachable
    );
}

// The last article holding the post.
fn its_last(held: &[(u32, u32)]) -> u32 {
    held.last().unwrap().1
}

// The pipeline goes down once the PAR2 volume is in.
#[tokio::test]
async fn a_par2_volume_two_deep_a_restart_cannot_read_back_comes_again_and_its_set_still_repairs() {
    let ember = payload(6 * ARTICLE + 5, 104);
    let set = par2_set("ember", &[("ember.bin", ember.as_slice())], 2);
    assert_lost_at_restart_repairs(&ember, &set, &set[1], its_last).await;
}

// The same with a PAR3 set: the pipeline goes down once its volume is in.
#[tokio::test]
async fn a_par3_volume_two_deep_a_restart_cannot_read_back_comes_again_and_its_set_still_repairs() {
    let ember = payload(6 * ARTICLE + 5, 106);
    let set = par3_set("ember", &[("ember.bin", ember.as_slice())], 2);
    assert!(set[1].name.contains(".vol"), "{}", set[1].name);
    assert_lost_at_restart_repairs(&ember, &set, &set[1], its_last).await;
}

// The PAR2 index, whose packets name the set, lost halfway through, before
// it is whole: the index comes again and the set is found once, from its
// packets as they come. Articles of it still in memory at the restart are
// not lost, so the pipeline goes down partway into it.
#[tokio::test]
async fn a_par2_index_two_deep_lost_before_its_packets_were_walked_comes_again_and_its_set_is_found_once()
 {
    let ember = payload(6 * ARTICLE + 5, 107);
    let set = par2_set("ember", &[("ember.bin", ember.as_slice())], 2);
    assert_lost_at_restart_repairs(&ember, &set, &set[0], |held| {
        assert!(held.len() > 2, "the index spans articles");
        held[held.len() / 2].1
    })
    .await;
}

// A stored RAR5 set of four volumes two deep, its second never posted,
// beside the `.rev` that rebuilds it: the set is rebuilt at depth and the
// member delivered whole. The `.rev` is delivered beside it, as recovery
// files below the root are.
#[tokio::test]
async fn a_rar_set_two_deep_missing_a_volume_is_rebuilt_from_its_rev() {
    let body = rev_body();
    let volumes = single_member_store_set("beacon.bin", &body, 4);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 1);
    let mut entries: Vec<(&str, Vec<u8>)> = volumes
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != 1)
        .map(|(_, (name, bytes))| (name.as_str(), bytes.clone()))
        .collect();
    entries.extend(
        revs.iter()
            .map(|post| (post.name.as_str(), post.bytes.clone())),
    );
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", two_deep(&entries))]);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        ["beacon.bin", "silver.horizon.part1.rev"].map(delivered)
    );
    assert!(graph.files[&delivered("beacon.bin")] == body, "beacon.bin");
    assert!(
        kinds(&graph).contains(&"rar_recovery"),
        "{:?}",
        kinds(&graph)
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A compressed, enciphered RAR, `cinder.rar`, two deep beside `beacon.bin`,
// `candle.bin` one deep, and no password: the store gives the RAR up to its
// engine, which asks, and nothing answers. The job ends as the locked RAR
// at the root does, partial on its password with the archive withheld and
// delivered as posted, and the members above it and beside it delivered.
#[tokio::test]
async fn a_compressed_rar_two_deep_whose_password_never_comes_ends_partial_with_the_members_above_it_delivered()
 {
    let locked = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles/rar5_solid_encrypted_small.rar"
    ))
    .unwrap();
    let outer = sevenz(&[
        ("candle.bin", candle()),
        (
            "level.rar",
            rar(&[("beacon.bin", beacon()), ("cinder.rar", locked.clone())]),
        ),
    ]);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]);
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Partial)),
        "{:?} {:?}",
        graph.status,
        graph.failure
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Password));
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        ["beacon.bin", "candle.bin", "cinder.rar"].map(delivered)
    );
    assert!(
        graph.files[&delivered("beacon.bin")] == beacon(),
        "beacon.bin"
    );
    assert!(
        graph.files[&delivered("candle.bin")] == candle(),
        "candle.bin"
    );
    assert!(
        graph.files[&delivered("cinder.rar")] == locked,
        "cinder.rar"
    );
    assert_eq!(
        graph.graph().delivery.withheld,
        vec![("cinder.rar".to_owned(), Why::EncryptedHeader)]
    );
    assert!(kinds(&graph).contains(&"rar_engine"), "{:?}", kinds(&graph));
}

// The member of the four-volume stored RAR5 set the next rows move.
fn spread() -> Vec<u8> {
    payload(11 * ARTICLE + 13, 109)
}

fn spread_set() -> Vec<(String, Vec<u8>)> {
    single_member_store_set("beacon.bin", &spread(), 4)
}

// `volumes` as posted, then the PAR2 set over the set's own volumes, which
// names each by its content.
fn named_by_par2(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    let set = spread_set();
    let files: Vec<(&str, &[u8])> = set
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut out = rar_posts(volumes);
    out.extend(par2_set("silver.horizon", &files, 2));
    out
}

// `posts` at the root, and the same posts as the entries of `level.rar` in
// `lantern.7z`, each shaped by `shape`: the namespace two deep binds,
// claims, moves and gives back as the root's does, so the member comes out
// whole at both, the repairs come out the same, and nothing is asked for
// twice. Recovery files below the root are delivered beside the member.
async fn assert_two_deep_as_at_the_root(posts: fn() -> Vec<Post>, shape: fn(Scenario) -> Scenario) {
    let root = run(&shape(Scenario::new(posts()))).await;
    assert!(
        root.completed(),
        "root: {:?} {:?}",
        root.status,
        root.failure
    );
    assert!(root.files[&delivered("beacon.bin")] == spread(), "root");
    let posted = posts();
    let entries: Vec<(&str, Vec<u8>)> = posted
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.clone()))
        .collect();
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", two_deep(&entries))]);
    let deep = run(&shape(scenario)).await;
    assert!(
        deep.completed(),
        "two deep: {:?} {:?}",
        deep.status,
        deep.failure
    );
    let mut names: Vec<String> = posted
        .iter()
        .filter(|post| post.name.ends_with(".par2"))
        .map(|post| delivered(&post.name))
        .chain([delivered("beacon.bin")])
        .collect();
    names.sort();
    assert_eq!(deep.files.keys().cloned().collect::<Vec<_>>(), names);
    assert!(deep.files[&delivered("beacon.bin")] == spread(), "two deep");
    assert_eq!(deep.graph().outcomes, root.graph().outcomes);
    assert!(root.refetched().is_empty(), "root: {:?}", root.refetched());
    assert!(deep.refetched().is_empty(), "{:?}", deep.refetched());
}

// The second and third volumes under each other's names beside the PAR2 set
// that names them, two deep: the set binds each by content and the namespace
// puts the names back, as at the root.
#[tokio::test]
async fn a_rar_set_two_deep_with_two_volumes_posted_swapped_beside_par2_delivers_as_at_the_root() {
    assert_two_deep_as_at_the_root(
        || {
            let set = spread_set();
            let mut swapped = set.clone();
            swapped.swap(1, 2);
            named_by_par2(
                set.into_iter()
                    .zip(swapped)
                    .map(|((name, _), (_, bytes))| (name, bytes))
                    .collect(),
            )
        },
        |scenario| scenario,
    )
    .await;
}

// Nameless volumes, the first of them last, beside the PAR2 set that names
// them, two deep: nothing says the first volume's place until the set does,
// and the namespace moves each to its name as at the root.
#[tokio::test]
async fn a_nameless_rar_set_two_deep_whose_first_volume_comes_last_beside_par2_delivers_as_at_the_root()
 {
    assert_two_deep_as_at_the_root(
        || {
            let mut volumes = obfuscate_volumes(&spread_set());
            volumes.rotate_left(1);
            named_by_par2(volumes)
        },
        |scenario| scenario,
    )
    .await;
}

// The set two deep, every batch the queue hands out arriving last article
// first: claims made before the headers that bind them come are given back
// and moved as at the root.
#[tokio::test]
async fn a_rar_set_two_deep_served_last_article_first_delivers_as_at_the_root() {
    assert_two_deep_as_at_the_root(|| rar_posts(spread_set()), Scenario::backwards).await;
}

// A compressed AES 7z, `cinder.7z`, beside `beacon.bin`, and `candle.bin`
// beside them, with no password: the 7z's store asks, nothing answers, and
// it withholds the archive, its folders shut. Two deep, inside `level.rar`
// beside `beacon.bin` with `candle.bin` one deep, the job ends as it does at
// the root: partial on the password, the archive withheld and delivered as
// posted, and the members above it and beside it delivered.
#[tokio::test]
async fn a_compressed_aes_7z_two_deep_whose_password_never_comes_ends_as_at_the_root_with_the_members_above_it_delivered()
 {
    let locked = container(Lzma2Options::from_level(1).into(), Some(SECRET));
    let root = run(&Scenario::new(vec![
        Post::plain("beacon.bin", beacon()),
        Post::plain("candle.bin", candle()),
        Post::plain("cinder.7z", locked.clone()),
    ]))
    .await;
    let outer = sevenz(&[
        ("candle.bin", candle()),
        (
            "level.rar",
            rar(&[("beacon.bin", beacon()), ("cinder.7z", locked.clone())]),
        ),
    ]);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]);
    let graph = run(&scenario).await;
    for outcome in [&root, &graph] {
        assert!(
            matches!(outcome.status, Some(JobStatus::Partial)),
            "{:?} {:?}",
            outcome.status,
            outcome.failure
        );
        assert_eq!(outcome.failure_kind, Some(HistoryFailureKind::Password));
        assert_eq!(
            outcome.graph().delivery.withheld,
            vec![("cinder.7z".to_owned(), Why::EncryptedHeader)]
        );
        assert!(
            kinds(outcome).contains(&"sevenz_store"),
            "{:?}",
            kinds(outcome)
        );
        assert!(outcome.refetched().is_empty(), "{:?}", outcome.refetched());
    }
    assert!(graph.files == root.files, "the files the root delivers");
    assert_eq!(kinds(&graph), ["rar_store", "sevenz_store"]);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        ["beacon.bin", "candle.bin", "cinder.7z"].map(delivered)
    );
    assert!(
        graph.files[&delivered("beacon.bin")] == beacon(),
        "beacon.bin"
    );
    assert!(
        graph.files[&delivered("candle.bin")] == candle(),
        "candle.bin"
    );
    assert!(graph.files[&delivered("cinder.7z")] == locked, "cinder.7z");
}

// `ember.bin` beside the PAR2 set over it in `level.rar` in `lantern.7z`,
// one article of it damaged.
fn repaired_two_deep() -> (Vec<u8>, Scenario) {
    let ember = payload(6 * ARTICLE + 5, 108);
    let set = par2_set("ember", &[("ember.bin", ember.as_slice())], 2);
    let mut entries: Vec<(&str, Vec<u8>)> = vec![("ember.bin", ember.clone())];
    entries.extend(
        set.iter()
            .map(|post| (post.name.as_str(), post.bytes.clone())),
    );
    let outer = two_deep(&entries);
    let damaged = articles_inside(&outer, &ember)[1];
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]).damaged(0, damaged);
    (ember, scenario)
}

// The pipeline crashes once the last article of the container is in, and
// the checkpoint it goes down on holds `ember.bin` damaged and nothing of it
// repaired. The restored job repairs it as the run that never went down
// does and delivers the member whole; it asks again only for what it held
// in memory.
#[tokio::test]
async fn a_par2_set_two_deep_crashed_before_its_repair_landed_repairs_and_delivers_the_member_whole()
 {
    let (ember, scenario) = repaired_two_deep();
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    assert!(whole.files[&delivered("ember.bin")] == ember);
    assert!(
        matches!(
            whole.graph().outcomes[..],
            [Some(RepairOutcome::Repaired { .. })]
        ),
        "{:?}",
        whole.graph().outcomes
    );
    let last = scenario.posts[0].bytes.len().div_ceil(ARTICLE);
    let mut before_the_repair = false;
    let mut at_the_crash = |rows: &crate::jobs::graph_checkpoint::GraphCheckpoint| {
        let ember = rows
            .members
            .iter()
            .find(|row| row.name == "ember.bin")
            .expect("the checkpoint holds ember.bin");
        let said: Vec<&str> = rows
            .coverage
            .iter()
            .filter(|row| row.node == ember.node && row.member == ember.member)
            .map(|row| row.provenance.as_str())
            .collect();
        before_the_repair = said.contains(&"damaged") && !said.contains(&"repaired");
    };
    let restarted = run_restarting_at(&scenario, &mut at_the_crash, Down::Crash, &|nth| {
        nth == last
    })
    .await;
    assert!(before_the_repair, "the crash came before the repair landed");
    assert_eq!(restarted.restarts, 1);
    assert_restarted(&restarted, &whole);
    assert_eq!(restarted.graph.outcomes, whole.graph().outcomes);
    assert_eq!(
        restarted.graph.blocks_reachable,
        whole.graph().blocks_reachable
    );
}

// A byte-identical archive posted again under another name is that
// archive, and one member delivers. Two deep, `cinder` and `ashen` side by
// side in `level.rar` in `lantern.7z`, once as stored 7zs and once as stored
// RAR5 volumes: each job is Complete with the members once, the second
// archive's bytes no file of their own, and nothing asked for twice.
#[tokio::test]
async fn the_repost_rule_holds_two_deep_for_a_7z_and_a_rar5_posted_again_under_another_name() {
    let members = sevenz_members();
    let beacon = vec![("beacon.bin", beacon())];
    for (archive, ext, held) in [
        (sevenz(&members), "7z", members.clone()),
        (rar(&beacon), "rar", beacon.clone()),
    ] {
        let (first, again) = (format!("cinder.{ext}"), format!("ashen.{ext}"));
        let outer = two_deep(&[(&first, archive.clone()), (&again, archive)]);
        let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]);
        let graph = run(&scenario).await;
        assert!(
            graph.completed(),
            "{ext}: {:?} {:?}",
            graph.status,
            graph.failure
        );
        assert_eq!(
            graph.files.keys().cloned().collect::<Vec<_>>(),
            held.iter()
                .map(|(name, _)| delivered(name))
                .collect::<Vec<_>>(),
            "{ext}: the members once, and neither archive"
        );
        for (name, bytes) in &held {
            assert!(graph.files[&delivered(name)] == *bytes, "{ext}: {name}");
        }
        assert!(
            graph.refetched().is_empty(),
            "{ext}: {:?}",
            graph.refetched()
        );
    }
}

// The pipeline drained and taken down while the repair of `ember.bin` two
// deep is out on its thread: the drain hears it back, so the restored job
// holds the repair, replays the damaged article of `lantern.7z` as the
// damage the repair already answered, lends no second repair, and announces
// the one once.
#[tokio::test]
async fn a_par2_set_two_deep_restarted_while_its_repair_is_out_applies_the_repair_once() {
    let (ember, scenario) = repaired_two_deep();
    let whole = run(&scenario).await;
    let [Some(RepairOutcome::Repaired { slices })] = whole.graph().outcomes[..] else {
        panic!("{:?}", whole.graph().outcomes);
    };
    let (restarted, lent_again) = run_restarted_while_repairing(&scenario).await;
    assert!(!lent_again, "the restored job lent the repair again");
    assert_eq!(restarted.status, whole.status);
    assert!(
        restarted.files == whole.files,
        "the files the whole run delivered"
    );
    assert!(restarted.files[&delivered("ember.bin")] == ember);
    assert_eq!(
        repairs_and_end(&restarted.events),
        [
            format!("RepairComplete {slices}"),
            "JobCompleted".to_owned()
        ]
    );
}

// The scenario of `assert_repaired_header_places_the_bytes_behind_it`, and
// `inner` run alone: what it delivers.
type WrapMembers = fn(Vec<(&str, Vec<u8>)>) -> (String, Vec<u8>);

async fn repaired_header(inner: Vec<u8>, first: &str, wrap: WrapMembers) -> (Outcome, Scenario) {
    let alone = run(&Scenario::new(vec![Post::plain(
        "mixed.rar",
        inner.clone(),
    )]))
    .await;
    assert!(alone.completed(), "alone: {:?}", alone.failure);
    let first = alone.files[&delivered(first)].clone();
    let set = par2_set("mixed", &[("mixed.rar", inner.as_slice())], 2);
    let mut entries: Vec<(&str, Vec<u8>)> = set
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.clone()))
        .collect();
    entries.push(("mixed.rar", inner));
    let (name, outer) = wrap(entries);
    const FRAME: usize = 4;
    let at = outer
        .windows(first.len())
        .position(|window| window == first.as_slice())
        .expect("a stored container holds its member's bytes as they are");
    let damaged = at.div_ceil(FRAME) as u32;
    assert!(
        (damaged as usize + 1) * FRAME <= at + first.len(),
        "the damaged article lies inside the first member"
    );
    // The next member's RAR5 header follows the first member's data: a
    // checksum, the size of the rest as a vint, then the rest.
    let header = at + first.len();
    let (size, width) = vint(&outer[header + 4..]);
    let header_end = header + 4 + width + size as usize;
    let in_header = ((header / FRAME + 1) as u32, header_end);
    assert!(
        (in_header.0 as usize + 1) * FRAME <= in_header.1,
        "the second damaged article lies in the next member's header"
    );
    let scenario = Scenario::new(vec![Post::plain(&name, outer)])
        .framed(FRAME)
        .damaged(0, damaged)
        .damaged(0, in_header.0);
    (alone, scenario)
}

// A RAR5 vint at the front of `bytes`: its value and its width.
fn vint(bytes: &[u8]) -> (u64, usize) {
    let mut value = 0u64;
    for (index, byte) in bytes.iter().enumerate() {
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            return (value, index + 1);
        }
    }
    panic!("a vint runs to its end");
}

// `inner`, a RAR posted as `mixed.rar`, beside the PAR2 set over it, as
// `wrap` nests both, in articles of 4 bytes: one inside the stored bytes of
// its first member, `first`, damaged, and one in the header of the member
// after it. Until the set repairs the header, the store cannot say whose the
// bytes behind it are and keeps them as its own; the repaired header names
// the member they are. They go to that member as any bytes do once placed,
// so every member `inner` holds is delivered byte-exact and whole, with
// nothing fetched again.
async fn assert_repaired_header_places_the_bytes_behind_it(
    inner: Vec<u8>,
    first: &str,
    wrap: WrapMembers,
) -> Outcome {
    let (alone, scenario) = repaired_header(inner, first, wrap).await;
    let graph = run(&scenario).await;
    assert!(
        graph.completed(),
        "{:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().delivery
    );
    assert!(
        matches!(
            graph.graph().outcomes[..],
            [Some(RepairOutcome::Repaired { .. })]
        ),
        "{:?}",
        graph.graph().outcomes
    );
    for (file, bytes) in &alone.files {
        assert!(graph.files.get(file) == Some(bytes), "{file}");
    }
    let delivery = &graph.graph().delivery;
    assert!(delivery.damaged.is_empty(), "{delivery:?}");
    assert!(
        delivery
            .files
            .iter()
            .filter(|file| !file.name.ends_with(".par2"))
            .all(|file| file.whole),
        "{delivery:?}"
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    graph
}

// The compressed fixture: the member the repaired header names is
// compressed, so the store gives `mixed.rar` up to the RAR engine, which
// reads the bytes back where they went.
async fn assert_repaired_bytes_read_back_after_refusal(wrap: WrapMembers) {
    let mixed = crate::pipeline::tests::rar5_fixture_bytes("rar5_multifile_lz.rar");
    let graph = assert_repaired_header_places_the_bytes_behind_it(mixed, "hello.txt", wrap).await;
    assert!(kinds(&graph).contains(&"rar_engine"), "{:?}", kinds(&graph));
}

// A stored RAR of two members: the member the repaired header names is
// stored, and the store delivers it itself.
async fn assert_repaired_header_delivers_the_stored_member_behind_it(wrap: WrapMembers) {
    let inner = rar(&[
        ("cinder.txt", payload(40, 3)),
        ("ember.bin", payload(60, 5)),
    ]);
    let graph = assert_repaired_header_places_the_bytes_behind_it(inner, "cinder.txt", wrap).await;
    assert!(
        !kinds(&graph).contains(&"rar_engine"),
        "{:?}",
        kinds(&graph)
    );
}

#[tokio::test]
async fn a_member_repaired_while_its_store_holds_it_is_read_back_repaired_when_the_store_gives_it_up_one_deep()
 {
    assert_repaired_bytes_read_back_after_refusal(|entries| ("lantern.rar".into(), rar(&entries)))
        .await;
}

#[tokio::test]
async fn a_member_repaired_while_its_store_holds_it_is_read_back_repaired_when_the_store_gives_it_up_two_deep()
 {
    assert_repaired_bytes_read_back_after_refusal(|entries| {
        ("lantern.7z".into(), two_deep(&entries))
    })
    .await;
}

#[tokio::test]
async fn a_stored_member_behind_a_repaired_header_is_delivered_whole_one_deep() {
    assert_repaired_header_delivers_the_stored_member_behind_it(|entries| {
        ("lantern.rar".into(), rar(&entries))
    })
    .await;
}

#[tokio::test]
async fn a_stored_member_behind_a_repaired_header_is_delivered_whole_two_deep() {
    assert_repaired_header_delivers_the_stored_member_behind_it(|entries| {
        ("lantern.7z".into(), two_deep(&entries))
    })
    .await;
}

// The stored pair one deep, the pipeline crashed before the repair landed:
// the bytes the store kept as its own behind the damaged header are still
// its own after the restore, and the repaired header still sends them to
// the member it names.
#[tokio::test]
async fn a_stored_member_behind_a_header_repaired_after_a_crash_is_delivered_whole() {
    let inner = rar(&[
        ("cinder.txt", payload(40, 3)),
        ("ember.bin", payload(60, 5)),
    ]);
    let (_, scenario) = repaired_header(inner, "cinder.txt", |entries| {
        ("lantern.rar".into(), rar(&entries))
    })
    .await;
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let last = scenario.posts[0].bytes.len().div_ceil(4);
    let mut before_the_repair = false;
    let mut at_the_crash = |rows: &crate::jobs::graph_checkpoint::GraphCheckpoint| {
        let mixed = rows
            .members
            .iter()
            .find(|row| row.name == "mixed.rar")
            .expect("the checkpoint holds mixed.rar");
        let said: Vec<&str> = rows
            .coverage
            .iter()
            .filter(|row| row.node == mixed.node && row.member == mixed.member)
            .map(|row| row.provenance.as_str())
            .collect();
        before_the_repair = said.contains(&"damaged") && !said.contains(&"repaired");
    };
    let restarted = run_restarting_at(&scenario, &mut at_the_crash, Down::Crash, &|nth| {
        nth == last
    })
    .await;
    assert!(before_the_repair, "the crash came before the repair landed");
    assert_eq!(restarted.restarts, 1);
    assert_restarted(&restarted, &whole);
    assert_eq!(restarted.graph.outcomes, whole.graph().outcomes);
}

// An enciphered stored 7z one deep, its password only a candidate: the store
// keeps the proof file's bytes as its own while the folder is locked, and
// the password that opens it places them in their member with nothing more
// delivered of the volume. Every member is delivered byte-exact.
#[tokio::test]
async fn a_locked_stored_7z_one_deep_opened_by_a_candidate_delivers_its_proof_member_whole() {
    let locked = container(EncoderConfiguration::new(EncoderMethod::COPY), Some(SECRET));
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.rar",
        rar(&[("cinder.7z", locked)]),
    )])
    .global_password(SECRET);
    let graph = run(&scenario).await;
    assert!(
        graph.completed(),
        "{:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().delivery
    );
    assert_eq!(kinds(&graph), ["rar_store", "sevenz_store"]);
    for (name, bytes) in sevenz_members() {
        assert!(graph.files.get(&delivered(name)) == Some(&bytes), "{name}");
    }
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A stored RAR holding the PAR2 set over `ember.bin` ahead of the member:
// the member's bytes reach the set as they land, with the checksums the
// articles brought, cut where the set's slices fall. The set finds every
// slice proven and reads nothing back.
#[tokio::test]
async fn a_set_inside_a_stored_rar_proves_the_member_beside_it_as_it_lands() {
    let ember = payload(5 * ARTICLE + 11, 93);
    let mut entries: Vec<(String, Vec<u8>)> =
        par2_set("inner", &[("ember.bin", ember.as_slice())], 2)
            .into_iter()
            .map(|post| (post.name, post.bytes))
            .collect();
    entries.push(("ember.bin".to_string(), ember.clone()));
    let named: Vec<(&str, Vec<u8>)> = entries
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let scenario = Scenario::new(vec![Post::plain("lantern.rar", rar(&named))]);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert!(graph.files.get(&delivered("ember.bin")) == Some(&ember));
    assert_eq!(graph.graph().outcomes, vec![Some(RepairOutcome::Clean)]);
    assert_eq!(graph.reads, 0, "a slice was read back");
}
