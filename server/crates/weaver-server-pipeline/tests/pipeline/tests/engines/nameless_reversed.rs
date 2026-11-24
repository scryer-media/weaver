// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR set posted under names that number nothing, its last volume posted
// first or in any other order, with no recovery set and beside a PAR2 set
// that names its volumes: every volume arrives whole, so the job completes
// with the member as written.
//
// Each case fails without the two rules it reads. A stored three-volume set
// whose headers state no number: its last volume, offered first to the
// store its first volume founds, is held after the first by its links; the
// middle volume then fits only there, and the last makes way for it
// (`claim::makes_way`). The compressed four-volume set, whose end records
// state each volume's number: the store hands its last volume, held after
// the first by its links, to the engine before its end record is read, and
// the engine's holder makes way for the volume whose front states that place
// (`EngineObject::provisional`). Before it, the run read the first and last
// volumes as the first two, found the member's checksum wrong and said both
// damaged; beside PAR2, the renames moved the volumes, the run that read
// again found the second place empty, and the set asked for a fifth volume.

use std::collections::{BTreeMap, BTreeSet};

use sha2::Digest;

use super::harness::{
    ARTICLE, Outcome, Post, Scenario, par2_set, payload, run, run_holding_inspecting_reads,
};
use super::rar_rev::lz_four;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::single_member_rar4_old_numbering_set;

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// A set: its label, its volumes as RAR named them, its one member, and the
// SHA-256 of that member as written.
struct Set {
    label: &'static str,
    volumes: Vec<(String, Vec<u8>)>,
    member: &'static str,
    sha256: String,
}

// A stored three-volume RAR4 set of one member whose headers state no
// volume number, and the compressed four-volume set.
fn sets() -> Vec<Set> {
    let body = payload(9 * ARTICLE, 83);
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/extraction_profiles/expected.json"
    )))
    .unwrap();
    vec![
        Set {
            label: "stored",
            volumes: single_member_rar4_old_numbering_set("silver.horizon", "beacon.bin", &body, 3),
            member: "beacon.bin",
            sha256: hex::encode(sha2::Sha256::digest(&body)),
        },
        Set {
            label: "lz_four",
            volumes: lz_four(),
            member: "compressible.txt",
            sha256: oracle["archives"]["rar4_lz_four"]["compressible.txt"]["sha256"]
                .as_str()
                .unwrap()
                .to_owned(),
        },
    ]
}

// The set's volumes posted in `order`, each under a name that numbers
// nothing, beside a PAR2 set over the volumes as named when `par2`; each
// batch the queue hands out served last article first when `backwards`.
fn posted(volumes: &[(String, Vec<u8>)], order: &[usize], par2: bool, backwards: bool) -> Scenario {
    let mut posts: Vec<Post> = order
        .iter()
        .map(|volume| {
            Post::plain(
                &format!("{:032x}", 0x0b5c_0000 + volume),
                volumes[*volume].1.clone(),
            )
        })
        .collect();
    if par2 {
        let files: Vec<(&str, &[u8])> = volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        posts.extend(par2_set("silver.horizon", &files, 4));
    }
    let mut scenario = Scenario::new(posts);
    scenario.article = 4096;
    match backwards {
        true => scenario.backwards(),
        false => scenario,
    }
}

// The case, when the job did not complete with the member as written.
fn failed(case: String, set: &Set, outcome: &Outcome) -> Option<String> {
    let whole = outcome
        .files
        .get(&delivered(set.member))
        .is_some_and(|bytes| hex::encode(sha2::Sha256::digest(bytes)) == set.sha256);
    (outcome.status != Some(JobStatus::Complete) || !whole).then(|| {
        format!(
            "{case}: {:?} {:?} {:?}",
            outcome.status,
            outcome.files.keys().collect::<Vec<_>>(),
            outcome.graph.claims
        )
    })
}

#[tokio::test]
async fn a_nameless_rar_set_posted_last_volume_first_completes() {
    let mut failures = Vec::new();
    for set in sets() {
        let order: Vec<usize> = (0..set.volumes.len()).rev().collect();
        for par2 in [false, true] {
            for backwards in [false, true] {
                let case = format!("{} par2 {par2} backwards {backwards}", set.label);
                let outcome = run(&posted(&set.volumes, &order, par2, backwards)).await;
                failures.extend(failed(case, &set, &outcome));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// Every order of `0..n`, each once.
fn orders(n: usize) -> Vec<Vec<usize>> {
    if n == 0 {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    for order in orders(n - 1) {
        for at in 0..=order.len() {
            let mut next = order.clone();
            next.insert(at, n - 1);
            out.push(next);
        }
    }
    out
}

// Both sets with no recovery set, posted in every order of their volumes:
// the stored set's 6 and the compressed set's 24, each served forward and
// backwards.
//
// The two middle volumes each only carry the member on, so their links
// place them in the order they come, and the store hands them to an engine
// with fronts that stop short of their end records: [0, 2, 1, 3] is held as
// it came. The engine reads each one's tail, whose end record states its
// number, and a volume its tail puts elsewhere goes back to be claimed
// there, before its run reads the member's checksum.
#[tokio::test]
async fn a_nameless_rar_set_posted_in_any_order_completes() {
    let mut failures = Vec::new();
    for set in sets() {
        for order in orders(set.volumes.len()) {
            for backwards in [false, true] {
                let case = format!("{} order {order:?} backwards {backwards}", set.label);
                let outcome = run(&posted(&set.volumes, &order, false, backwards)).await;
                failures.extend(failed(case, &set, &outcome));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// The last article of each post: where a volume's end record lies.
fn tail_article(scenario: &Scenario, file: u32) -> (u32, u32) {
    let post = &scenario.posts[file as usize];
    let articles = post.articles(scenario.article).count() as u32;
    (file, articles - 1)
}

// What each run of the job said, by node and generation.
type Spoken = std::sync::Arc<std::sync::Mutex<Vec<(weaver_graph::NodeId, u64, String)>>>;

fn hearing(scenario: Scenario) -> (Scenario, Spoken) {
    let spoken = Spoken::default();
    let kept = std::sync::Arc::clone(&spoken);
    let scenario = scenario.on_run_message(move |node, generation, message| {
        kept.lock().unwrap().push((node, generation, message));
    });
    (scenario, spoken)
}

// The generations each node's runs spoke under: what a run emits, and its
// end. A verification or a checkpoint written speaks for no run.
fn generations(spoken: &Spoken) -> BTreeMap<weaver_graph::NodeId, BTreeSet<u64>> {
    let mut out: BTreeMap<_, BTreeSet<u64>> = BTreeMap::new();
    for (node, generation, heard) in spoken.lock().unwrap().iter() {
        if heard.starts_with("Emit(") || heard == "Ended" {
            out.entry(*node).or_default().insert(*generation);
        }
    }
    out
}

// The compressed four-volume set posted [0, 2, 1, 3] under names that
// number nothing, the server holding the last article of both middle posts,
// where their end records lie. The links hold the third volume second and
// the second third, and neither place is stated while their tails are held:
// the run reads the first volume and waits, reading no byte of either. Once
// the tails come, the second volume is given back and claimed second, the
// third takes the third place, and the one run reads the set through.
#[tokio::test]
async fn a_run_waits_for_the_place_of_a_volume_its_tail_holds() {
    let set = sets().remove(1);
    let scenario = posted(&set.volumes, &[0, 2, 1, 3], false, false);
    let held = [tail_article(&scenario, 1), tail_article(&scenario, 2)];
    let (scenario, spoken) = hearing(scenario);
    let mut read = Vec::new();
    let outcome = run_holding_inspecting_reads(
        &scenario,
        |article| held.contains(&article),
        |positions| read = positions,
    )
    .await;
    assert!(
        !read.is_empty(),
        "no run was reading while the tails were held"
    );
    for (node, reached) in &read {
        let past_first: Vec<_> = reached
            .iter()
            .filter(|(volume, reached)| **volume > 0 && **reached > 0)
            .collect();
        assert!(
            past_first.is_empty(),
            "node {node:?} read {past_first:?} before their places were stated; said {:#?}",
            spoken.lock().unwrap()
        );
    }
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.files.keys()
    );
    let whole = outcome
        .files
        .get(&delivered(set.member))
        .is_some_and(|bytes| hex::encode(sha2::Sha256::digest(bytes)) == set.sha256);
    assert!(whole, "{:?}", outcome.files.keys());
    let runs = generations(&spoken);
    assert!(
        runs.values().all(|generations| generations.len() == 1),
        "a run started again: {runs:?}; said {:#?}",
        spoken.lock().unwrap()
    );
}

// The set posted in its own order under names that number nothing, the
// server holding the last article of its second post, where that volume's
// end record lies: the run reads the first volume and waits, reading no
// byte of the second. Once the tail comes, the one run reads the set
// through, and the job completes with the member as written.
async fn waits_for_its_second_tail(set: &Set, scenario: Scenario) {
    let held = tail_article(&scenario, 1);
    let (scenario, spoken) = hearing(scenario);
    let mut read = Vec::new();
    let outcome = run_holding_inspecting_reads(
        &scenario,
        |article| article == held,
        |positions| read.extend(positions.into_iter().map(|(_, reached)| reached)),
    )
    .await;
    assert!(!read.is_empty(), "{}: no run was reading", set.label);
    for reached in &read {
        assert!(
            reached.range(1..).all(|(_, reached)| *reached == 0),
            "{}: read {reached:?} before the second place was stated",
            set.label
        );
    }
    let whole = outcome
        .files
        .get(&delivered(set.member))
        .is_some_and(|bytes| hex::encode(sha2::Sha256::digest(bytes)) == set.sha256);
    assert!(
        outcome.status == Some(JobStatus::Complete) && whole,
        "{}: {:?} {:?}",
        set.label,
        outcome.status,
        outcome.files.keys()
    );
    let runs = generations(&spoken);
    assert!(
        runs.values().all(|generations| generations.len() == 1),
        "{}: a run started again: {runs:?}",
        set.label
    );
}

// The compressed set with no number in any end record: each record loses
// the field that numbers its volume and the flag that says it is there,
// and its header checksum is made again over what is left. A RAR4 end
// record of 20 bytes is its 7-byte header, the data checksum, the number,
// and 7 reserved bytes.
fn unnumbered(set: Set) -> Set {
    let volumes = set
        .volumes
        .into_iter()
        .map(|(name, mut bytes)| {
            let at = bytes.len() - 20;
            let flags = u16::from_le_bytes([bytes[at + 3], bytes[at + 4]]);
            assert_eq!(
                (bytes[at + 2], flags & 0x000e, &bytes[at + 5..at + 7]),
                (0x7b, 0x000e, &[0x14, 0x00][..]),
                "{name}"
            );
            bytes.drain(at + 11..at + 13);
            bytes[at + 3..at + 5].copy_from_slice(&(flags & !0x0008).to_le_bytes());
            bytes[at + 5..at + 7].copy_from_slice(&18_u16.to_le_bytes());
            let crc = crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, &bytes[at + 2..]);
            bytes[at..at + 2].copy_from_slice(&(crc as u16).to_le_bytes());
            (name, bytes)
        })
        .collect();
    Set {
        label: "unnumbered",
        volumes,
        ..set
    }
}

// Release by the tail read: the compressed set's end record states the
// number its links hold the second volume at, or, with the numbers taken
// out, states none; either way the run reads on where it waited.
#[tokio::test]
async fn a_run_reads_on_once_the_tail_it_waits_on_is_read() {
    for set in [sets().remove(1), unnumbered(sets().remove(1))] {
        let scenario = posted(&set.volumes, &[0, 1, 2, 3], false, false);
        waits_for_its_second_tail(&set, scenario).await;
    }
}

// The compressed two-volume set, whose second volume is longer than the
// 16 KiB PAR2 names a file by.
fn lz_two() -> Set {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/extraction_profiles"
    );
    let four = sets().remove(1);
    Set {
        label: "lz_two",
        volumes: (1..=2)
            .map(|part| {
                let bytes = std::fs::read(format!("{dir}/rar4_lz_two.part0{part}.rar")).unwrap();
                (format!("silver.horizon.part{part}.rar"), bytes)
            })
            .collect(),
        ..four
    }
}

// The compressed two-volume set posted under names that number nothing,
// beside a PAR2 set that names its volumes, the server holding the last
// article of the second post, where that volume's end record lies, and
// serving it as `scenario` says once the hold ends. PAR2 names the volume
// by its first 16 KiB, which come before the tail, and the name states its
// place, so the run reads into it while its tail is still held. The job
// completes with the member as written. No real post is named: it stands
// for an obfuscated two-volume RAR4 release with a PAR2 set beside it.
async fn reads_on_into_the_named_volume(
    set: &Set,
    scenario: impl FnOnce(Scenario, (u32, u32)) -> Scenario,
) {
    assert!(set.volumes[1].1.len() > 16 * 1024);
    let posted = posted(&set.volumes, &[0, 1], true, false);
    let held = tail_article(&posted, 1);
    let scenario = scenario(posted, held);
    let mut read = Vec::new();
    let outcome = run_holding_inspecting_reads(
        &scenario,
        |article| article == held,
        |positions| read = positions,
    )
    .await;
    assert!(
        read.iter()
            .any(|(_, reached)| reached.get(&1).is_some_and(|reached| *reached > 0)),
        "no run read into the named volume while its tail was held: {read:?}"
    );
    let whole = outcome
        .files
        .get(&delivered(set.member))
        .is_some_and(|bytes| hex::encode(sha2::Sha256::digest(bytes)) == set.sha256);
    assert!(
        outcome.status == Some(JobStatus::Complete) && whole,
        "{:?} {:?}",
        outcome.status,
        outcome.files.keys()
    );
}

// Release by a recovery rename: the tail the run waited on is held, and the
// run reads on once PAR2 names the volume.
#[tokio::test]
async fn a_run_reads_on_once_a_recovery_names_the_volume_it_waits_on() {
    reads_on_into_the_named_volume(&lz_two(), |scenario, _| scenario).await;
}

// The same, with the held tail served damaged once the hold ends: a damaged
// tail states nothing, and the named volume no longer waits on it, so the
// run is fed while the tail is held and PAR2 repairs the damaged article.
#[tokio::test]
async fn a_run_a_recovery_named_is_fed_though_its_tail_comes_damaged() {
    reads_on_into_the_named_volume(&lz_two(), |scenario, (file, ordinal)| {
        scenario.damaged(file, ordinal)
    })
    .await;
}

// Each set posted in its own order completes in one run per node: a wait
// for a tail that confirms the place costs no run started again.
#[tokio::test]
async fn an_in_order_nameless_rar_set_completes_in_one_run() {
    for set in sets() {
        let order: Vec<usize> = (0..set.volumes.len()).collect();
        for backwards in [false, true] {
            let (scenario, spoken) = hearing(posted(&set.volumes, &order, false, backwards));
            let outcome = run(&scenario).await;
            let case = format!("{} backwards {backwards}", set.label);
            assert_eq!(failed(case, &set, &outcome), None);
            let runs = generations(&spoken);
            assert!(
                runs.values().all(|generations| generations.len() == 1),
                "{} backwards {backwards}: a run started again: {runs:?}",
                set.label
            );
        }
    }
}

// Release by a tail nothing will bring: the compressed set's second volume
// loses its last article, or it comes damaged and nothing repairs it, so
// the volume closes with its tail unread. The run reads on, ends on what
// the volume lacks, and the job fails, as one with no tail to wait on does.
#[tokio::test]
async fn a_run_reads_on_past_a_tail_that_never_comes() {
    let set = sets().remove(1);
    let scenario = || posted(&set.volumes, &[0, 1, 2, 3], false, false);
    let tail = tail_article(&scenario(), 1);
    for (case, scenario) in [
        ("lost", scenario().lost(tail.0, tail.1)),
        ("damaged", scenario().damaged(tail.0, tail.1)),
    ] {
        let (scenario, spoken) = hearing(scenario);
        let outcome = run(&scenario).await;
        assert!(
            matches!(outcome.status, Some(JobStatus::Failed { .. })),
            "{case}: {:?}",
            outcome.status
        );
        assert!(
            spoken
                .lock()
                .unwrap()
                .iter()
                .any(|(_, _, heard)| heard == "Ended"),
            "{case}: the run never ended"
        );
    }
}

// Release by a tail that can be fetched only in order: every article is
// uuencoded, so no tail is brought ahead. The run waits for each one as
// the normal order brings it, and the job completes in one run.
#[tokio::test]
async fn a_run_reads_on_where_no_tail_can_be_brought_ahead() {
    let set = sets().remove(1);
    let (scenario, spoken) = hearing(posted(&set.volumes, &[0, 1, 2, 3], false, false).uuencoded());
    let outcome = run(&scenario).await;
    assert_eq!(failed("uuencoded".to_owned(), &set, &outcome), None);
    let runs = generations(&spoken);
    assert!(
        runs.values().all(|generations| generations.len() == 1),
        "a run started again: {runs:?}"
    );
}
