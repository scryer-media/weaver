// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A six-volume RAR set posted as a release's `part01` to `part06`, two of
// the posted names swapped against the true order so the true last volume
// carries a middle number, its articles' yEnc headers naming each volume
// under an obfuscated stem with a one-digit `partN`, and PAR2 recovery
// volumes over the set with no index. Every volume arrives whole, so the
// set is one store whatever the names say and whatever order the posts
// arrive in, and every member is delivered.

use super::harness::{Outcome, Post, Scenario, kinds, par2_volumes_sliced, payload, run};
use super::rar::delivered;
use crate::pipeline::graph::lines::Lines;
use crate::pipeline::tests::direct_store::{
    multi_member_store_set, single_member_rar4_store_set, single_member_rar4_store_set_numbered,
};
use weaver_graph::holds::HoldBudgets;

const FRAME: usize = 32 * 1024;
const VOLUMES: usize = 6;
const RELEASE: &str = "Meshuggah-ObZen-24BIT-WEB-FLAC-2008-OBZEN";
const HEADER_STEM: &str = "5Kw2CyRs84fwY5gX9e2ZrWyDxKT0LIJG";

// The posted number of each volume in true order: the fifth volume is
// posted as `part06` and the last, short one as `part05`.
const POSTED_NUMBER: [usize; VOLUMES] = [1, 2, 3, 4, 6, 5];

// Five tracks and the release's small files, laid end to end so a track
// runs across every boundary the set split at.
fn members() -> Vec<(String, Vec<u8>)> {
    let chunk = 4 * FRAME + 711;
    let sizes = [
        ("01-combustion.flac", chunk + chunk / 3),
        ("02-electric_red.flac", 2 * chunk - chunk / 7),
        ("03-bleed.flac", chunk + 97),
        ("04-lethargica.flac", chunk + chunk / 5),
        ("05-obzen.flac", chunk / 2),
        ("00-cover.jpg", 3 * 1024 + 7),
        ("00-obzen.m3u", 211),
        ("00-obzen.nfo", 1501),
        ("00-obzen.sfv", 307),
    ];
    sizes
        .iter()
        .enumerate()
        .map(|(seed, (name, len))| (name.to_string(), payload(*len, 70 + seed as u8)))
        .collect()
}

// What the yEnc headers name each volume.
#[derive(Clone, Copy, Debug)]
enum Headers {
    // No header name: the posted names stand.
    None,
    // The obfuscated stem with each volume's true one-digit number.
    True,
    // The obfuscated stem with the posted number, one digit.
    Posted,
}

// Whom the PAR2 set names.
#[derive(Clone, Copy, Debug)]
enum Recovery {
    None,
    // The volumes under the header stem's true names, with no index.
    VolumesOnly,
    // The same set with its index, first.
    WithIndex,
}

#[derive(Clone, Copy, Debug)]
enum Order {
    Posted,
    Par2First,
    Backwards,
    // Every post side by side, the volumes' fronts first.
    SideBySide,
    // Side by side, the swapped volumes' fronts first.
    SwappedFirst,
}

const ORDERS: [Order; 5] = [
    Order::Posted,
    Order::Par2First,
    Order::Backwards,
    Order::SideBySide,
    Order::SwappedFirst,
];

fn header_name(volume: usize, headers: Headers) -> Option<String> {
    match headers {
        Headers::None => None,
        Headers::True => Some(format!("{HEADER_STEM}.part{}.rar", volume + 1)),
        Headers::Posted => Some(format!("{HEADER_STEM}.part{}.rar", POSTED_NUMBER[volume])),
    }
}

// The scenario: the volumes posted in true order under their posted
// names, then the recovery, arranged in `order`.
fn scenario(
    set: &[(String, Vec<u8>)],
    headers: Headers,
    recovery: Recovery,
    order: Order,
) -> Scenario {
    let truth: Vec<(String, Vec<u8>)> = set
        .iter()
        .enumerate()
        .map(|(volume, (_, bytes))| {
            (
                format!("{HEADER_STEM}.part{}.rar", volume + 1),
                bytes.clone(),
            )
        })
        .collect();
    let files: Vec<(&str, &[u8])> = truth
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut par2 = match recovery {
        Recovery::None => Vec::new(),
        _ => par2_volumes_sliced(HEADER_STEM, &files, FRAME as u64, 7, 7),
    };
    if matches!(recovery, Recovery::VolumesOnly) {
        par2.remove(0);
    }
    // Each post with the header name its articles give it.
    let mut posts: Vec<(Post, Option<String>)> = set
        .iter()
        .enumerate()
        .map(|(volume, (_, bytes))| {
            let posted = format!("{RELEASE}.part{:02}.rar", POSTED_NUMBER[volume]);
            (
                Post::plain(&posted, bytes.clone()),
                header_name(volume, headers),
            )
        })
        .collect();
    let par2: Vec<(Post, Option<String>)> = par2.into_iter().map(|post| (post, None)).collect();
    let volumes = posts.len() as u32;
    let mut together = Vec::new();
    match order {
        Order::Posted => posts.extend(par2),
        Order::Par2First => {
            let volumes = std::mem::replace(&mut posts, par2);
            posts.extend(volumes);
        }
        Order::Backwards => {
            posts.extend(par2);
            posts.reverse();
        }
        Order::SideBySide => {
            posts.extend(par2);
            together = (0..posts.len() as u32).collect();
        }
        Order::SwappedFirst => {
            posts.extend(par2);
            together = vec![5, 4];
            together.extend((0..posts.len() as u32).filter(|post| *post != 4 && *post != 5));
        }
    }
    debug_assert!(volumes as usize == VOLUMES);
    let headed: Vec<(u32, String)> = posts
        .iter()
        .enumerate()
        .filter_map(|(at, (_, header))| header.clone().map(|name| (at as u32, name)))
        .collect();
    let mut scenario =
        Scenario::new(posts.into_iter().map(|(post, _)| post).collect()).framed(FRAME);
    for (at, name) in headed {
        scenario = scenario.header(at, &name);
    }
    if !together.is_empty() {
        scenario = scenario.together(&together);
    }
    scenario
}

// What is wrong with the outcome, if anything.
fn wrong(graph: &Outcome, members: &[(String, Vec<u8>)], case: &str) -> Option<String> {
    if !graph.completed() {
        return Some(format!(
            "{case}: {:?} {:?} kinds {:?} delivery {:?} files {:?}",
            graph.status,
            graph.failure,
            kinds(graph),
            graph.graph().delivery,
            graph.files.keys().collect::<Vec<_>>(),
        ));
    }
    for (name, body) in members {
        if graph.files.get(&delivered(name)).map(Vec::as_slice) != Some(body.as_slice()) {
            return Some(format!(
                "{case}: {name} not whole; delivered {:?}",
                graph.files.keys().collect::<Vec<_>>()
            ));
        }
    }
    (!graph.graph().delivery.withheld.is_empty())
        .then(|| format!("{case}: withheld {:?}", graph.graph().delivery))
}

// Every case's failure, so a run names every shape that fails.
fn assert_none_wrong(wrong: Vec<String>) {
    assert!(
        wrong.is_empty(),
        "{} failed:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

// Every shape completes where something in the post orders the set; where
// `ordered` says nothing does, the job ends all the same (`run` fails a job
// that stalls).
async fn every_shape(
    set: Vec<(String, Vec<u8>)>,
    members: &[(String, Vec<u8>)],
    format: &str,
    ordered: impl Fn(Headers, Recovery) -> bool,
) {
    let mut failed = Vec::new();
    for headers in [Headers::None, Headers::True, Headers::Posted] {
        for recovery in [Recovery::None, Recovery::VolumesOnly, Recovery::WithIndex] {
            for order in ORDERS {
                let graph = run(&scenario(&set, headers, recovery, order)).await;
                let case = format!("{format} {headers:?} {recovery:?} {order:?}");
                if ordered(headers, recovery) {
                    failed.extend(wrong(&graph, members, &case));
                }
            }
        }
    }
    assert_none_wrong(failed);
}

// Every set's headers, or its numbered volumes' end records, order it.
fn headers_order(_: Headers, _: Recovery) -> bool {
    true
}

#[tokio::test]
async fn a_rar5_set_posted_with_its_last_volume_numbered_in_the_middle_is_one_store() {
    let members = members();
    let borrowed: Vec<(&str, Vec<u8>)> = members
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let set = multi_member_store_set(&borrowed, VOLUMES);
    every_shape(set, &members, "rar5", headers_order).await;
}

#[tokio::test]
async fn a_rar4_set_posted_with_its_last_volume_numbered_in_the_middle_is_one_store() {
    let body = payload(VOLUMES * 4 * FRAME - 3 * FRAME + 41, 77);
    let members = vec![("02-electric_red.flac".to_string(), body.clone())];
    every_shape(
        single_member_rar4_store_set_numbered(&members[0].0, &body, VOLUMES),
        &members,
        "rar4 numbered",
        headers_order,
    )
    .await;
    // Volumes whose end records state no number: with no PAR2 and no true
    // header name, the headers order them, the last volume's closing the
    // track the fifth carries on.
    every_shape(
        single_member_rar4_store_set(&members[0].0, &body, VOLUMES),
        &members,
        "rar4",
        headers_order,
    )
    .await;
}

// Two middle volumes of one track, posted under each other's numbers, in a
// set whose end records state no number, with no PAR2 and no true header
// name: each only carries the track on, so nothing in the headers orders
// them and their names place them. The track's checksum fails, so the job
// fails, every volume delivered as it came and none of them whole.
#[tokio::test]
async fn a_rar4_set_with_two_middle_volumes_swapped_ends_as_its_names_lay_it() {
    let body = payload(VOLUMES * 4 * FRAME - 3 * FRAME + 41, 77);
    let set = single_member_rar4_store_set("02-electric_red.flac", &body, VOLUMES);
    let posts: Vec<Post> = [0, 2, 1, 3, 4, 5]
        .into_iter()
        .enumerate()
        .map(|(posted, volume)| {
            Post::plain(
                &format!("{RELEASE}.part{:02}.rar", posted + 1),
                set[volume].1.clone(),
            )
        })
        .collect();
    let graph = run(&Scenario::new(posts).framed(FRAME)).await;
    let volumes: Vec<String> = (1..=VOLUMES)
        .map(|posted| format!("{RELEASE}.part{posted:02}.rar"))
        .collect();
    let failure = format!(
        "no file was delivered whole; {VOLUMES} short: {}",
        volumes.join(", ")
    );
    assert!(!graph.completed(), "{:?}", graph.status);
    assert_eq!(graph.failure.as_deref(), Some(failure.as_str()));
    assert_eq!(kinds(&graph), ["rar_store"]);
    let delivery = &graph.graph().delivery;
    assert!(delivery.whole.is_empty(), "{delivery:?}");
    assert_eq!(delivery.damaged, volumes);
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
}

// A twelve-volume RAR4 set whose end records state no number, its true
// last volume posted as `part02` and every volume after the first one
// number later, with no PAR2 and no header name, arriving in `order`. The
// last volume's header closes the track every other volume carries on, so
// it goes last; the headers do not say how many volumes come before it, so
// the set is ordered once, when the job is quiet, and its track is
// delivered whole then.
fn last_posted_second(order: &[usize]) -> (Scenario, Vec<u8>) {
    const LONG: usize = 12;
    let body = payload(LONG * 4 * FRAME - 3 * FRAME + 41, 91);
    let set = single_member_rar4_store_set("02-electric_red.flac", &body, LONG);
    // Posted number, less one, of each true volume.
    let posted: Vec<usize> = (0..LONG)
        .map(|volume| match volume {
            0 => 0,
            _ if volume == LONG - 1 => 1,
            _ => volume + 1,
        })
        .collect();
    let mut posts: Vec<(usize, Post)> = set
        .iter()
        .enumerate()
        .map(|(volume, (_, bytes))| {
            let name = format!("{RELEASE}.part{:02}.rar", posted[volume] + 1);
            (posted[volume], Post::plain(&name, bytes.clone()))
        })
        .collect();
    posts.sort_by_key(|(number, _)| *number);
    let mut scenario =
        Scenario::new(posts.into_iter().map(|(_, post)| post).collect()).framed(FRAME);
    if !order.is_empty() {
        scenario = scenario.together(&order.iter().map(|at| *at as u32).collect::<Vec<_>>());
    }
    (scenario, body)
}

fn whole_track(graph: &Outcome, body: &[u8], case: &str) {
    assert!(
        graph.completed(),
        "{case}: {:?} {:?}",
        graph.status,
        graph.failure
    );
    let delivered = delivered("02-electric_red.flac");
    assert_eq!(
        graph.files.keys().collect::<Vec<_>>(),
        [&delivered],
        "{case}"
    );
    assert!(graph.files[&delivered] == body, "{case}: track bytes");
}

// One post after another, and every post side by side, fronts first. `run`
// fails a job that stops moving, so the job going on past the order laid at
// quiet is the job's own.
#[tokio::test]
async fn a_rar4_set_with_its_last_volume_posted_second_completes_once_in() {
    let side_by_side: Vec<usize> = (0..12).collect();
    for (case, order) in [("in turn", Vec::new()), ("side by side", side_by_side)] {
        let (scenario, body) = last_posted_second(&order);
        whole_track(&run(&scenario).await, &body, case);
    }
}

// The same post under holds with room for a few articles in memory and as
// many on scratch: what waits for the order spills, and the track is
// delivered whole from what spilled.
#[tokio::test]
async fn a_rar4_set_with_its_last_volume_posted_second_completes_from_spilled_holds() {
    let tight = HoldBudgets {
        resident_limit: 4 * FRAME as u64,
        scratch_total: 8 * FRAME as u64,
        scratch_ceiling: 4 * FRAME as u64,
        disk_reserve: 0,
    };
    let (lines, guard) = Lines::capture();
    let (scenario, body) = last_posted_second(&[]);
    let graph = run(&scenario.holds(tight)).await;
    drop(guard);
    whole_track(&graph, &body, "tight");
    assert!(!lines.of("holds spilled").is_empty(), "holds spilled");
    let breached: Vec<_> = lines
        .of("object refused")
        .into_iter()
        .filter(|line| line.contains("why=holds_breached"))
        .collect();
    assert!(breached.is_empty(), "{breached:?}");
}

// The bench job's post as Weaver took it in: the PAR2 volumes posted as
// `<release>.vol-01.par2` to `vol-07.par2` with no index, the first of them
// ahead of every volume, renaming each volume by its content to the
// poster's one-digit `<stem>.partN.rar`; the volumes posted under the
// release's `part01` to `part06`, the true last volume as `part05` and the
// true fifth as `part06`; and the posts taken in the bench's order:
// `vol-01`, `part01` to `part03`, `vol-02` to `vol-05`, `part04`, `vol-06`,
// `vol-07`, `part05`, `part06`.
fn bench_posts(set: &[(String, Vec<u8>)]) -> Vec<Post> {
    let truth: Vec<(String, Vec<u8>)> = set
        .iter()
        .enumerate()
        .map(|(volume, (_, bytes))| {
            (
                format!("{HEADER_STEM}.part{}.rar", volume + 1),
                bytes.clone(),
            )
        })
        .collect();
    let files: Vec<(&str, &[u8])> = truth
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut par2 = par2_volumes_sliced(HEADER_STEM, &files, FRAME as u64, 7, 7);
    par2.remove(0);
    let par2: Vec<Post> = par2
        .into_iter()
        .enumerate()
        .map(|(at, post)| Post {
            // Each in one article, as the bench's were in their first: the
            // set is read, and renames, as soon as a volume is bound.
            least_article: post.bytes.len(),
            ..Post::plain(&format!("{RELEASE}.vol-{:02}.par2", at + 1), post.bytes)
        })
        .collect();
    let volume = |volume: usize| {
        Post::plain(
            &format!("{RELEASE}.part{:02}.rar", POSTED_NUMBER[volume]),
            set[volume].1.clone(),
        )
    };
    let mut par2 = par2.into_iter();
    let mut posts = vec![par2.next().unwrap(), volume(0), volume(1), volume(2)];
    posts.extend(par2.by_ref().take(4));
    posts.push(volume(3));
    posts.extend(par2);
    posts.extend([volume(5), volume(4)]);
    posts
}

async fn the_bench_post(set: Vec<(String, Vec<u8>)>, members: &[(String, Vec<u8>)], format: &str) {
    let posts = || bench_posts(&set);
    let count = posts().len() as u32;
    let scenarios = [
        ("in turn", Scenario::new(posts()).framed(FRAME)),
        (
            "headers",
            (0..VOLUMES).fold(Scenario::new(posts()).framed(FRAME), |scenario, volume| {
                let at = posts()
                    .iter()
                    .position(|post| post.bytes == set[volume].1)
                    .unwrap() as u32;
                scenario.header(at, &header_name(volume, Headers::True).unwrap())
            }),
        ),
        (
            "side by side",
            Scenario::new(posts())
                .framed(FRAME)
                .together(&(0..count).collect::<Vec<_>>()),
        ),
    ];
    let mut failed = Vec::new();
    for (case, scenario) in scenarios {
        let graph = run(&scenario).await;
        failed.extend(wrong(&graph, members, &format!("{format} bench {case}")));
    }
    assert_none_wrong(failed);
}

#[tokio::test]
async fn the_bench_post_with_its_last_volume_numbered_in_the_middle_is_one_store() {
    let members = members();
    let borrowed: Vec<(&str, Vec<u8>)> = members
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    the_bench_post(multi_member_store_set(&borrowed, VOLUMES), &members, "rar5").await;
    let body = payload(VOLUMES * 4 * FRAME - 3 * FRAME + 41, 77);
    let single = vec![("02-electric_red.flac".to_string(), body.clone())];
    the_bench_post(
        single_member_rar4_store_set_numbered(&single[0].0, &body, VOLUMES),
        &single,
        "rar4 numbered",
    )
    .await;
}

// The compressed four-volume RAR4 set as RAR wrote it, but with its end
// records stating no volume number, as an old writer's do: a set only its
// names order, and one only the engine reads.
fn lz_four_unnumbered() -> Vec<(String, Vec<u8>)> {
    super::rar_rev::lz_four()
        .into_iter()
        .map(|(name, mut bytes)| {
            // The end record is the volume's last header: its type, its
            // flags with VOLUME_NUMBER (0x0008) cleared, and its CRC made
            // whole again over what follows the CRC field.
            let size = 20;
            let at = bytes.len() - size;
            assert_eq!(bytes[at + 2], 0x7b, "{name} ends on its end record");
            bytes[at + 3] &= !0x08;
            let crc = (par2_rs::checksum::crc32(&bytes[at + 2..]) & 0xffff) as u16;
            bytes[at..at + 2].copy_from_slice(&crc.to_le_bytes());
            (name, bytes)
        })
        .collect()
}

// The set posted under a release's names out of order, two volumes swapped
// or every one backwards, then PAR2 recovery volumes, with or without the
// index, naming each volume under its true one-digit name: the recovery
// renames every volume, one at a time, to a stem none of the posted names
// carries. Every volume arrives whole.
#[tokio::test]
async fn a_compressed_rar4_set_posted_swapped_and_renamed_by_par2_volumes_delivers() {
    for index in [false, true] {
        for posted in [[1, 2, 4, 3], [2, 1, 3, 4], [4, 3, 2, 1]] {
            renamed_by_par2_delivers(index, posted).await;
        }
    }
}

const LZ_FRAME: usize = 2048;

async fn renamed_by_par2_delivers(index: bool, posted: [u32; 4]) {
    let posts = renamed_posts(index, posted);
    let graph = run(&Scenario::new(posts).framed(LZ_FRAME)).await;
    assert!(
        graph.completed(),
        "index {index} posted {posted:?}: {:?} {:?} kinds {:?} delivery {:?}",
        graph.status,
        graph.failure,
        kinds(&graph),
        graph.graph().delivery
    );
    assert_lz_four_delivered(&graph.files, &format!("index {index} posted {posted:?}"));
}

// The same set under the index and the recovery volumes, posted in order or
// with its last two numbers swapped, its articles served in one order: the
// front of the first volume, then the last volume whole, then the index,
// so the recovery renames the last volume first while neither volume
// beside it has come, and only then the two between, the rest of the first
// and the recovery volumes. The set is one engine at its true places and
// delivers its member as RAR wrote it.
#[tokio::test]
async fn a_compressed_rar4_volume_renamed_before_its_neighbours_come_delivers() {
    use super::restart::{Down, run_restarting_at};
    use crate::jobs::model::JobStatus;
    for posted in [[1, 2, 3, 4], [1, 2, 4, 3]] {
        let posts = renamed_posts(true, posted);
        let articles: Vec<u32> = posts
            .iter()
            .map(|post| post.bytes.len().div_ceil(LZ_FRAME) as u32)
            .collect();
        let all = |file: u32| (0..articles[file as usize]).map(move |at| (file, at));
        let order: Vec<(u32, u32)> = (0..2)
            .map(|at| (0, at))
            .chain(all(3))
            .chain(all(4))
            .chain(all(1))
            .chain(all(2))
            .chain((2..articles[0]).map(|at| (0, at)))
            .chain((5..posts.len() as u32).flat_map(all))
            .collect();
        let scenario = Scenario::new(posts).framed(LZ_FRAME).first(&order);
        let served = run_restarting_at(&scenario, &mut |_| {}, Down::Crash, &|_| false).await;
        assert_eq!(served.restarts, 0);
        assert_eq!(
            served.order[..order.len().min(served.order.len())],
            order[..order.len().min(served.order.len())],
            "posted {posted:?}: served out of the order given"
        );
        assert_eq!(
            served.status,
            Some(JobStatus::Complete),
            "posted {posted:?}: {:?}",
            served.graph.delivery
        );
        assert_lz_four_delivered(&served.files, &format!("posted {posted:?}"));
    }
}

// The set posted under a release's names, numbered as `posted` gives each
// volume in true order, then PAR2 recovery volumes, after the index or
// without it, naming each volume under its true one-digit name.
fn renamed_posts(index: bool, posted: [u32; 4]) -> Vec<Post> {
    let set = lz_four_unnumbered();
    let truth: Vec<(String, &[u8])> = set
        .iter()
        .enumerate()
        .map(|(volume, (_, bytes))| {
            (
                format!("{HEADER_STEM}.part{}.rar", volume + 1),
                bytes.as_slice(),
            )
        })
        .collect();
    let files: Vec<(&str, &[u8])> = truth
        .iter()
        .map(|(name, bytes)| (name.as_str(), *bytes))
        .collect();
    let mut par2 = par2_volumes_sliced(HEADER_STEM, &files, LZ_FRAME as u64, 4, 4);
    if !index {
        par2.remove(0);
    }
    let mut posts: Vec<Post> = set
        .iter()
        .zip(posted)
        .map(|((_, bytes), number)| {
            Post::plain(&format!("{RELEASE}.part{number:02}.rar"), bytes.clone())
        })
        .collect();
    posts.extend(par2);
    posts
}

// The set's one member delivered, alone, as RAR wrote it.
fn assert_lz_four_delivered(files: &std::collections::BTreeMap<String, Vec<u8>>, label: &str) {
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles/expected.json"
    )))
    .unwrap();
    let expected = &oracle["archives"]["rar4_lz_four"]["compressible.txt"];
    let member = delivered("compressible.txt");
    assert_eq!(
        files.keys().cloned().collect::<Vec<_>>(),
        vec![member.clone()],
        "{label}"
    );
    assert_eq!(
        files[&member].len() as u64,
        expected["size"].as_u64().unwrap(),
        "{label}"
    );
    use sha2::Digest;
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&files[&member])),
        expected["sha256"].as_str().unwrap(),
        "{label}"
    );
}
