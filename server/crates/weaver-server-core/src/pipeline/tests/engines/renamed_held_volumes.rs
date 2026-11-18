// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A stored RAR4 set posted as a pack's was. The NZB names each volume
// `<release>.partNN.rar`, every article's header names it under a stem with
// its vowels stripped, and a PAR2 set over the true names renames every
// volume by its content. The first volume's file header names the member
// `[site] title.mp4`; every later one names it `[site.com] title.mp4`, as
// the packer that wrote the set did. RAR reads a continuation as the part
// the volume before carries on, whatever its header names it (unrar's
// MergeArchive compares no names), and 7-Zip lists the set as one member.
// Every byte arrives, so the member is delivered whole under the name its
// first part gives it, as it is when every header names it alike.

use super::harness::{Outcome, Post, Scenario, kinds, par2_volumes_sliced, payload, run};
use super::rar::delivered;
use crate::pipeline::tests::direct_store::{
    single_member_rar4_store_set, single_member_rar4_store_set_numbered,
};

const FIRST: &str = "[site] double date 21.02.2017.mp4";
const LATER: &str = "[site.com] double date  21.02.2017.mp4";
// Articles, and PAR2 slices, of 512 KiB: each volume is twelve of them,
// six MiB, past what a front ever shows.
const FRAME: usize = 512 * 1024;
const VOLUMES: usize = 5;

const TRUE_STEM: &str = "12_[site].double.date.21.02.2017";
const HEADER_STEM: &str = "12_st.dbl.dt.21.02.2017";

fn body() -> Vec<u8> {
    payload(VOLUMES * 12 * FRAME - 4_099, 71)
}

// The posts: the PAR2 index and its volume first, then each volume under
// its posted name, its articles naming it by the stripped stem.
fn posted(set: Vec<Vec<u8>>) -> Scenario {
    let truth: Vec<(String, Vec<u8>)> = set
        .into_iter()
        .enumerate()
        .map(|(index, bytes)| (format!("{TRUE_STEM}.part{:02}.rar", index + 1), bytes))
        .collect();
    let files: Vec<(&str, &[u8])> = truth
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = par2_volumes_sliced("Pack.XXX-U4A", &files, FRAME as u64, 4, 1);
    let first = posts.len() as u32;
    posts.extend(truth.iter().enumerate().map(|(index, (_, bytes))| {
        Post::plain(
            &format!("Pack.XXX-U4A.part{:02}.rar", index + 1),
            bytes.clone(),
        )
    }));
    (0..VOLUMES as u32).fold(Scenario::new(posts).framed(FRAME), |scenario, volume| {
        scenario.header(
            first + volume,
            &format!("{HEADER_STEM}.part{:02}.rar", volume + 1),
        )
    })
}

// A writer of a single-member stored set: member name, body, volumes.
type Build = fn(&str, &[u8], usize) -> Vec<(String, Vec<u8>)>;

// The set `build` writes, its first volume naming the member `first` and
// every later one `later`.
fn set(
    build: Build,
    body: &[u8],
    first: &str,
    later: &str,
) -> Vec<Vec<u8>> {
    let head = build(first, body, VOLUMES).remove(0).1;
    std::iter::once(head)
        .chain(
            build(later, body, VOLUMES)
                .into_iter()
                .skip(1)
                .map(|(_, bytes)| bytes),
        )
        .collect()
}

fn assert_delivers(graph: &Outcome, body: &[u8], case: &str) {
    assert!(
        graph.completed(),
        "{case}: {:?} {:?} kinds {:?} delivery {:?}",
        graph.status,
        graph.failure,
        kinds(graph),
        graph.graph().delivery,
    );
    assert_eq!(
        graph.files.get(&delivered(FIRST)).map(Vec::as_slice),
        Some(body),
        "{case}: delivered {:?}",
        graph.files.keys().collect::<Vec<_>>()
    );
    assert!(
        graph.graph().delivery.withheld.is_empty(),
        "{case}: {:?}",
        graph.graph().delivery
    );
}

#[tokio::test]
async fn a_set_whose_first_part_names_its_member_otherwise_delivers() {
    let body = body();
    let builds: [(&str, Build); 2] = [
        ("rar4 numbered", single_member_rar4_store_set_numbered),
        ("rar4", single_member_rar4_store_set),
    ];
    for (format, build) in builds {
        // The control: every header names the member alike.
        let alike = run(&posted(set(build, &body, FIRST, FIRST))).await;
        assert_delivers(&alike, &body, &format!("{format} named alike"));
        let renamed = run(&posted(set(build, &body, FIRST, LATER))).await;
        assert_delivers(&renamed, &body, &format!("{format} named otherwise"));
    }
}
