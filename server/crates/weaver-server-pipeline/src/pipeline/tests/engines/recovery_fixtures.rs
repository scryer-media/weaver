// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod dead_posts {
    use super::super::harness::*;

    // Twelve files of four articles each.
    pub fn files() -> Vec<Post> {
        (0..12u8)
            .map(|at| Post::plain(&format!("lantern{at:02}.mkv"), payload(4 * ARTICLE, at)))
            .collect()
    }
}

pub mod deep {
    use super::super::depth::delivered;
    use super::super::harness::{Outcome, Post, Scenario, par2_set, run};

    // The scenario of `assert_repaired_header_places_the_bytes_behind_it`, and
    // `inner` run alone: what it delivers.
    pub type WrapMembers = fn(Vec<(&str, Vec<u8>)>) -> (String, Vec<u8>);

    pub async fn repaired_header(
        inner: Vec<u8>,
        first: &str,
        wrap: WrapMembers,
    ) -> (Outcome, Scenario) {
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
    pub fn vint(bytes: &[u8]) -> (u64, usize) {
        let mut value = 0u64;
        for (index, byte) in bytes.iter().enumerate() {
            value |= u64::from(byte & 0x7f) << (7 * index);
            if byte & 0x80 == 0 {
                return (value, index + 1);
            }
        }
        panic!("a vint runs to its end");
    }
}

pub mod par3 {
    use super::super::harness::{Outcome, repair_and_terminal_events};
    use super::super::rar_fixtures::delivered;
    use std::collections::BTreeMap;
    use weaver_graph::recovery::RepairOutcome;

    // Every named file delivered with its bytes, and nothing else.
    pub fn assert_delivers(outcome: &Outcome, files: &[(&str, &[u8])]) {
        assert!(
            outcome.completed(),
            "{:?} {:?}",
            outcome.status,
            outcome.failure
        );
        let expected: BTreeMap<String, &[u8]> = files
            .iter()
            .map(|(name, bytes)| (delivered(name), *bytes))
            .collect();
        let got: BTreeMap<String, &[u8]> = outcome
            .files
            .iter()
            .map(|(name, bytes)| (name.clone(), bytes.as_slice()))
            .collect();
        assert!(got == expected, "{:?}", got.keys());
    }

    pub fn assert_repaired_and_announced(graph: &Outcome, outcomes: &[RepairOutcome], slices: u64) {
        assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
        let expected: Vec<Option<RepairOutcome>> = outcomes.iter().copied().map(Some).collect();
        assert_eq!(graph.graph().outcomes, expected);
        assert_eq!(
            repair_and_terminal_events(graph),
            vec![
                format!("RepairComplete {slices}"),
                "JobCompleted".to_string()
            ]
        );
    }
}

pub mod plain_par2 {
    use super::super::harness::{Post, par2_set};

    // Plain files followed by one PAR2 set over them, the index then the
    // volume; the files are numbered as listed, the index and volume after.
    pub fn covered(files: &[(&str, Vec<u8>)], blocks: u32) -> Vec<Post> {
        let described: Vec<(&str, &[u8])> = files
            .iter()
            .map(|(name, bytes)| (*name, bytes.as_slice()))
            .collect();
        let mut posts: Vec<Post> = files
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect();
        posts.extend(par2_set("lantern", &described, blocks));
        posts
    }
}

pub mod reposted {
    use super::super::harness::{ARTICLE, Outcome, Post, payload};

    pub const MEMBER: &str = "beacon.bin";

    pub fn body() -> Vec<u8> {
        payload(7 * ARTICLE + 13, 51)
    }

    // The volumes, then each again under its number padded otherwise.
    pub fn twice(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
        let copies: Vec<(String, Vec<u8>)> = volumes
            .iter()
            .map(|(name, bytes)| (name.replace(".part0", ".part"), bytes.clone()))
            .collect();
        volumes
            .into_iter()
            .chain(copies)
            .map(|(name, bytes)| Post::plain(&name, bytes))
            .collect()
    }

    pub fn assert_one_member(graph: &Outcome, body: &[u8]) {
        assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
        let name = format!("lantern beacon/{MEMBER}");
        assert_eq!(
            graph.files.keys().cloned().collect::<Vec<_>>(),
            vec![name.clone()],
            "delivered names"
        );
        assert!(graph.files[&name] == body, "member bytes");
        assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    }
}

pub mod seek {
    use super::super::harness::{ARTICLE, Outcome, Post, payload};
    use crate::pipeline::tests::direct_store::multi_member_store_set;

    pub const FOLDER: &str = "lantern beacon";

    pub fn members(count: usize, seed: u8) -> Vec<(String, Vec<u8>)> {
        (0..count)
            .map(|index| {
                (
                    format!("beacon{index}.mkv"),
                    payload(9 * ARTICLE + 7 * (index + 1), seed + index as u8),
                )
            })
            .collect()
    }

    pub fn store_set(members: &[(String, Vec<u8>)], volumes: usize) -> Vec<(String, Vec<u8>)> {
        let named: Vec<(&str, Vec<u8>)> = members
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.clone()))
            .collect();
        multi_member_store_set(&named, volumes)
    }

    pub fn posts(volumes: &[(String, Vec<u8>)]) -> Vec<Post> {
        volumes
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect()
    }

    pub fn named(files: &[(String, Vec<u8>)]) -> Vec<(&str, &[u8])> {
        files
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect()
    }

    // Every member delivered at its name with its bytes, and no article asked
    // for twice.
    pub fn assert_members(outcome: &Outcome, members: &[(String, Vec<u8>)]) {
        assert_members_delivered(outcome, members);
        assert_eq!(outcome.refetched(), Vec::new(), "refetched",);
    }

    // Every member delivered at its name with its bytes, and nothing else.
    pub fn assert_members_delivered(outcome: &Outcome, members: &[(String, Vec<u8>)]) {
        assert!(
            outcome.completed(),
            "{:?} {:?}",
            outcome.status,
            outcome.failure
        );
        let mut expected: Vec<String> = members
            .iter()
            .map(|(name, _)| format!("{FOLDER}/{name}"))
            .collect();
        expected.sort();
        assert_eq!(
            outcome.files.keys().cloned().collect::<Vec<_>>(),
            expected,
            "delivered names",
        );
        for (name, bytes) in members {
            assert!(
                outcome.files[&format!("{FOLDER}/{name}")] == *bytes,
                "{name} bytes",
            );
        }
    }
}
