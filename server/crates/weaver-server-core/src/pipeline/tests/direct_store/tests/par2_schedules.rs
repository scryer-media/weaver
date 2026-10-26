// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Member coverage under a PAR2 set through the graph engine's direct store:
// one stored RAR volume, plain or with its data encrypted, whose articles
// arrive as a prefix, with a hole, or whole, in every order of the three
// groups they fall in, with the pipeline taken down between groups. The set
// carries its slice checksums, or only its whole-file hashes. Whatever
// arrives, the set mends the volume and the member is delivered byte for
// byte, as a run served in posting order delivers it.

use super::super::scenarios::encrypted_store_set;
use super::super::single_member_store_set;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::engines::harness::{
    ARTICLE, Outcome, Post, Scenario, par2_volumes, payload, run,
};
use crate::pipeline::tests::engines::restart::{Down, Restarted, run_restarting_at};

const MEMBER: &str = "source.bin";
const SECRET: &str = "lantern-coverage-secret";

// How much of the volume the server has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Coverage {
    // Every article up to and including the middle one; the rest are lost.
    Prefix,
    // Every article but the middle one.
    Hole,
    Whole,
}

const COVERAGES: [Coverage; 3] = [Coverage::Prefix, Coverage::Hole, Coverage::Whole];

// The PAR2 packet type of a slice-checksum packet.
const SLICE_CHECKSUMS: &[u8; 16] = b"PAR 2.0\0IFSC\0\0\0\0";

// The packets of a PAR2 file with every slice-checksum packet left out.
fn without_slice_checksums(bytes: &[u8]) -> Vec<u8> {
    const HEADER: usize = 64;
    let mut kept = Vec::new();
    let mut at = 0;
    while at + HEADER <= bytes.len() {
        assert_eq!(&bytes[at..at + 8], b"PAR2\0PKT", "a packet at {at}");
        let length = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap()) as usize;
        if &bytes[at + 48..at + 64] != SLICE_CHECKSUMS {
            kept.extend_from_slice(&bytes[at..at + length]);
        }
        at += length;
    }
    kept
}

struct Fixture {
    member: Vec<u8>,
    // The volume, then the set's index and volumes.
    posts: Vec<(String, Vec<u8>)>,
    encrypted: bool,
}

impl Fixture {
    fn new(encrypted: bool, checksums: bool) -> Self {
        let member = payload(557, 64);
        let volume = if encrypted {
            encrypted_store_set(MEMBER, &member, 1, SECRET, Some(SECRET), false).remove(0)
        } else {
            single_member_store_set(MEMBER, &member, 1).remove(0)
        };
        let set = par2_volumes(
            "lantern",
            &[(volume.0.as_str(), volume.1.as_slice())],
            12,
            4,
        );
        let mut posts = vec![volume];
        posts.extend(set.into_iter().map(|post| {
            let bytes = if checksums {
                post.bytes
            } else {
                without_slice_checksums(&post.bytes)
            };
            (post.name, bytes)
        }));
        Self {
            member,
            posts,
            encrypted,
        }
    }

    fn articles(&self) -> u32 {
        self.posts[0].1.len().div_ceil(ARTICLE) as u32
    }

    // The volume's articles in three groups: before the middle one, the
    // middle one, and after it.
    fn groups(&self) -> [Vec<(u32, u32)>; 3] {
        let middle = self.articles() / 2;
        [
            (0..middle).map(|ordinal| (0, ordinal)).collect(),
            vec![(0, middle)],
            (middle + 1..self.articles())
                .map(|ordinal| (0, ordinal))
                .collect(),
        ]
    }

    fn scenario(&self, coverage: Coverage) -> Scenario {
        let posts = self
            .posts
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect();
        let mut scenario = Scenario::new(posts);
        if self.encrypted {
            scenario = scenario.password(SECRET);
        }
        let [_, middle, tail] = self.groups();
        let lost = match coverage {
            Coverage::Prefix => tail,
            Coverage::Hole => middle,
            Coverage::Whole => Vec::new(),
        };
        for (file, ordinal) in lost {
            scenario = scenario.lost(file, ordinal);
        }
        scenario
    }
}

fn member(outcome_files: &std::collections::BTreeMap<String, Vec<u8>>) -> Option<&Vec<u8>> {
    outcome_files
        .iter()
        .find(|(path, _)| path.rsplit('/').next() == Some(MEMBER))
        .map(|(_, bytes)| bytes)
}

fn judge(fixture: &Fixture, whole: &Outcome, context: &str) {
    assert_eq!(
        whole.status,
        Some(JobStatus::Complete),
        "{context}: {:?} {:?}",
        whole.failure,
        whole.files.keys().collect::<Vec<_>>()
    );
    assert!(
        member(&whole.files) == Some(&fixture.member),
        "{context}: the member is not delivered whole: {:?}",
        whole.files.keys().collect::<Vec<_>>()
    );
}

fn agree(ran: &Restarted, whole: &Outcome, context: &str) {
    assert_eq!(ran.status, whole.status, "{context}: status");
    assert_eq!(ran.files, whole.files, "{context}: delivered files");
    assert!(
        ran.from_disk.is_empty(),
        "{context}: fetched again though on disk: {:?}",
        ran.from_disk
    );
}

async fn campaign(encrypted: bool, checksums: bool) {
    let fixture = Fixture::new(encrypted, checksums);
    for coverage in COVERAGES {
        let context = format!("encrypted={encrypted} checksums={checksums} {coverage:?}");
        let whole = run(&fixture.scenario(coverage)).await;
        judge(&fixture, &whole, &context);
        // Every order of the three groups, the pipeline taken down after
        // each group but the last.
        let groups = fixture.groups();
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let mut first = Vec::new();
            let mut downs = Vec::new();
            for group in order {
                first.extend(groups[group].iter().copied());
                downs.push(first.len());
            }
            downs.pop();
            let scenario = fixture.scenario(coverage).first(&first);
            let ran = run_restarting_at(&scenario, &mut |_| {}, Down::Crash, &|asked| {
                downs.contains(&asked)
            })
            .await;
            agree(&ran, &whole, &format!("{context} order={order:?}"));
        }
    }
}

#[tokio::test]
async fn plain_direct_volume_schedules() {
    campaign(false, true).await;
}

#[tokio::test]
async fn encrypted_direct_volume_schedules() {
    campaign(true, true).await;
}

#[tokio::test]
async fn plain_no_ifsc_schedules() {
    campaign(false, false).await;
}

#[tokio::test]
async fn encrypted_no_ifsc_schedules() {
    campaign(true, false).await;
}
