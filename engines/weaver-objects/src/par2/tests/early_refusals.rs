// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A refusal reaches the set while the payload still comes: the set asks for
// the blocks the refused slices need then, the way it asks after a repair
// that came up short, so its first repair finds them in hand.

use super::*;

// Ten slices; two articles of the payload, slices 2 to 5, never come.
const LENGTH: usize = 5000;
const LOST: [ByteRange; 2] = [
    ByteRange {
        start: 2 * ARTICLE,
        end: 3 * ARTICLE,
    },
    ByteRange {
        start: 3 * ARTICLE,
        end: 4 * ARTICLE,
    },
];

// The rest of a volume whose head arrived, and its close.
fn rest(job: &mut Job<Par2>, volume: weaver_graph::VolumeIndex, file: &(String, Vec<u8>)) {
    let mut out = Vec::new();
    let length = file.1.len() as u64;
    job.object.sized(volume, length, &mut out);
    job.object.deliver(
        weaver_graph::Delivery {
            volume,
            offset: ARTICLE,
            bytes: &weaver_graph::Evidenced::from(file.1[ARTICLE as usize..].to_vec()),
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut out,
    );
    job.object.volume_closed(volume, &mut out);
}

// The index in hand, every recovery volume's head sampled, and the payload
// arriving but for the lost articles, whose bytes the store holds as zeros.
fn downloading(
    data: &[u8],
    files: &[(String, Vec<u8>)],
) -> (Job<Par2>, MemberId, Vec<weaver_graph::VolumeIndex>) {
    let mut job = job(&files[..1]);
    let volumes: Vec<_> = files[1..].iter().map(|file| head(&mut job, file)).collect();
    let member = job
        .namespace
        .announce("payload.bin", Some(data.len() as u64));
    let mut held = data.to_vec();
    for lost in LOST {
        held[lost.as_range().start as usize..lost.end as usize].fill(0);
    }
    job.store.members.insert(member, held);
    let arrived = [
        ByteRange::new(0, LOST[0].start),
        ByteRange::new(LOST[1].end, data.len() as u64),
    ];
    for range in arrived {
        let bytes = &data[range.as_range().start as usize..range.end as usize];
        job.arrive(member, range.start, bytes, Provenance::Fresh);
    }
    bind(&mut job);
    for range in arrived {
        let bytes = &data[range.as_range().start as usize..range.end as usize];
        let pieces = segments(range.start, bytes, ARTICLE, BLOCK, true);
        job.object.feed(member, &pieces);
    }
    (job, member, volumes)
}

fn now(object: &Par2) -> Vec<weaver_graph::VolumeIndex> {
    seeks(object)
        .iter()
        .filter(|seek| seek.when == weaver_graph::When::Now)
        .map(|seek| seek.volume)
        .collect()
}

// The refused articles ask for the volumes their four slices need while the
// payload still comes, the same volumes a first repair short of them asked
// for before; with those in hand, one repair rebuilds the payload, and a
// set that rebuilt asks for nothing more.
#[test]
fn refused_articles_ask_for_their_blocks_before_the_first_repair() {
    let data = payload(61, LENGTH);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 12, 3);

    // As before: the first repair comes up short and says what it needs.
    let (mut before, member, _) = downloading(&data, &files);
    for lost in LOST {
        Recovery::refused(&mut before.object, member, lost);
    }
    assert_eq!(
        before
            .object
            .repair(&mut before.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    let after_repair = now(&before.object);
    assert!(!after_repair.is_empty());

    let (mut job, member, volumes) = downloading(&data, &files);
    assert!(now(&job.object).is_empty(), "nothing lost, nothing asked");
    for lost in LOST {
        Recovery::refused(&mut job.object, member, lost);
    }
    assert_eq!(job.object.needs_seen(&job.object.order[0]), (0, 4));
    let early = now(&job.object);
    assert_eq!(
        early, after_repair,
        "the same volumes, asked before any repair"
    );
    assert!(
        early.len() < volumes.len(),
        "the shortfall leaves a volume parked"
    );
    for (volume, file) in volumes.iter().zip(&files[1..]) {
        if early.contains(volume) {
            rest(&mut job, *volume, file);
        }
    }
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Repaired { slices: 4 }
    );
    assert_eq!(job.store.members[&member], data);
    assert!(
        now(&job.object).is_empty(),
        "a rebuilt set asks for no more"
    );
}

// No refusal and no damage: every volume stays parked.
#[test]
fn a_whole_payload_unparks_nothing() {
    let data = payload(62, LENGTH);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 12, 3);
    let mut job = job(&files[..1]);
    let volumes: Vec<_> = files[1..].iter().map(|file| head(&mut job, file)).collect();
    let member = job.member("payload.bin", &data);
    bind(&mut job);
    feed(&mut job, member, &data, None);
    assert!(now(&job.object).is_empty());
    let deferred = seeks(&job.object)
        .iter()
        .filter(|seek| seek.when == weaver_graph::When::Deferred)
        .count();
    assert_eq!(deferred, volumes.len());
}

// A slice the checksums say is damaged, and that a refusal also touches, is
// counted once, whichever came first.
#[test]
fn a_refused_slice_that_came_damaged_is_counted_once() {
    let data = payload(63, LENGTH);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 12, 3);
    let mut wrong = data.clone();
    wrong[10] ^= 0x5A;
    wrong[600] ^= 0x5A;
    let first = ByteRange::new(0, ARTICLE);
    for refused_first in [true, false] {
        let mut job = job(&files[..1]);
        for file in &files[1..] {
            head(&mut job, file);
        }
        let (member, _) = damaged_member(&mut job, "payload.bin", &data, 10);
        job.store.members.insert(member, wrong.clone());
        bind(&mut job);
        if refused_first {
            Recovery::refused(&mut job.object, member, first);
            assert_eq!(job.object.needs_seen(&job.object.order[0]), (0, 2));
        }
        feed(&mut job, member, &wrong, Some(first));
        if !refused_first {
            Recovery::refused(&mut job.object, member, first);
        }
        assert_eq!(
            job.object.needs_seen(&job.object.order[0]),
            (2, 0),
            "refused first: {refused_first}"
        );
    }
}

// Refused slices past every block the set can reach rule it impossible, as
// they did, and ask for no volume: none could rebuild it.
#[test]
fn a_loss_beyond_recovery_rules_impossible_and_asks_for_nothing() {
    let data = payload(64, LENGTH);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 3, 2);
    let (mut job, member, _) = downloading(&data, &files);
    for lost in LOST {
        Recovery::refused(&mut job.object, member, lost);
    }
    assert!(now(&job.object).is_empty());
    assert_eq!(
        job.object.outcome(),
        Some(RepairOutcome::Impossible {
            damaged: 4,
            reachable: 3
        })
    );
}
