// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the object builds again on every call, and what it no longer does.

use std::sync::atomic::Ordering;

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{Provenance, Recovery, SliceVerdict};

use super::{BLOCK, OWN, bind, damaged_member, feed, job, verdicts, wanted};
use crate::par2::Par2;
use crate::par2::evidence::VETOED_READS;
use crate::testing::{Job, par2_set, payload};

// Two sets on two grids: every feed lists the grids in the object's own
// room, so only the first needs new room for them.
#[test]
fn feeds_list_the_grids_in_one_room() {
    let one = payload(31, 5000);
    let two = payload(32, 6000);
    let mut files = par2_set("first", &[("one.bin", &one)], BLOCK, 2, 1);
    files.extend(par2_set("second", &[("two.bin", &two)], 1024, 2, 1));
    let mut job = job(&files);
    let first = job.member("one.bin", &one);
    let second = job.member("two.bin", &two);
    bind(&mut job);
    for _ in 0..5 {
        feed(&mut job, first, &one, None);
        feed(&mut job, second, &two, None);
    }
    assert_eq!(job.object.grid_room, 1);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
}

// Asked before any set is found, the object reads the index for one: one
// sort of the volumes serves the whole call.
#[test]
fn wants_sorts_the_volumes_once() {
    let data = payload(33, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 2);
    let mut job = Job::new(Par2::new(OWN), OWN);
    let volumes: Vec<_> = files
        .iter()
        .rev()
        .map(|(name, bytes)| job.name(name, bytes))
        .collect();
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    let mut seeks = Vec::new();
    Recovery::wants(&job.object, &mut seeks);
    assert_eq!(job.object.volume_sorts.load(Ordering::Relaxed), 1);
    // The index, named last, carries no blocks: it is the one read.
    let index = *volumes.last().unwrap();
    assert!(!seeks.is_empty());
    assert!(seeks.iter().all(|seek| seek.volume == index));
}

// The bound members' lengths are listed again only when a binding or a
// length changed: a bind over the same namespace leaves them be.
#[test]
fn binding_lists_lengths_only_when_one_changed() {
    let data = payload(34, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    let member = job
        .namespace
        .announce("payload.bin", Some(data.len() as u64));
    job.arrive(member, 0, &data[..2800], Provenance::Fresh);
    let lists = |job: &mut Job<Par2>| {
        bind(job);
        (
            job.object.length_lists,
            job.object.lengths.get(&member).copied(),
        )
    };
    assert_eq!(lists(&mut job), (1, None));
    assert_eq!(lists(&mut job), (1, None));
    job.arrive(member, 2800, &data[2800..], Provenance::Fresh);
    assert_eq!(lists(&mut job), (2, Some(5000)));
    assert_eq!(lists(&mut job), (2, Some(5000)));
}

// Fifty members of one set, two of them damaged. Each member's verdicts
// read the sets in the order they were filed when found, and what is wanted
// reads each set's damage where its members' slices were judged: neither
// puts the sets in order again nor reads every member's slices again.
#[test]
fn verdicts_and_wants_read_the_order_and_the_damage_the_object_keeps() {
    let parts: Vec<(String, Vec<u8>)> = (0..50)
        .map(|index| (format!("part{index:02}.bin"), payload(100 + index, 1024)))
        .collect();
    let described: Vec<(&str, &[u8])> = parts
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let files = par2_set("set", &described, BLOCK, 4, 2);
    let mut job = job(&files[..1]);
    let later: Vec<_> = files[1..]
        .iter()
        .map(|(name, bytes)| job.name(name, bytes))
        .collect();
    let mut members = Vec::new();
    for (index, (name, bytes)) in parts.iter().enumerate() {
        if index % 25 == 7 {
            let (member, bad) = damaged_member(&mut job, name, bytes, 100);
            members.push((member, Some(bad)));
        } else {
            members.push((job.member(name, bytes), None));
        }
    }
    bind(&mut job);
    for (member, bad) in &members {
        let bytes = job.store.members[member].clone();
        feed(&mut job, *member, &bytes, *bad);
    }

    job.object.set_orderings.store(0, Ordering::Relaxed);
    let judged: Vec<Vec<SliceVerdict>> = members
        .iter()
        .map(|(member, _)| verdicts(&job, *member))
        .collect();
    let orderings = job.object.set_orderings.load(Ordering::Relaxed);
    let intact = vec![SliceVerdict::Intact, SliceVerdict::Intact];
    let damaged = vec![SliceVerdict::Damaged, SliceVerdict::Unknown];
    for (index, verdict) in judged.iter().enumerate() {
        let expected = if index % 25 == 7 { &damaged } else { &intact };
        assert_eq!(verdict, expected, "part{index:02}.bin");
    }

    VETOED_READS.with(|reads| reads.set(0));
    let asked = wanted(&mut job);
    let reads = VETOED_READS.with(std::cell::Cell::get);
    assert_eq!(asked, later[..1]);
    assert_eq!((orderings, reads), (0, 0), "orderings, slice reads");
}
