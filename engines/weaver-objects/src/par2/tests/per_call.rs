// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the object builds again on every call, and what it no longer does.

use std::sync::atomic::Ordering;

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{Provenance, Recovery, SliceVerdict};

use weaver_graph::{ByteRange, Object, Provenance as Fresh, VolumeIndex};

use super::{ARTICLE, BLOCK, OWN, bind, damaged_member, feed, job, verdicts, wanted};
use crate::par2::evidence::VETOED_READS;
use crate::par2::{Par2, REFUSED_WALKED};
use crate::testing::{Job, par2_set, payload};

// `count` files of 1500 bytes each, named in order.
fn parts(count: u64, seed: u64) -> Vec<(String, Vec<u8>)> {
    (0..count)
        .map(|at| (format!("part{at:03}.bin"), payload(seed + at, 1500)))
        .collect()
}

fn described(parts: &[(String, Vec<u8>)]) -> Vec<(&str, &[u8])> {
    parts
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect()
}

// The index's packets but its slice checksums, then each file's slice
// checksums, in the order they lie.
fn checksums_apart(index: &[u8]) -> (Vec<u8>, Vec<Vec<u8>>) {
    let (mut head, mut checksums) = (Vec::new(), Vec::new());
    let mut at = 0;
    while at < index.len() {
        let len = u64::from_le_bytes(index[at + 8..at + 16].try_into().unwrap()) as usize;
        let packet = &index[at..at + len];
        if packet[48..64].starts_with(b"PAR 2.0\0IFSC") {
            checksums.push(packet.to_vec());
        } else {
            head.extend_from_slice(packet);
        }
        at += len;
    }
    (head, checksums)
}

fn deliver(job: &mut Job<Par2>, volume: VolumeIndex, offset: usize, bytes: &[u8]) {
    job.object.deliver(
        weaver_graph::Delivery {
            volume,
            offset: offset as u64,
            bytes: &weaver_graph::Evidenced::from(bytes.to_vec()),
            provenance: Fresh::Fresh,
        },
        &mut |_, _| false,
        &mut Vec::new(),
    );
}

// What binding and folding have cost: whole passes, sets bound and members
// folded.
fn spent(job: &Job<Par2>) -> (usize, usize, usize) {
    (
        job.object.claims.whole_passes,
        job.object.set_binds,
        job.object.members_folded,
    )
}

fn since(before: (usize, usize, usize), after: (usize, usize, usize)) -> (usize, usize, usize) {
    (after.0 - before.0, after.1 - before.1, after.2 - before.2)
}

// Forty members bound by a set whose descriptions came first, each file's
// slice checksums after in a delivery of its own, a bind after each: each
// folds the member of its file alone and binds nothing again, since what a
// binding reads of the set did not change.
#[test]
fn slice_checksums_one_at_a_time_fold_their_member_alone() {
    let parts = parts(40, 200);
    let files = par2_set("set", &described(&parts), BLOCK, 2, 1);
    let (head, checksums) = checksums_apart(&files[0].1);
    assert_eq!(checksums.len(), parts.len());
    let mut index = head.clone();
    checksums
        .iter()
        .for_each(|packet| index.extend_from_slice(packet));
    let mut job = Job::new(Par2::new(OWN), OWN);
    let members: Vec<_> = parts
        .iter()
        .map(|(name, bytes)| job.member(name, bytes))
        .collect();
    let volume = job.name(&files[0].0, &index);
    deliver(&mut job, volume, 0, &head);
    bind(&mut job);
    assert!(members.iter().all(|member| job.object.covers(*member)));

    let before = spent(&job);
    let entries = job.object.entries();
    let mut at = head.len();
    for packet in &checksums {
        deliver(&mut job, volume, at, packet);
        at += packet.len();
        bind(&mut job);
    }
    assert_eq!(since(before, spent(&job)), (0, 0, 40));
    // No set weighs its members again.
    assert_eq!(job.object.entries() - entries, 0);
    assert!(members.iter().all(|member| job.object.covers(*member)));
}

// Eight sets each holding an unnamed member at a free file of its length,
// then twenty unnamed members fitting no set, a bind after each: none binds
// a set again. One more of the length of one set's free file binds that set
// alone, which comes to hold it beside the first.
#[test]
fn an_unnamed_change_binds_only_the_sets_that_could_hold_it() {
    const SETS: u64 = 8;
    const CHANGES: u64 = 20;
    let sets: Vec<[(String, Vec<u8>); 2]> = (0..SETS)
        .map(|set| {
            [
                (format!("named{set}.bin"), payload(500 + set, 1500)),
                (
                    format!("spare{set}.bin"),
                    payload(600 + set, 1600 + 10 * set as usize),
                ),
            ]
        })
        .collect();
    let mut files = Vec::new();
    for (set, described) in sets.iter().enumerate() {
        files.extend(par2_set(
            &format!("set{set}"),
            &[
                (described[0].0.as_str(), &described[0].1),
                (described[1].0.as_str(), &described[1].1),
            ],
            BLOCK,
            1,
            1,
        ));
    }
    let mut job = job(&files);
    for (set, [named, spare]) in sets.iter().enumerate() {
        job.member(&named.0, &named.1);
        job.member(
            &format!("junk{set}.dat"),
            &payload(900 + set as u64, spare.1.len()),
        );
    }
    bind(&mut job);
    assert_eq!(job.object.held_by.len(), SETS as usize);

    let before = spent(&job);
    let entries = job.object.entries();
    for change in 0..CHANGES {
        job.member(&format!("noise{change}.dat"), &payload(700 + change, 999));
        bind(&mut job);
    }
    let (wholes, binds, _) = since(before, spent(&job));
    assert_eq!(job.object.held_by.len(), SETS as usize);
    // Each bind lists the length of the one member that changed.
    assert_eq!(
        (wholes, binds, job.object.entries() - entries),
        (0, 0, CHANGES as usize),
        "whole passes, set binds, entries"
    );

    let before = spent(&job);
    let twin = job.member("twin.dat", &payload(800, 1600 + 10 * 3));
    bind(&mut job);
    assert_eq!(since(before, spent(&job)).1, 1, "set binds");
    let key = job.object.order[3];
    let held: Vec<_> = crate::par2::holders_of(&job.object.holders, twin).collect();
    assert_eq!(held, [key]);
}

// Forty members bound, one of them arriving: when it comes to its length,
// the bind folds that member alone.
#[test]
fn a_member_coming_to_its_length_folds_alone() {
    let parts = parts(40, 300);
    let mut job = job(&par2_set("set", &described(&parts), BLOCK, 2, 1));
    let (last, rest) = parts.split_last().unwrap();
    for (name, bytes) in rest {
        job.member(name, bytes);
    }
    let member = job.namespace.announce(&last.0, Some(last.1.len() as u64));
    job.arrive(member, 0, &last.1[..ARTICLE as usize], Fresh::Fresh);
    bind(&mut job);

    let before = spent(&job);
    job.arrive(member, ARTICLE, &last.1[ARTICLE as usize..], Fresh::Fresh);
    bind(&mut job);
    assert_eq!(since(before, spent(&job)).2, 1);
    assert!(job.object.covers(member));
}

// Forty members bound, one article of one refused: what was refused of the
// set is read from that member's file alone.
#[test]
fn refused_slices_walk_the_refused_member_alone() {
    let parts = parts(40, 400);
    let mut job = job(&par2_set("set", &described(&parts), BLOCK, 2, 1));
    let members: Vec<_> = parts
        .iter()
        .map(|(name, bytes)| job.member(name, bytes))
        .collect();
    bind(&mut job);
    Recovery::refused(&mut job.object, members[7], ByteRange::new(0, ARTICLE));
    let key = job.object.order[0];
    REFUSED_WALKED.with(|walked| walked.set(0));
    let slices = job.object.refused_slices(&key);
    assert_eq!((slices, REFUSED_WALKED.with(std::cell::Cell::get)), (2, 1));
}

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

// A member is cut at the slices of the set that binds it, and a member no
// set binds at every set's, each size once, after what the caller holds.
#[test]
fn block_sizes_names_the_sizes_of_the_members_own_set() {
    let one = payload(34, 5000);
    let two = payload(35, 6000);
    let three = payload(36, 7000);
    let mut files = par2_set("first", &[("one.bin", &one)], 1024, 2, 1);
    files.extend(par2_set("second", &[("two.bin", &two)], BLOCK, 2, 1));
    files.extend(par2_set("third", &[("three.bin", &three)], 1024, 2, 1));
    let mut job = job(&files);
    let first = job.member("one.bin", &one);
    let second = job.member("two.bin", &two);
    job.member("three.bin", &three);
    let other = job.member("other.bin", &payload(37, 3000));
    bind(&mut job);
    assert_eq!(job.object.slice_sizes().count(), 3);
    let sizes = |member| {
        let mut sizes = vec![7];
        Recovery::block_sizes(&job.object, member, &mut sizes);
        sizes
    };
    assert_eq!(sizes(first), [7, 1024]);
    assert_eq!(sizes(second), [7, BLOCK]);
    assert_eq!(sizes(other), [7, BLOCK, 1024]);
}

// A member two sets describe is cut for both: the set that binds it, and
// the set it moves to if that one cannot rebuild it.
#[test]
fn block_sizes_names_every_set_that_describes_the_member() {
    let shared = payload(38, 5000);
    let alone = payload(39, 5000);
    let mut files = par2_set("first", &[("shared.bin", &shared)], BLOCK, 1, 1);
    files.extend(par2_set("second", &[("shared.bin", &shared)], 1024, 4, 1));
    files.extend(par2_set("third", &[("alone.bin", &alone)], 2048, 2, 1));
    let mut job = job(&files);
    let member = job.member("shared.bin", &shared);
    let single = job.member("alone.bin", &alone);
    bind(&mut job);
    let sizes = |member| {
        let mut sizes = Vec::new();
        Recovery::block_sizes(&job.object, member, &mut sizes);
        sizes
    };
    assert_eq!(sizes(member), [BLOCK, 1024]);
    assert_eq!(sizes(single), [2048]);
}

// Each packet of an index, in the order they lie.
fn packets(index: &[u8]) -> Vec<&[u8]> {
    let mut packets = Vec::new();
    let mut at = 0;
    while at < index.len() {
        let len = u64::from_le_bytes(index[at + 8..at + 16].try_into().unwrap()) as usize;
        packets.push(&index[at..at + len]);
        at += len;
    }
    packets
}

// A set's packets taken a few at a time over many takes, then its facts
// taken once: the packets are encoded once, for that walk.
#[test]
fn packets_taken_over_many_takes_are_encoded_once_for_the_walk() {
    let parts = parts(30, 500);
    let files = par2_set("set", &described(&parts), BLOCK, 2, 1);
    let index = &files[0].1;
    let packets = packets(index);
    let mut job = Job::new(Par2::new(OWN), OWN);
    let volume = job.name(&files[0].0, index);
    let start = weaver_graph::sealed_encodes();
    let mut at = 0;
    let mut takes = 0;
    for group in packets.chunks(3) {
        let bytes: Vec<u8> = group.concat();
        deliver(&mut job, volume, at, &bytes);
        at += bytes.len();
        takes += 1;
    }
    assert!(takes > 10, "{takes} takes");
    assert_eq!(job.object.sets.len(), 1);
    assert_eq!(weaver_graph::sealed_encodes() - start, 0, "no take encodes");
    let facts = job.object.facts();
    assert_eq!(
        weaver_graph::sealed_encodes() - start,
        1,
        "the walk encodes once"
    );
    let again = job.object.facts();
    assert_eq!(
        weaver_graph::sealed_encodes() - start,
        1,
        "a second walk encodes nothing"
    );
    assert_eq!(facts, again);
    let state = job.object.sets.values().next().unwrap();
    assert_eq!(
        state.packets().bytes().map(<[u8]>::to_vec),
        rmp_serde::to_vec(state.packets().get()).ok(),
        "the bytes are the packets' as they stand"
    );
}

// Thirty members waiting on a set whose descriptions come one take at a
// time after its other packets, a bind after each: until the last one makes
// the set usable no pass binds it, since it holds nothing to bind again.
// The last binds it and it covers every member.
#[test]
fn descriptions_before_a_set_is_usable_bind_it_in_no_pass() {
    const FILE_DESC: &[u8] = b"PAR 2.0\0FileDesc";
    let parts = parts(30, 1100);
    let files = par2_set("set", &described(&parts), BLOCK, 2, 1);
    let (mut head, mut descriptions) = (Vec::new(), Vec::new());
    for packet in packets(&files[0].1) {
        if &packet[48..64] == FILE_DESC {
            descriptions.push(packet);
        } else {
            head.extend_from_slice(packet);
        }
    }
    assert_eq!(descriptions.len(), parts.len());
    let mut index = head.clone();
    descriptions
        .iter()
        .for_each(|packet| index.extend_from_slice(packet));
    let mut job = Job::new(Par2::new(OWN), OWN);
    let members: Vec<_> = parts
        .iter()
        .map(|(name, bytes)| job.member(name, bytes))
        .collect();
    let volume = job.name(&files[0].0, &index);
    deliver(&mut job, volume, 0, &head);
    bind(&mut job);

    let before = spent(&job);
    let (last, rest) = descriptions.split_last().unwrap();
    let mut at = head.len();
    for packet in rest {
        deliver(&mut job, volume, at, packet);
        at += packet.len();
        bind(&mut job);
        assert!(
            job.object
                .sets
                .values()
                .all(|state| state.usable().is_none())
        );
    }
    assert_eq!(since(before, spent(&job)).1, 0, "set binds");
    assert!(members.iter().all(|member| !job.object.covers(*member)));

    deliver(&mut job, volume, at, last);
    bind(&mut job);
    assert_eq!(since(before, spent(&job)).1, 1, "set binds");
    assert!(members.iter().all(|member| job.object.covers(*member)));
}

// A description packet of `set` for a file of `length` its main packet does
// not list, its hashes `hash`.
fn stray_description(set: &[u8], name: &[u8], length: u64, hash: [u8; 16]) -> Vec<u8> {
    let mut id = Vec::new();
    id.extend_from_slice(&hash);
    id.extend_from_slice(&length.to_le_bytes());
    id.extend_from_slice(name);
    let mut body = par2_rs::checksum::md5(&id).to_vec();
    body.extend_from_slice(&hash);
    body.extend_from_slice(&hash);
    body.extend_from_slice(&length.to_le_bytes());
    body.extend_from_slice(name);
    body.resize(body.len().next_multiple_of(4), 0);
    let mut packet = Vec::new();
    packet.extend_from_slice(b"PAR2\0PKT");
    packet.extend_from_slice(&(64 + body.len() as u64).to_le_bytes());
    packet.extend_from_slice(&[0; 16]);
    packet.extend_from_slice(set);
    packet.extend_from_slice(b"PAR 2.0\0FileDesc");
    packet.extend_from_slice(&body);
    let md5 = par2_rs::checksum::md5(&packet[32..]);
    packet[16..32].copy_from_slice(&md5);
    packet
}

// A usable set binding its members, then a description that takes what it
// describes past the output limit: the set is usable no longer, and since
// it held members the next pass binds it again.
#[test]
fn a_set_that_stops_being_usable_binds_again_on_the_next_pass() {
    let parts = parts(4, 1200);
    let files = par2_set("set", &described(&parts), BLOCK, 2, 1);
    let head = files[0].1.clone();
    let set = &packets(&head)[0][32..48];
    let stray = stray_description(set, b"stray.bin", 5000, [9; 16]);
    let mut index = head.clone();
    index.extend_from_slice(&stray);
    let mut object = Par2::new(OWN);
    object.output_limit = 10_000;
    let mut job = Job::new(object, OWN);
    let members: Vec<_> = parts
        .iter()
        .map(|(name, bytes)| job.member(name, bytes))
        .collect();
    let volume = job.name(&files[0].0, &index);
    deliver(&mut job, volume, 0, &head);
    bind(&mut job);
    assert!(members.iter().all(|member| job.object.covers(*member)));

    let before = spent(&job);
    deliver(&mut job, volume, head.len(), &stray);
    bind(&mut job);
    assert!(
        job.object
            .sets
            .values()
            .all(|state| state.usable().is_none())
    );
    assert_eq!(since(before, spent(&job)).1, 1, "set binds");
    // No name binds them now: the set keeps each as an unnamed member it
    // already bound.
    assert!(job.object.claims.bindings().is_empty());
    assert!(members.iter().all(|member| job.object.covers(*member)));
}
