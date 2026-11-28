// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Whether what the object took since its last bind leaves a bind something
// to do: a set filed or whose index changed, or a member a set came to
// describe. Packets the object already holds leave nothing.

use super::*;

// Bind as the namespace does: the bind, then what the sets came to bind or
// describe said again, which takes `unfed`.
fn bind_and_feed(job: &mut Job<Par2>) {
    bind(job);
    let mut unfed = Vec::new();
    job.object.unfed(&mut unfed);
}

#[test]
fn a_fresh_object_has_no_bind_due() {
    let object = Par2::new(OWN);
    assert!(!object.bind_due());
}

#[test]
fn a_take_that_files_a_set_leaves_a_bind_due_until_it_binds() {
    let data = payload(61, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1);
    let mut job = job(&files[..1]);
    assert!(!job.object.sets.is_empty());
    assert!(job.object.bind_due(), "the set filed is to be bound");
    job.member("payload.bin", &data);
    bind_and_feed(&mut job);
    assert!(!job.object.bind_due());
    bind_and_feed(&mut job);
    assert!(
        !job.object.bind_due(),
        "a bind with nothing new leaves none"
    );
}

#[test]
fn a_take_that_changes_a_sets_index_leaves_a_bind_due() {
    let (mut job, volume, _) = undescribed(62, false);
    let mut unfed = Vec::new();
    job.object.unfed(&mut unfed);
    assert!(!job.object.bind_due());
    let carrier = job.store.volumes[&volume].clone();
    deliver_articles(&mut job.object, volume, &carrier, None);
    assert!(
        job.object
            .sets
            .values()
            .all(|state| state.usable().is_some()),
        "the volume carried the descriptions"
    );
    assert!(job.object.bind_due());
    bind_and_feed(&mut job);
    assert!(!job.object.bind_due());
}

#[test]
fn a_set_that_comes_to_describe_a_bound_member_leaves_it_to_be_said_again() {
    let shared = payload(63, 5000);
    let first = par2_set("first", &[("shared.bin", &shared)], BLOCK, 1, 1);
    let second = par2_set("second", &[("shared.bin", &shared)], 1024, 4, 1);
    let mut job = job(&first);
    job.member("shared.bin", &shared);
    bind_and_feed(&mut job);
    assert!(!job.object.bind_due());
    for (name, bytes) in &second {
        job.volume(name, bytes, ARTICLE as usize, false);
    }
    assert!(job.object.moved(), "the take filled `unfed`");
    assert!(job.object.bind_due());
    // The bind takes the sets to bind; what is to be said again stays
    // until it is said.
    bind(&mut job);
    assert!(job.object.rebound.is_empty());
    assert!(job.object.moved());
    assert!(job.object.bind_due());
    let mut unfed = Vec::new();
    job.object.unfed(&mut unfed);
    assert!(!job.object.bind_due());
}

#[test]
fn packets_the_object_holds_already_leave_no_bind_due() {
    let data = payload(64, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 2);
    let mut job = job(&files[..2]);
    job.member("payload.bin", &data);
    bind_and_feed(&mut job);
    assert!(!job.object.bind_due());
    // The second volume repeats the set's main and description packets and
    // carries recovery slices of the set the object knows.
    let slices = |job: &Job<Par2>| -> usize {
        job.object
            .sets
            .values()
            .map(|state| state.recovery.len())
            .sum()
    };
    let before = slices(&job);
    job.volume(&files[2].0, &files[2].1, ARTICLE as usize, false);
    assert!(slices(&job) > before, "its slices were taken");
    assert!(!job.object.bind_due());
}
