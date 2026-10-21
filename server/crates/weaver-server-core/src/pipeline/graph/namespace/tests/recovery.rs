// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The namespace's rules for its recovery sets: which members a set covers,
// when it rules, and what the namespace holds for an index still to come.

use super::*;

const VIDEO_FIX: Fix = Fix {
    name: "video.mkv",
    read: ByteRange { start: 0, end: 8 },
    fix: (2, b"OK"),
};

impl Job {
    // Ask the namespace to rule the root's sets, its source `reading` or
    // done.
    fn rule(&mut self, reading: bool) {
        let mut build = self.build();
        let mut lent = Inline::default();
        self.writer
            .rule(
                &mut self.tree,
                &mut build,
                &Source,
                (NodeId::ROOT, &|_| reading),
                &mut lent,
                &mut self.out,
            )
            .expect("rule");
        self.run_lent(&mut build, lent);
    }

    fn set_node(&self) -> NodeId {
        self.writer.sets(&self.tree)[0].1
    }
}

// A damaged member the set fixes, all its bytes in but not closed.
fn damaged_member(job: &mut Job) -> MemberId {
    let member = job.member("video.mkv", Some(8));
    job.send(member, 0, b"go", Provenance::Fresh);
    job.send(member, 2, b"XX", Provenance::Damaged);
    job.send(member, 4, b"good", Provenance::Fresh);
    member
}

#[test]
fn a_set_lives_in_the_namespace_of_the_members_it_describes() {
    let mut job = Job::new(PLENTY).with_set(None);
    let video = job.member("video.mkv", Some(4));
    let other = job.member("other.mkv", Some(4));
    let set = job.set_node();
    assert_eq!(job.writer.sets(&job.tree), vec![(NodeId::ROOT, set)]);
    let sets = job.tree.recovery_nodes(NodeId::ROOT);
    assert_eq!(
        job.writer
            .covering(&job.tree, &sets, video)
            .collect::<Vec<_>>(),
        vec![set]
    );
    assert_eq!(
        job.writer.covering(&job.tree, &sets, other).count(),
        0,
        "a member the set does not describe has no set"
    );
    assert_eq!(job.writer.set_name(&job.tree, set), Some("set.par2"));
}

#[test]
fn a_sets_own_volume_carries_recovery_and_a_member_it_covers_does_not() {
    let mut job = Job::new(PLENTY).with_set(None);
    let own = job.root().find_by_name("set.par2").expect("own volume").id;
    let video = job.member("video.mkv", Some(4));
    assert!(job.writer.carries_recovery(&job.tree, NodeId::ROOT, own));
    assert!(!job.writer.carries_recovery(&job.tree, NodeId::ROOT, video));
}

#[test]
fn a_set_does_not_rule_while_a_member_it_covers_is_open() {
    let mut job = Job::new(PLENTY).with_set(Some(VIDEO_FIX));
    let member = damaged_member(&mut job);
    assert!(!job.root().get(member).unwrap().closed);
    job.rule(false);
    assert!(job.par.lock().unwrap().read.is_none(), "no repair ran");
    assert_eq!(job.file("video.mkv"), b"goXXgood");
}

#[test]
fn a_set_does_not_rule_while_its_namespace_may_still_announce_members() {
    let mut job = Job::new(PLENTY).with_set(Some(VIDEO_FIX));
    let member = damaged_member(&mut job);
    job.close(member);
    job.rule(true);
    assert!(job.par.lock().unwrap().read.is_none(), "no repair ran");
    assert_eq!(job.file("video.mkv"), b"goXXgood");
}

#[test]
fn a_set_rules_once_every_member_it_covers_is_closed_and_known() {
    let mut job = Job::new(PLENTY).with_set(Some(VIDEO_FIX));
    let member = damaged_member(&mut job);
    job.close(member);
    job.rule(false);
    assert_eq!(
        job.par.lock().unwrap().read.as_deref(),
        Some(&b"goXXgood"[..])
    );
    assert_eq!(job.file("video.mkv"), b"goOKgood");
}

// Beside the set only a member it does not describe: the index covers
// nothing, and its own volume is still open.
fn an_index_covering_nothing(job: &mut Job) -> MemberId {
    let other = job.member("other.mkv", Some(4));
    job.send(other, 0, b"body", Provenance::Fresh);
    job.close(other);
    assert_eq!(
        job.writer
            .covering(&job.tree, &job.tree.recovery_nodes(NodeId::ROOT), other)
            .count(),
        0
    );
    job.root().find_by_name("set.par2").expect("own volume").id
}

#[test]
fn an_index_that_covers_nothing_and_has_no_volume_in_is_not_due() {
    let mut job = Job::new(PLENTY).with_set(None);
    let own = an_index_covering_nothing(&mut job);
    assert!(!job.root().get(own).unwrap().closed);
    job.rule(false);
    assert!(
        job.par.lock().unwrap().volume.is_none(),
        "no repair was lent"
    );
    assert_eq!(job.writer.outcomes(&job.tree), vec![None]);
}

#[test]
fn an_index_that_covers_nothing_is_due_once_its_volume_closes() {
    let mut job = Job::new(PLENTY).with_set(None);
    let own = an_index_covering_nothing(&mut job);
    job.close(own);
    job.rule(false);
    assert_eq!(
        job.par.lock().unwrap().volume.as_deref(),
        Some(PAR2_SIGNATURE),
        "the repair was lent and read the set's own volume"
    );
}

#[test]
fn a_namespace_without_sets_has_ruled_and_one_with_an_unruled_set_has_not() {
    let mut job = Job::new(PLENTY);
    job.member("video.mkv", Some(4));
    assert!(job.writer.ruled(&job.tree, NodeId::ROOT));
    assert!(job.writer.all_ruled(&job.tree));

    let mut job = Job::new(PLENTY).with_set(None);
    job.member("video.mkv", Some(4));
    assert!(!job.writer.ruled(&job.tree, NodeId::ROOT));
    assert_eq!(job.writer.outcomes(&job.tree), vec![None]);
}

#[test]
fn the_namespace_holds_only_for_declared_indexes_still_to_come() {
    let job = Job::new(PLENTY);
    let arrived = MemberId(1);
    let held = job
        .writer
        .holding_for([MemberId(0), arrived, MemberId(2)], |member| {
            member != arrived
        });
    assert_eq!(held, vec![MemberId(0), MemberId(2)]);
    assert!(
        job.writer.holding_for([arrived], |_| false).is_empty(),
        "nothing is held once every index is in"
    );
}
