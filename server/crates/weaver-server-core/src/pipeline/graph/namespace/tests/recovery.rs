// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The namespace's rules for its recovery sets: which members a set covers,
// when it rules, and what the namespace holds for an index still to come.

use std::sync::atomic::{AtomicBool, Ordering};

use super::super::super::hardware::Shutdown;
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

// What a repair rebuilds waits on disk while the loan is out, past any
// memory budget the job has, and lands byte for byte when it returns; the
// output file goes with it.
#[test]
fn a_repairs_output_waits_on_disk_past_the_memory_budget_and_lands_whole() {
    const REBUILT: &[u8] = b"OKOKOK";
    let mut job = Job::new(HoldBudgets {
        resident_limit: 2,
        ..PLENTY
    })
    .with_set(Some(Fix {
        name: "video.mkv",
        read: ByteRange { start: 0, end: 8 },
        fix: (1, REBUILT),
    }));
    let member = damaged_member(&mut job);
    job.close(member);
    let set = job.set_node();
    let mut lent = Inline::default();
    job.writer
        .repair(&mut job.tree, NodeId::ROOT, &[set], &mut lent)
        .expect("lent");
    let Ok([loan]) = <[Loan; 1]>::try_from(lent.0) else {
        panic!("one loan");
    };
    let returned = loan.run(weaver_graph::Budget::default(), &Default::default());
    assert!(REBUILT.len() as u64 > job.accountant.budgets().resident_limit);
    let output = job.dir.path().join(format!(
        "{}/graph-recovery-{}-{}.repaired",
        weaver_graph::names::INTERNAL_DIR,
        NodeId::ROOT.0,
        set.0
    ));
    assert_eq!(
        std::fs::read(&output).expect("the repair's output on disk"),
        REBUILT
    );
    assert_eq!(job.accountant.usage().resident, 0);
    let mut build = job.build();
    job.writer
        .returned(
            &mut job.tree,
            &mut build,
            &Source,
            &weaver_objects::Objects::default(),
            returned,
            &mut job.out,
        )
        .expect("returned");
    assert_eq!(job.file("video.mkv"), b"gOKOKOKd");
    assert!(!output.exists(), "the output goes with the repair");
}

// A set that rebuilds a store's map lets the store place the body it held
// behind it, and the set's proof of that body reaches the member: the held
// bytes go down before the proof is read back through the map, so what came
// marked damaged is marked repaired in the namespace below.
#[test]
fn a_rebuilt_map_lets_the_sets_proof_reach_the_bytes_it_held() {
    let mut job = Job::new(PLENTY).with_set(Some(Fix {
        name: "a.7z",
        read: body(),
        fix: (map_at().start, MAP),
    }));
    let member = job.member("a.7z", Some(map_at().end));
    job.send(member, 0, &seven_z(), Provenance::Fresh);
    job.send(member, body().start, b"hello", Provenance::Damaged);
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), body().len());
    job.close(member);
    let set = job.set_node();
    let mut lent = Inline::default();
    job.writer
        .repair(&mut job.tree, NodeId::ROOT, &[set], &mut lent)
        .expect("lent");
    let mut build = job.build();
    let ruled = job.run_lent(&mut build, lent);
    assert_eq!(ruled, vec![(set, RepairOutcome::Repaired { slices: 1 })]);

    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 0);
    assert_eq!(job.file("inner.bin"), b"hello");
    let (late, _) = job.tree.child_of(NodeId::ROOT, member).expect("a 7z");
    let below = job.tree.namespace(late).expect("its namespace");
    assert_eq!(
        below.damaged_ranges().collect::<Vec<_>>(),
        Vec::new(),
        "the set proved the body it held"
    );
}

// An output that refuses the repair's write fails the repair with that
// refusal, and nothing it rebuilt reaches the member.
#[test]
fn a_repair_whose_output_refuses_a_write_fails_and_applies_nothing() {
    let mut job = Job::new(PLENTY).with_set(Some(Fix {
        name: "video.mkv",
        read: ByteRange { start: 0, end: 8 },
        fix: (1, b"OKOKOK"),
    }));
    let member = damaged_member(&mut job);
    job.close(member);
    let set = job.set_node();
    let before = job.file("video.mkv");
    let heard = job.out.len();
    let mut lent = Inline::default();
    job.writer
        .repair(&mut job.tree, NodeId::ROOT, &[set], &mut lent)
        .expect("lent");
    let Ok([loan]) = <[Loan; 1]>::try_from(lent.0) else {
        panic!("one loan");
    };
    // A directory where the output file goes: the first write cannot make it.
    let output = job.dir.path().join(format!(
        "{}/graph-recovery-{}-{}.repaired",
        weaver_graph::names::INTERNAL_DIR,
        NodeId::ROOT.0,
        set.0
    ));
    std::fs::create_dir_all(&output).unwrap();
    let returned = loan.run(weaver_graph::Budget::default(), &Default::default());
    let mut build = job.build();
    let error = job
        .writer
        .returned(
            &mut job.tree,
            &mut build,
            &Source,
            &weaver_objects::Objects::default(),
            returned,
            &mut job.out,
        )
        .expect_err("the repair failed");
    let WriteError::Leaf(LeafError::Io { path, .. }) = &error else {
        panic!("the output's refusal: {error:?}");
    };
    assert_eq!(path, &output);
    assert_eq!(job.file("video.mkv"), before);
    assert_eq!(job.out.len(), heard);
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
fn the_coordinator_holds_only_for_declared_indexes_still_to_come() {
    use crate::pipeline::graph::metadata::{MetadataState, RecoveryFormat, RecoveryMetadata};
    let arrived = MemberId(1);
    let mut metadata = RecoveryMetadata::default();
    for member in [MemberId(0), arrived, MemberId(2)] {
        metadata.observe(
            member,
            RecoveryFormat::Par2,
            if member == arrived {
                MetadataState::Ready
            } else {
                MetadataState::Unresolved
            },
        );
    }
    assert_eq!(
        metadata.bootstrap_files(),
        Some(vec![MemberId(0), MemberId(2)])
    );
    let mut settled = RecoveryMetadata::default();
    settled.observe(arrived, RecoveryFormat::Par2, MetadataState::Ready);
    assert_eq!(
        settled.bootstrap_files(),
        None,
        "nothing is held once every index is in"
    );
}

// The set's one loan, lent out of the job.
fn lend_one(job: &mut Job, set: NodeId) -> Loan {
    let mut lent = Inline::default();
    job.writer
        .repair(&mut job.tree, NodeId::ROOT, &[set], &mut lent)
        .expect("lent");
    let Ok([loan]) = <[Loan; 1]>::try_from(lent.0) else {
        panic!("one loan");
    };
    loan
}

// A repair the shutdown reaches while it is running reads nothing more:
// the read after it is refused, and the repair comes back stopped with
// nothing it came to applied.
#[test]
fn a_shutdown_that_starts_mid_repair_refuses_the_next_read_and_applies_nothing() {
    let mut job = Job::new(PLENTY).with_set(Some(VIDEO_FIX));
    let member = damaged_member(&mut job);
    job.close(member);
    let set = job.set_node();
    let loan = lend_one(&mut job, set);
    let started = Arc::new(AtomicBool::new(false));
    let flip = Arc::clone(&started);
    job.par.lock().unwrap().between = Some(Box::new(move || flip.store(true, Ordering::Release)));
    let returned = loan.run(weaver_graph::Budget::default(), &Shutdown::of(started));
    let seen = job.par.lock().unwrap();
    assert!(seen.volume.is_some(), "the repair ran before the shutdown");
    assert!(
        seen.read.is_none(),
        "the read after the shutdown is refused"
    );
    drop(seen);
    assert!(returned.stopped());
    let mut build = job.build();
    job.writer
        .returned(
            &mut job.tree,
            &mut build,
            &Source,
            &weaver_objects::Objects::default(),
            returned,
            &mut job.out,
        )
        .expect("returned");
    assert!(!job.tree.lent(set));
    assert_eq!(job.file("video.mkv"), b"goXXgood");
}

// A job failed while its repair is out has its working directory released
// under the repair: the repair reads and writes nothing more, and nothing
// it does makes the directory again.
#[test]
fn a_repair_out_when_its_job_fails_never_makes_the_working_directory_again() {
    let mut job = Job::new(PLENTY).with_set(Some(VIDEO_FIX));
    let member = damaged_member(&mut job);
    job.close(member);
    let set = job.set_node();
    let loan = lend_one(&mut job, set);
    let working_dir = job.dir.path().to_path_buf();
    let released = working_dir.clone();
    job.par.lock().unwrap().between = Some(Box::new(move || {
        std::fs::remove_dir_all(&released).expect("released");
    }));
    let returned = loan.run(weaver_graph::Budget::default(), &Shutdown::default());
    assert!(
        job.par.lock().unwrap().read.is_none(),
        "the read after the release is refused"
    );
    assert!(returned.stopped());
    assert!(!working_dir.exists(), "the working directory stays gone");
}

// A repair that goes out as the pipeline shuts down reads nothing and
// applies nothing: the set takes its object back unruled and repairs on its
// next ruling, as the restored job rules it.
#[test]
fn a_repair_stopped_by_shutdown_reads_nothing_applies_nothing_and_rules_again() {
    let mut job = Job::new(PLENTY).with_set(Some(VIDEO_FIX));
    let member = damaged_member(&mut job);
    job.close(member);
    let set = job.set_node();
    let loan = lend_one(&mut job, set);
    let shutdown = Shutdown::of(Arc::new(AtomicBool::new(true)));
    let returned = loan.run(weaver_graph::Budget::default(), &shutdown);
    assert!(
        job.par.lock().unwrap().read.is_none(),
        "the stopped repair read nothing"
    );
    assert!(returned.stopped());
    let mut build = job.build();
    job.writer
        .returned(
            &mut job.tree,
            &mut build,
            &Source,
            &weaver_objects::Objects::default(),
            returned,
            &mut job.out,
        )
        .expect("returned");
    assert!(!job.tree.lent(set));
    assert_eq!(job.file("video.mkv"), b"goXXgood");

    job.rule(false);
    assert_eq!(job.file("video.mkv"), b"goOKgood");
}
