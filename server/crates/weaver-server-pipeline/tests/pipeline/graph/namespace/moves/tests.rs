// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;

use weaver_graph::holds::{HoldBudgets, HoldsAccountant};

use super::*;
use weaver_server_disk::operations::disk::CapacityReader;

// Names that meet as one path in another case, as a file and a
// directory of one path, and as a directory another name runs through.
const NAMES: [&str; 14] = [
    "a.bin",
    "A.BIN",
    "a (1).bin",
    "d",
    "d/",
    "D/",
    "d/a.bin",
    "D/A.BIN",
    "d/e/",
    "d/e/f.bin",
    "d/e",
    "e/",
    "e/a.bin",
    "f.bin",
];

// A small deterministic generator, so every seed replays one sequence.
struct Draw(u64);

impl Draw {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }

    fn name(&mut self) -> &'static str {
        NAMES[self.below(NAMES.len())]
    }
}

fn writer(dir: &Path) -> NamespaceWriter {
    let budgets = HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    };
    NamespaceWriter::new(
        1,
        dir.to_path_buf(),
        Arc::new(HoldsAccountant::new(budgets)),
        CapacityReader::unknown(),
    )
}

// Over seeded sets of files that sit at and wait for colliding names,
// a pass weighs each move as the scan of every file does, and what it
// keeps after each move it runs is what a fresh read of the files gives.
#[test]
fn a_pass_weighs_each_move_as_a_scan_of_every_file_would() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut weighed = [0usize; 2];
    for seed in [1u64, 7, 42, 1_234_567, 0x9e37_79b9_7f4a_7c15, 31_337] {
        let mut draw = Draw(seed);
        let mut writer = writer(dir.path());
        let mut holders = Vec::new();
        for _ in 0..12 {
            let holder = (NodeId(draw.below(3) as u32), MemberId(draw.below(6) as u32));
            let mut leaf = Leaf::at(dir.path(), draw.name());
            if draw.below(3) > 0 {
                leaf.intend(draw.name());
            }
            writer.files_mut(holder.0).leaves.insert(holder.1, leaf);
            if !holders.contains(&holder) {
                holders.push(holder);
            }
        }
        writer.moves.push(Waiting {
            node: holders[0].0,
            member: holders[0].1,
            stamp: None,
        });
        let mut held = Held::of(&writer);
        for _ in 0..400 {
            let (node, member) = holders[draw.below(holders.len())];
            let leaf = writer.files_mut(node).leaves.get(&member).unwrap();
            let (sits, waits) = (leaf.name().to_owned(), leaf.wants().map(str::to_owned));
            let name = draw.name();
            let free = held.free(name, &sits, waits.as_deref());
            assert_eq!(
                free,
                writer.free_at(name, (node, member)),
                "seed {seed} {name}"
            );
            weighed[usize::from(free)] += 1;
            // Now and then a file that waits moves to the name it wants.
            let moves_now = draw.below(4) == 0;
            if let Some(waits) = waits.filter(|_| moves_now) {
                let leaf = writer.files_mut(node).leaves.get_mut(&member).unwrap();
                let left = leaf.move_on().expect("move").expect("a name it left");
                held.moved(&left);
                let mut leaf = Leaf::at(dir.path(), &waits);
                if draw.below(2) == 0 {
                    leaf.intend(draw.name());
                }
                // A file named again: what the pass keeps is read again.
                if leaf.wants().is_some() {
                    writer.files_mut(node).leaves.insert(member, leaf);
                    held = Held::of(&writer);
                }
            }
            assert_eq!(held, Held::of(&writer), "seed {seed}");
        }
    }
    // Both answers came up many times.
    assert!(weighed.iter().all(|count| *count > 100), "{weighed:?}");
}

// Over seeded sequences of moves asked for, members named again, stamps
// and passes, the moves keep the order a list that drops each member's
// moves by a scan of every move keeps.
#[test]
fn the_moves_keep_the_order_a_scan_of_every_move_would() {
    for seed in [1u64, 7, 42, 1_234_567, 0x9e37_79b9_7f4a_7c15, 31_337] {
        let mut draw = Draw(seed);
        let mut moves = Moves::default();
        let mut scanned: Vec<Waiting> = Vec::new();
        for step in 0..4_000 {
            let (node, member) = (
                NodeId(draw.below(3) as u32),
                MemberId(draw.below(24) as u32),
            );
            match draw.below(8) {
                0..=3 => {
                    let waiting = Waiting {
                        node,
                        member,
                        stamp: (draw.below(2) == 0).then(|| draw.below(9) as u64),
                    };
                    moves.push(waiting);
                    scanned.push(waiting);
                }
                4 | 5 => {
                    moves.drop_member(node, member);
                    scanned.retain(|waiting| (waiting.node, waiting.member) != (node, member));
                }
                6 => {
                    let stamp = draw.below(9) as u64;
                    for waiting in moves.iter_mut() {
                        waiting.stamp.get_or_insert(stamp);
                    }
                    for waiting in &mut scanned {
                        waiting.stamp.get_or_insert(stamp);
                    }
                }
                _ => {
                    // A pass takes them all and keeps some, in order.
                    let keep = draw.below(3);
                    let kept: Vec<Waiting> = moves
                        .take()
                        .into_iter()
                        .filter(|waiting| waiting.member.0 as usize % 3 != keep)
                        .collect();
                    moves = kept.into();
                    scanned.retain(|waiting| waiting.member.0 as usize % 3 != keep);
                }
            }
            let ours: Vec<Waiting> = moves.iter().copied().collect();
            assert_eq!(ours, scanned, "seed {seed} step {step}");
            assert_eq!(moves.len(), scanned.len());
            assert_eq!(moves.is_empty(), scanned.is_empty());
        }
    }
}

// Every member of a long chain named again, twice over, while all of
// them wait: each drop reads that member's own moves, never every move.
#[test]
fn naming_a_chain_again_reads_each_members_moves_alone() {
    const N: u32 = 20_000;
    let mut moves = Moves::default();
    let waiting = |member: u32| Waiting {
        node: NodeId::ROOT,
        member: MemberId(member),
        stamp: None,
    };
    for member in 0..N {
        moves.push(waiting(member));
    }
    for _ in 0..2 {
        for member in 0..N {
            moves.drop_member(NodeId::ROOT, MemberId(member));
            moves.push(waiting(member));
        }
    }
    let order: Vec<u32> = moves.iter().map(|waiting| waiting.member.0).collect();
    assert_eq!(order, (0..N).collect::<Vec<_>>());
    // Each drop reads one place, and closing the gaps reads each place
    // once for the gaps it closes.
    assert!(moves.read <= 8 * N as usize, "{} places read", moves.read);
}

// Over seeded sets of files that come, change, move and go, in
// namespaces that go and come again, the kept count is what a fresh read
// of every file gives, and a new file of a member with a file or without
// one is told what a scan of every file says (`free_for_new` holds
// itself to `free_at`).
#[test]
fn a_new_file_is_told_what_a_scan_of_every_file_would_say() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut answers = [0usize; 2];
    for seed in [1u64, 7, 42, 1_234_567, 0x9e37_79b9_7f4a_7c15, 31_337] {
        let mut draw = Draw(seed);
        let mut writer = writer(dir.path());
        writer.moves.push(Waiting {
            node: NodeId::ROOT,
            member: MemberId(0),
            stamp: None,
        });
        for _ in 0..600 {
            let (node, member) = (NodeId(draw.below(3) as u32), MemberId(draw.below(8) as u32));
            match draw.below(10) {
                0..=3 => {
                    let mut leaf = Leaf::at(dir.path(), draw.name());
                    if draw.below(2) == 0 {
                        leaf.intend(draw.name());
                    }
                    writer.files_mut(node).leaves.insert(member, leaf);
                }
                4 => {
                    writer.files_mut(node).leaves.remove(&member);
                }
                5 => {
                    if let Some(leaf) = writer.files_mut(node).leaves.get_mut(&member) {
                        leaf.intend(draw.name());
                    }
                }
                6 => {
                    if let Some(leaf) = writer.files_mut(node).leaves.get_mut(&member) {
                        leaf.move_on().expect("move");
                    }
                }
                7 => {
                    // The namespace goes, and may come again empty.
                    writer.files.remove(&node);
                    if draw.below(2) == 0 {
                        writer.files_mut(node);
                    }
                }
                _ => {}
            }
            let name = draw.name();
            answers[usize::from(writer.free_for_new(name, (node, member)))] += 1;
            let fresh = Held::of(&writer);
            assert_eq!(*writer.kept.read(&writer.files), fresh, "seed {seed}");
        }
    }
    // Both answers came up many times.
    assert!(answers.iter().all(|count| *count > 100), "{answers:?}");
}

// A move waits while a long run of new files opens, one after another:
// the kept count reads each file once as it comes, never every file for
// each new one.
#[test]
fn new_files_read_each_file_once_while_a_move_waits() {
    const N: u32 = 600;
    let dir = tempfile::tempdir().expect("temp dir");
    let mut writer = writer(dir.path());
    for member in 0..N {
        let leaf = Leaf::at(dir.path(), &format!("f{member}.bin"));
        writer
            .files_mut(NodeId::ROOT)
            .leaves
            .insert(MemberId(member), leaf);
    }
    writer.moves.push(Waiting {
        node: NodeId::ROOT,
        member: MemberId(0),
        stamp: None,
    });
    let before = TALLY.with(std::cell::Cell::get);
    for member in N..2 * N {
        let at = (NodeId::ROOT, MemberId(member));
        let leaf = writer.new_leaf(at, &format!("g{member}.bin"), false, dir.path());
        assert_eq!(leaf.name(), format!("g{member}.bin"));
        writer
            .files_mut(NodeId::ROOT)
            .leaves
            .insert(MemberId(member), leaf);
    }
    let kept = TALLY.with(std::cell::Cell::get).kept - before.kept;
    assert_eq!(kept, u64::from(2 * N - 1));
}
