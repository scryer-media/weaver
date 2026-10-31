// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where a member's file sits, and when it moves. A member the ledger names
// again keeps its file where it is: the name it wants is an intent, which
// the next checkpoint records beside the name the file sits at. Only once
// that checkpoint has committed does the file move, as the continuation of
// the commit, so a crash at any moment leaves the database naming a place
// the file is: where it sat, or, when the move ran before the next
// checkpoint said so, the name the database already says it wants.
//
// A file enters a name only once nothing sits there, on disk or in the
// database's record. A file that left a name still sits there in the
// database until the checkpoint that says it left has committed, so the
// name stays shut until then: a restore never finds one member's bytes at
// a name the database gives another. A member whose first byte arrives
// while its name is shut opens its file under the internal directory and
// moves to its name the same way.

use weaver_graph::names::{INTERNAL_DIR, key};
use weaver_graph::{MemberId, NodeId, Tree};

use super::{NamespaceWriter, WriteError};
use crate::pipeline::graph::leaf::Leaf;

// A member whose file waits to move to the name it wants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Waiting {
    node: NodeId,
    member: MemberId,
    // The checkpoint that records the name it wants, once one is taken.
    stamp: Option<u64>,
}

impl NamespaceWriter {
    // The member wants `name`: its file, if it has one, moves there once
    // the checkpoint that records it has committed. A member named again
    // before it moved waits for the checkpoint that records the later name,
    // behind every move asked for before it.
    pub(super) fn intend(&mut self, node: NodeId, member: MemberId, name: &str) {
        self.moves
            .retain(|waiting| (waiting.node, waiting.member) != (node, member));
        let Some(leaf) = self
            .files
            .get_mut(&node)
            .and_then(|files| files.leaves.get_mut(&member))
        else {
            return;
        };
        leaf.intend(name);
        if leaf.wants().is_some() {
            self.moves.push(Waiting {
                node,
                member,
                stamp: None,
            });
        }
    }

    // Whether the member's file sits at `name`.
    pub(super) fn sits_at(&self, node: NodeId, member: MemberId, name: &str) -> bool {
        self.files
            .get(&node)
            .and_then(|files| files.leaves.get(&member))
            .is_some_and(|leaf| key(leaf.name()) == key(name))
    }

    // The restored members whose files wait to move, and the names the
    // files sit at.
    pub(crate) fn waiting(&self) -> Vec<(NodeId, MemberId, String)> {
        self.moves
            .iter()
            .filter_map(|waiting| {
                let leaf = self.files.get(&waiting.node)?.leaves.get(&waiting.member)?;
                Some((waiting.node, waiting.member, leaf.name().to_owned()))
            })
            .collect()
    }

    // A new file for the member, at its name when nothing sits there and
    // no pending move can change the allocation. Otherwise it waits under
    // the internal directory until a checkpoint records its ownership and
    // intended name: a crash must not expose an unrecorded temporary name.
    pub(super) fn new_leaf(&mut self, node: NodeId, member: MemberId, name: &str) -> Leaf {
        if !self.moves_waiting() && self.free_at(name, (node, member)) {
            return Leaf::at(&self.working_dir, name);
        }
        let mut leaf = Leaf::at(&self.working_dir, &waiting_name(node, member, name));
        leaf.intend(name);
        self.moves.push(Waiting {
            node,
            member,
            stamp: None,
        });
        leaf
    }

    // The member's file goes, and the name it sat at stays shut until the
    // checkpoint that says so has committed, when the ledger may give it
    // again.
    pub(super) fn discard(&mut self, leaf: Leaf) -> Result<(), WriteError> {
        let name = key(leaf.name());
        leaf.remove()?;
        self.left.insert(name, None);
        Ok(())
    }

    // The checkpoint `stamp` records every name wanted, every name left and
    // every gone node so far.
    pub(crate) fn recorded(&mut self, stamp: u64) {
        for waiting in &mut self.moves {
            waiting.stamp.get_or_insert(stamp);
        }
        for left in self.left.values_mut() {
            left.get_or_insert(stamp);
        }
        for (_, gone) in &mut self.forgotten {
            gone.get_or_insert(stamp);
        }
    }

    // Whether a file still waits to move, or a name a file left waits for
    // the checkpoint that frees it.
    pub(crate) fn moves_waiting(&self) -> bool {
        !self.moves.is_empty() || !self.left.is_empty()
    }

    // Whether anything waits for a checkpoint to commit: a move, a name a
    // file left, or a gone node's file the one the database holds may name.
    pub(crate) fn awaits_commit(&self) -> bool {
        self.moves_waiting() || !self.forgotten.is_empty()
    }

    // The checkpoint whose commit lets every waiting move run, as far as
    // the database is what holds it back.
    pub(crate) fn awaited(&self) -> u64 {
        let moves = self.moves.iter().filter_map(|waiting| waiting.stamp);
        let left = self.left.values().filter_map(|stamp| *stamp);
        let gone = self.forgotten.iter().filter_map(|(_, stamp)| *stamp);
        moves.chain(left).chain(gone).max().unwrap_or(0)
    }

    // The checkpoints through `stamp` have committed. Every name a file
    // left, moved or discarded, that they record is free, and is returned.
    // Every move they record runs, in the order the ledger asked for them,
    // into a name nothing sits at; the rest wait for a later commit. A name
    // a move leaves is shut until the checkpoint that records the move has
    // committed, so it is freed by a later commit, and one a move left
    // before a move failed is freed the same way.
    fn committed(&mut self, stamp: u64) -> (Vec<String>, Result<bool, WriteError>) {
        self.committed = self.committed.max(stamp);
        let committed = self.committed;
        let mut freed = Vec::new();
        self.left.retain(|name, left| {
            let shut = left.is_none_or(|left| left > committed);
            if !shut {
                freed.push(name.clone());
            }
            shut
        });
        let moved = self
            .remove_forgotten(committed)
            .and_then(|()| self.run_moves(committed));
        (freed, moved)
    }

    // Remove every gone node's file that the checkpoints through
    // `committed` stop naming.
    fn remove_forgotten(&mut self, committed: u64) -> Result<(), WriteError> {
        let (gone, kept) = std::mem::take(&mut self.forgotten)
            .into_iter()
            .partition(|(_, stamp)| stamp.is_some_and(|stamp| stamp <= committed));
        self.forgotten = kept;
        for (leaf, _) in gone {
            leaf.remove()?;
        }
        Ok(())
    }

    // Run every waiting move the checkpoints through `committed` record
    // whose name nothing sits at. True when a file moved.
    fn run_moves(&mut self, committed: u64) -> Result<bool, WriteError> {
        let mut moved = false;
        let mut at = 0;
        while let Some(&waiting) = self.moves.get(at) {
            let wants = self
                .files
                .get(&waiting.node)
                .and_then(|files| files.leaves.get(&waiting.member))
                .and_then(|leaf| leaf.wants().map(str::to_owned));
            let Some(wants) = wants else {
                self.moves.remove(at);
                continue;
            };
            let recorded = waiting.stamp.is_some_and(|stamp| stamp <= committed);
            if !recorded || !self.free_at(&wants, (waiting.node, waiting.member)) {
                at += 1;
                continue;
            }
            let dir = self.working_dir.clone();
            let left = self
                .files
                .get_mut(&waiting.node)
                .and_then(|files| files.leaves.get_mut(&waiting.member))
                .map(|leaf| leaf.move_on(&dir))
                .transpose()?
                .flatten();
            if let Some(left) = left {
                self.left.insert(key(&left), None);
                moved = true;
                // Select the actual moved payload, before recording its new location.
                crate::e2e_failpoint::fire_rename(&wants);
            }
            self.moves.remove(at);
        }
        Ok(moved)
    }

    // The checkpoints through `stamp` have committed: each name they free
    // is free in the ledger, which moves the member that wanted it there,
    // and the moves they cover run. A move that fails fails the commit
    // once the names already freed are given back. True when the next
    // checkpoint has something to record.
    pub(crate) fn commit_moves(&mut self, tree: &mut Tree, stamp: u64) -> Result<bool, WriteError> {
        let (freed, moved) = self.committed(stamp);
        let mut effects = Vec::new();
        for name in &freed {
            tree.vacated(name, &mut effects);
        }
        self.moved(tree, &effects)?;
        Ok(moved? || !freed.is_empty())
    }

    // A restored member's file, where the checkpoint says it sits. A file
    // the checkpoint says waits to move is opened where it sat when it is
    // still there; when it is not but is at the name it wants, the move ran
    // before the checkpoint that would have said so, and the file is
    // adopted there. Anywhere else it is lost, and its bytes are fetched
    // again where it sat.
    pub(super) fn restored_leaf(
        &mut self,
        node: NodeId,
        member: MemberId,
        output_name: &str,
        on_disk: Option<&str>,
    ) -> Leaf {
        let Some(sat) = on_disk else {
            return Leaf::at(&self.working_dir, output_name);
        };
        let mut leaf = Leaf::at(&self.working_dir, sat);
        let wanted = Leaf::at(&self.working_dir, output_name);
        if !leaf.path().exists() && wanted.path().exists() {
            self.left.insert(key(sat), None);
            return wanted;
        }
        leaf.intend(output_name);
        self.moves.push(Waiting {
            node,
            member,
            stamp: Some(0),
        });
        leaf
    }

    // Whether a file may enter `name`: no other member's file sits there,
    // and no file left it that the database may still place there. A name a
    // file left stays shut until the checkpoint recording that move commits:
    // were another file to enter it sooner and the job stop first, the
    // restored row of the file that left would still say it sits there, and
    // restore would open the newcomer's bytes as that file.
    fn free_at(&self, name: &str, mover: (NodeId, MemberId)) -> bool {
        let name = key(name);
        let committed = self.committed;
        let shut = self
            .left
            .get(&name)
            .is_some_and(|left| left.is_none_or(|left| left > committed));
        if shut {
            return false;
        }
        // With no file waiting to move, every file sits at the name the
        // ledger gave its member, and the ledger gives a name to one member.
        if self.moves.is_empty() {
            return true;
        }
        !self.files.iter().any(|(node, files)| {
            files
                .leaves
                .iter()
                .any(|(member, leaf)| (*node, *member) != mover && key(leaf.name()) == name)
        })
    }
}

// Where a member's file waits for its name, under the directory finalize
// never delivers. A directory member waits as a directory.
fn waiting_name(node: NodeId, member: MemberId, name: &str) -> String {
    let directory = if Leaf::is_directory(name) { "/" } else { "" };
    format!(
        "{INTERNAL_DIR}/graph-waiting-{}-{}{directory}",
        node.0, member.0
    )
}
