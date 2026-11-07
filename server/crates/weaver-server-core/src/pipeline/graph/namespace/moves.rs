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
//
// The rule holds name by name, so a move waiting for one member never stops
// another opening at its own. After a crash the restore reads, for each
// member, the names the last committed checkpoint gives it: where its file
// sat and the one it wants. Each such name stays out of every other
// member's reach until a checkpoint that stops giving it has committed: a
// member's file sits there, its waiting move wants it, or it is shut as a
// name the member left, the last also when the ledger moved the member off
// a name before its file ever reached it. A new file opens at its name only
// when no other member holds that name in any of these ways, and only when
// that name is the one the ledger keeps for it; the next checkpoint then
// records it there. So whatever stopped, the bytes a restore finds at a
// name are those of the member the database gives that name, and a restored
// move never finds another member's file in its way.

use std::path::Path;

use weaver_graph::names::{INTERNAL_DIR, key};
use weaver_graph::{MemberId, NodeId, Tree};

use super::{NamespaceWriter, WriteError};
use crate::pipeline::graph::envelope::free_name;
use crate::pipeline::graph::leaf::Leaf;

// Where a restored member's file is: the one name of it the checkpoint
// holds to. A file the checkpoint says waits to move is where it sat while
// it is still there, or while nothing is at the name it wants. When it is
// gone from where it sat and a file is at the name it wants, the move ran
// before the checkpoint that would have said so, and that file is the
// member's. A file at the name it wants while it still sits where it sat
// is not the member's: the member's file never reached it.
pub(crate) fn restored_at<'a>(
    working_dir: &Path,
    output_name: &'a str,
    on_disk: Option<&'a str>,
) -> Result<&'a str, super::super::leaf::LeafError> {
    let exists = |name: &str| {
        let path = Leaf::path_at(working_dir, name);
        path.try_exists()
            .map_err(|source| super::super::leaf::LeafError::Io { path, source })
    };
    if let Some(sat) = on_disk
        && (exists(sat)? || !exists(output_name)?)
    {
        return Ok(sat);
    }
    Ok(output_name)
}

// How many files opened under a waiting name, and how many files moved on
// disk, on this thread: a job's writer runs on its pipeline's thread, so a
// test reads its own job's, across every writer a restart makes.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Tally {
    pub(crate) waiting_opens: u64,
    pub(crate) moves: u64,
}

#[cfg(test)]
thread_local! {
    pub(crate) static TALLY: std::cell::Cell<Tally> = std::cell::Cell::default();
}

#[cfg(test)]
fn count(change: impl FnOnce(&mut Tally)) {
    TALLY.with(|tally| {
        let mut now = tally.get();
        change(&mut now);
        tally.set(now);
    });
}

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

    // A new file for the member, at its name when that name is free for it
    // and is the one it keeps. A name the ledger gave only while the one the
    // member wants is leaving (`held_off`) changes once that one is vacated,
    // so a file opened there would be left at a name no checkpoint may ever
    // record. Otherwise it waits under the internal directory until a
    // checkpoint records its ownership and intended name.
    pub(super) fn new_leaf(
        &mut self,
        (node, member): (NodeId, MemberId),
        name: &str,
        held_off: bool,
        root: &Path,
    ) -> Leaf {
        if !held_off && self.free_at(name, (node, member)) {
            return Leaf::at(root, name);
        }
        #[cfg(test)]
        count(|tally| tally.waiting_opens += 1);
        let waiting = waiting_name(root, node, member, name);
        let mut leaf = Leaf::at(root, &waiting);
        leaf.intend(name);
        self.moves.push(Waiting {
            node,
            member,
            stamp: None,
        });
        leaf
    }

    // The file goes once the checkpoint that stops naming it has committed,
    // and the name it sat at stays shut until then, when the ledger may
    // give it again. Until that commit the database may still name the
    // file where it sits, so a restore finds it there. A file never written
    // is not on disk and leaves nothing: its name may be given again before
    // that commit, and removing it then would remove the file given it.
    pub(super) fn discard(&mut self, leaf: Leaf) -> Result<(), WriteError> {
        self.left.insert(key(leaf.name()), None);
        if leaf.on_disk() {
            self.leaving.push((leaf, None));
        }
        Ok(())
    }

    // The checkpoint `stamp` records every name wanted, every name left and
    // every file that goes so far.
    pub(crate) fn recorded(&mut self, stamp: u64) {
        for waiting in &mut self.moves {
            waiting.stamp.get_or_insert(stamp);
        }
        for left in self.left.values_mut() {
            left.get_or_insert(stamp);
        }
        for (_, gone) in &mut self.leaving {
            gone.get_or_insert(stamp);
        }
    }

    // Whether a file still waits to move, or a name a file left waits for
    // the checkpoint that frees it.
    pub(crate) fn moves_waiting(&self) -> bool {
        !self.moves.is_empty() || !self.left.is_empty()
    }

    // Whether anything waits for a checkpoint to commit: a move, a name a
    // file left, or a file that goes which the one the database holds may
    // name.
    pub(crate) fn awaits_commit(&self) -> bool {
        self.moves_waiting() || !self.leaving.is_empty()
    }

    // The checkpoint whose commit lets every waiting move run, as far as
    // the database is what holds it back.
    pub(crate) fn awaited(&self) -> u64 {
        let moves = self.moves.iter().filter_map(|waiting| waiting.stamp);
        let left = self.left.values().filter_map(|stamp| *stamp);
        let gone = self.leaving.iter().filter_map(|(_, stamp)| *stamp);
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
            .remove_left(committed)
            .and_then(|()| self.run_moves(committed));
        (freed, moved)
    }

    // Remove every file that goes which the checkpoints through
    // `committed` stop naming, then every directory that leaves empty:
    // a directory member that goes, and each directory above what went.
    fn remove_left(&mut self, committed: u64) -> Result<(), WriteError> {
        let (gone, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.leaving)
            .into_iter()
            .partition(|(_, stamp)| stamp.is_some_and(|stamp| stamp <= committed));
        self.leaving = kept;
        let mut emptied = std::collections::BTreeSet::new();
        for (leaf, _) in gone {
            let name = leaf.name().trim_end_matches('/').to_owned();
            if Leaf::is_directory(leaf.name()) {
                emptied.insert(name.clone());
            } else {
                leaf.remove()?;
            }
            emptied.extend(above(&name));
        }
        self.remove_emptied(emptied)
    }

    // Remove each of `emptied`, deepest first, that is empty. A directory
    // still holding something stays, and so does one a member sits at or
    // waits to move to, and the internal directory.
    fn remove_emptied(
        &self,
        emptied: std::collections::BTreeSet<String>,
    ) -> Result<(), WriteError> {
        let mut emptied: Vec<String> = emptied.into_iter().collect();
        emptied.sort_by_key(|name| std::cmp::Reverse(name.matches('/').count()));
        for name in emptied {
            let held = key(&name) == key(INTERNAL_DIR)
                || self.files.values().any(|files| {
                    files.leaves.values().any(|other| {
                        std::iter::once(other.name())
                            .chain(other.wants())
                            .any(|at| Leaf::is_directory(at) && key(at) == key(&name))
                    })
                });
            if !held {
                for root in self.roots() {
                    Leaf::at(root, &format!("{name}/")).remove()?;
                }
            }
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
            let leaf = self
                .files
                .get_mut(&waiting.node)
                .and_then(|files| files.leaves.get_mut(&waiting.member));
            #[cfg(test)]
            if leaf.as_ref().is_some_and(|leaf| leaf.on_disk()) {
                count(|tally| tally.moves += 1);
            }
            let left = leaf.map(Leaf::move_on).transpose()?.flatten();
            if let Some(left) = left {
                self.left.insert(key(&left), None);
                self.remove_emptied(above(left.trim_end_matches('/')).collect())?;
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

    // A restored member's file, where `restored_at` finds it. One found
    // at the name it wants has moved there and leaves the name it sat at
    // shut until a commit; one still where it sat waits to move again.
    // The file lies under the directory its member's class gives, unless
    // it is found under the other one only, as a member whose class changed
    // after its file was opened left it.
    pub(super) fn restored_leaf(
        &mut self,
        (node, member): (NodeId, MemberId),
        output_name: &str,
        on_disk: Option<&str>,
        root: &Path,
    ) -> Result<Leaf, WriteError> {
        let root = self
            .restored_root(root, output_name, on_disk)?
            .to_path_buf();
        let at = restored_at(&root, output_name, on_disk)?;
        let mut leaf = Leaf::found(&root, at)?;
        let Some(sat) = on_disk else {
            return Ok(leaf);
        };
        if at != sat {
            self.left.insert(key(sat), None);
            return Ok(leaf);
        }
        leaf.intend(output_name);
        self.moves.push(Waiting {
            node,
            member,
            stamp: Some(0),
        });
        Ok(leaf)
    }

    // The directories the job's files lie under: the working directory,
    // and the output directory when it is another.
    pub(super) fn roots(&self) -> impl Iterator<Item = &Path> {
        std::iter::once(self.working_dir.as_path())
            .chain((self.output_dir != self.working_dir).then_some(self.output_dir.as_path()))
    }

    fn restored_root<'a>(
        &'a self,
        root: &'a Path,
        output_name: &str,
        on_disk: Option<&str>,
    ) -> Result<&'a Path, WriteError> {
        let there = |dir: &Path| -> Result<bool, WriteError> {
            for name in std::iter::once(output_name).chain(on_disk) {
                let path = Leaf::path_at(dir, name);
                if path
                    .try_exists()
                    .map_err(|source| super::super::leaf::LeafError::Io { path, source })?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        if there(root)? {
            return Ok(root);
        }
        for other in self.roots().filter(|other| *other != root) {
            if there(other)? {
                return Ok(other);
            }
        }
        Ok(root)
    }

    // Whether a file may enter `name`: no other member's file sits there or
    // waits to move there, and no member left it that the database may
    // still place there. A name a member left stays shut until the
    // checkpoint recording that commits: were another file to enter it
    // sooner and the job stop first, the restored row of the member that
    // left would still give it that name, and restore would open the
    // newcomer's bytes as that member's file, or its move would find them in
    // the way. The directories the name runs through are held the same way:
    // a name is shut while one of them is, and taken while another member's
    // file sits at one or waits to move to one. A directory is shared, so
    // two directory members may sit at one name.
    fn free_at(&self, name: &str, mover: (NodeId, MemberId)) -> bool {
        let directory = Leaf::is_directory(name);
        let name = key(name);
        let above: Vec<&str> = name.match_indices('/').map(|(at, _)| &name[..at]).collect();
        let committed = self.committed;
        let shut = std::iter::once(name.as_str())
            .chain(above.iter().copied())
            .any(|path| {
                self.left
                    .get(path)
                    .is_some_and(|left| left.is_none_or(|left| left > committed))
            });
        if shut {
            return false;
        }
        // With no file waiting to move, every file sits at the name the
        // ledger gave its member, and the ledger gives a name to one member
        // and never a file a directory's path.
        if self.moves.is_empty() {
            return true;
        }
        !self.files.iter().any(|(node, files)| {
            files.leaves.iter().any(|(member, leaf)| {
                (*node, *member) != mover
                    && std::iter::once(leaf.name())
                        .chain(leaf.wants())
                        .any(|held| {
                            let at = key(held);
                            let file = !Leaf::is_directory(held);
                            (at == name && (file || !directory))
                                || (file && above.contains(&at.as_str()))
                        })
            })
        })
    }
}

// The directories `name` runs through.
fn above(name: &str) -> impl Iterator<Item = String> + '_ {
    name.match_indices('/').map(|(at, _)| name[..at].to_owned())
}

// Where a member's file waits for its name, under the directory finalize
// never delivers: a name no file sits at, as one that goes may still sit at
// the member's last. A directory member waits as a directory.
fn waiting_name(dir: &std::path::Path, node: NodeId, member: MemberId, name: &str) -> String {
    let directory = if Leaf::is_directory(name) { "/" } else { "" };
    let waiting = free_name(
        dir,
        &format!("{INTERNAL_DIR}/graph-waiting-{}-{}", node.0, member.0),
    );
    format!("{waiting}{directory}")
}
