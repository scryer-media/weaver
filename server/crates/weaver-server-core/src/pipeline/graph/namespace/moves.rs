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
            .map_err(|source| super::super::leaf::LeafError::Io {
                device: crate::pipeline::graph::limits::Device::Working,
                path,
                source,
            })
    };
    if let Some(sat) = on_disk
        && (exists(sat)? || !exists(output_name)?)
    {
        return Ok(sat);
    }
    Ok(output_name)
}

// How many files opened under a waiting name, how many files moved on
// disk, how many files a pass of the moves read to weigh them, and how many
// changed files the kept count read again (`Kept`), on this thread: a job's
// writer runs on its pipeline's thread, so a test reads its own job's,
// across every writer a restart makes.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Tally {
    pub(crate) waiting_opens: u64,
    pub(crate) moves: u64,
    pub(crate) weighed: u64,
    pub(crate) kept: u64,
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

// Every waiting move, in the order asked for, with where each member's
// moves sit in it: a member named again drops its moves without a scan of
// every move, so a long chain of members named again costs each one its own
// moves. A dropped move leaves a gap until the gaps outnumber the moves.
#[derive(Debug, Default)]
pub(super) struct Moves {
    order: Vec<Option<Waiting>>,
    at: std::collections::HashMap<(NodeId, MemberId), Vec<usize>>,
    live: usize,
    // How many places in the order dropping and closing gaps read.
    #[cfg(test)]
    read: usize,
}

impl Moves {
    pub(super) fn push(&mut self, waiting: Waiting) {
        self.at
            .entry((waiting.node, waiting.member))
            .or_default()
            .push(self.order.len());
        self.order.push(Some(waiting));
        self.live += 1;
    }

    pub(super) fn is_empty(&self) -> bool {
        self.live == 0
    }

    fn len(&self) -> usize {
        self.live
    }

    fn iter(&self) -> impl Iterator<Item = &Waiting> {
        self.order.iter().flatten()
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut Waiting> {
        self.order.iter_mut().flatten()
    }

    // Every move of the member goes; the others keep their order.
    pub(super) fn drop_member(&mut self, node: NodeId, member: MemberId) {
        let Some(places) = self.at.remove(&(node, member)) else {
            return;
        };
        for place in places {
            #[cfg(test)]
            {
                self.read += 1;
            }
            if self.order[place].take().is_some() {
                self.live -= 1;
            }
        }
        if self.order.len() > 2 * self.live + 16 {
            let kept = self.take();
            #[cfg(test)]
            let read = self.read;
            *self = Self::from(kept);
            #[cfg(test)]
            {
                self.read = read;
            }
        }
    }

    // The moves in order, none left here.
    fn take(&mut self) -> Vec<Waiting> {
        #[cfg(test)]
        {
            self.read += self.order.len();
        }
        self.at.clear();
        self.live = 0;
        std::mem::take(&mut self.order)
            .into_iter()
            .flatten()
            .collect()
    }
}

impl From<Vec<Waiting>> for Moves {
    fn from(order: Vec<Waiting>) -> Self {
        let mut moves = Self::default();
        for waiting in order {
            moves.push(waiting);
        }
        moves
    }
}

impl NamespaceWriter {
    // The member wants `name`: its file, if it has one, moves there once
    // the checkpoint that records it has committed. A member named again
    // before it moved waits for the checkpoint that records the later name,
    // behind every move asked for before it.
    pub(super) fn intend(&mut self, node: NodeId, member: MemberId, name: &str) {
        self.moves.drop_member(node, member);
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
        let to = self.disk_under(root);
        if !held_off && self.free_for_new(name, (node, member)) {
            return Leaf::at(root, name).through(to.as_ref());
        }
        #[cfg(test)]
        count(|tally| tally.waiting_opens += 1);
        let waiting = waiting_name(
            self.envelopes.given(),
            to.as_ref(),
            root,
            node,
            member,
            name,
        );
        let mut leaf = Leaf::at(root, &waiting).through(to.as_ref());
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
        for waiting in self.moves.iter_mut() {
            waiting.stamp.get_or_insert(stamp);
        }
        for left in self.left.values_mut() {
            left.get_or_insert(stamp);
        }
        for (_, gone) in &mut self.leaving {
            gone.get_or_insert(stamp);
        }
        for emptied in self.spills_emptying.values_mut() {
            emptied.get_or_insert(stamp);
        }
        // A scratch file nothing lives on, as `scratch_rewinding` says: this
        // checkpoint names none of it. One allocated past since an earlier
        // checkpoint found it empty waits for this one instead.
        for (node, files) in self.files.iter() {
            let extent = files.holds.scratch_extent();
            if files.holds.scratch_bytes() != 0 || extent == 0 {
                continue;
            }
            let waiting = self
                .scratch_rewinding
                .entry(*node)
                .or_insert((stamp, extent));
            if waiting.1 != extent {
                *waiting = (stamp, extent);
            }
        }
    }

    // Whether a file still waits to move, or a name a file left waits for
    // the checkpoint that frees it.
    pub(crate) fn moves_waiting(&self) -> bool {
        !self.moves.is_empty() || !self.left.is_empty()
    }

    // Whether anything waits for a checkpoint to commit: a move, a name a
    // file left, a file that goes or is emptied, or scratch offsets to
    // start over, which the one the database holds may name.
    pub(crate) fn awaits_commit(&self) -> bool {
        self.moves_waiting()
            || !self.leaving.is_empty()
            || !self.spills_emptying.is_empty()
            || !self.scratch_rewinding.is_empty()
    }

    // The checkpoint whose commit lets every waiting move run, as far as
    // the database is what holds it back.
    pub(crate) fn awaited(&self) -> u64 {
        let moves = self.moves.iter().filter_map(|waiting| waiting.stamp);
        let left = self.left.values().filter_map(|stamp| *stamp);
        let gone = self.leaving.iter().filter_map(|(_, stamp)| *stamp);
        let emptied = self.spills_emptying.values().filter_map(|stamp| *stamp);
        let rewound = self.scratch_rewinding.values().map(|(stamp, _)| *stamp);
        moves
            .chain(left)
            .chain(gone)
            .chain(emptied)
            .chain(rewound)
            .max()
            .unwrap_or(0)
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
        self.rewind_scratches(committed);
        let moved = self
            .remove_left(committed)
            .and_then(|()| self.empty_released_spills(committed))
            .and_then(|()| self.run_moves(committed));
        (freed, moved)
    }

    // The checkpoints through `committed` have committed: each scratch file
    // found empty when one of them was taken, and allocated nothing since,
    // holds no bytes the database names, so its offsets start over.
    fn rewind_scratches(&mut self, committed: u64) {
        let due: Vec<(NodeId, u64)> = self
            .scratch_rewinding
            .iter()
            .filter(|(_, (stamp, _))| *stamp <= committed)
            .map(|(node, (_, extent))| (*node, *extent))
            .collect();
        for (node, extent) in due {
            self.scratch_rewinding.remove(&node);
            let unchanged = self
                .files
                .get(&node)
                .is_some_and(|files| files.holds.scratch_extent() == extent);
            if unchanged && let Some(files) = self.files.get_mut(&node) {
                files.holds.rewind_scratch();
            }
        }
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
        self.remove_emptied(emptied, None)
    }

    // Remove each of `emptied`, deepest first, that is empty. A directory
    // still holding something stays, and so does one a member sits at or
    // waits to move to, and the internal directory. Where the files sit and
    // wait is read once, or taken from the pass that keeps it (`held`),
    // never scanned again for each directory.
    fn remove_emptied(
        &self,
        emptied: std::collections::BTreeSet<String>,
        held: Option<&Held>,
    ) -> Result<(), WriteError> {
        if emptied.is_empty() {
            return Ok(());
        }
        let read;
        let files = match held {
            Some(held) => held,
            None => {
                read = Held::of(self);
                &read
            }
        };
        let mut emptied: Vec<String> = emptied.into_iter().collect();
        emptied.sort_by_key(|name| std::cmp::Reverse(name.matches('/').count()));
        for name in emptied {
            let held = key(&name) == key(INTERNAL_DIR) || files.at(&key(&name)).1 > 0;
            #[cfg(test)]
            assert_eq!(
                held,
                key(&name) == key(INTERNAL_DIR) || self.directory_held_by_scan(&name),
                "{name}"
            );
            if !held {
                for root in self.roots() {
                    Leaf::at(root, &format!("{name}/"))
                        .through(self.disk_under(root).as_ref())
                        .remove()?;
                }
            }
        }
        Ok(())
    }

    // Whether a directory member sits at the directory `name` or waits to
    // move to it, by a scan of every file: what `remove_emptied` read for
    // each directory before. Tests hold it to that.
    #[cfg(test)]
    fn directory_held_by_scan(&self, name: &str) -> bool {
        self.files.values().any(|files| {
            files.leaves.values().any(|other| {
                std::iter::once(other.name())
                    .chain(other.wants())
                    .any(|at| Leaf::is_directory(at) && key(at) == key(name))
            })
        })
    }

    // Run every waiting move the checkpoints through `committed` record
    // whose name nothing sits at. True when a file moved.
    //
    // Where every file sits or waits to move is read once per pass, the
    // first time a move's name is not shut, and kept as the moves run: a
    // pass over a long chain of waiting moves costs the files once, never
    // the files for each move.
    fn run_moves(&mut self, committed: u64) -> Result<bool, WriteError> {
        let mut moved = false;
        let mut held: Option<Held> = None;
        let mut kept = Vec::with_capacity(self.moves.len());
        let mut pending = self.moves.take().into_iter();
        while let Some(waiting) = pending.next() {
            let Some(leaf) = self
                .files
                .get(&waiting.node)
                .and_then(|files| files.leaves.get(&waiting.member))
            else {
                continue;
            };
            let Some(wants) = leaf.wants().map(str::to_owned) else {
                continue;
            };
            let recorded = waiting.stamp.is_some_and(|stamp| stamp <= committed);
            if !recorded || self.name_shut(&wants) {
                kept.push(waiting);
                continue;
            }
            let free = held.get_or_insert_with(|| Held::of(self)).free(
                &wants,
                leaf.name(),
                Some(wants.as_str()),
            );
            if !free {
                kept.push(waiting);
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
            let left = match leaf.map(Leaf::move_on).transpose() {
                Ok(left) => left.flatten(),
                Err(error) => {
                    self.moves = restore_moves(kept, waiting, pending).into();
                    return Err(error.into());
                }
            };
            if let Some(left) = left {
                if let Some(held) = held.as_mut() {
                    held.moved(&left);
                }
                self.left.insert(key(&left), None);
                let emptied = above(left.trim_end_matches('/')).collect();
                if let Err(error) = self.remove_emptied(emptied, held.as_ref()) {
                    self.moves = restore_moves(kept, waiting, pending).into();
                    return Err(error);
                }
                moved = true;
            }
        }
        self.moves = kept.into();
        Ok(moved)
    }

    // Whether `name`, or a directory it runs through, is shut: a file left
    // it and the checkpoint that records that has not committed.
    fn name_shut(&self, name: &str) -> bool {
        let name = key(name);
        let committed = self.committed;
        std::iter::once(name.as_str())
            .chain(name.match_indices('/').map(|(at, _)| &name[..at]))
            .any(|path| {
                self.left
                    .get(path)
                    .is_some_and(|left| left.is_none_or(|left| left > committed))
            })
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
        let mut leaf = Leaf::found(&root, at)?.through(self.disk_under(&root).as_ref());
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
                    .map_err(|source| super::super::leaf::LeafError::Io {
                        device: crate::pipeline::graph::limits::Device::Working,
                        path,
                        source,
                    })?
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
    //
    // Where the files sit and wait is read from the kept count, brought up
    // to date with the files changed since the last new file asked, never
    // from a scan of every file for each new one.
    fn free_for_new(&mut self, name: &str, (node, member): (NodeId, MemberId)) -> bool {
        if self.name_shut(name) {
            return false;
        }
        // With no file waiting to move, every file sits at the name the
        // ledger gave its member, and the ledger gives a name to one member
        // and never a file a directory's path.
        if self.moves.is_empty() {
            return true;
        }
        let held = self.kept.read(&self.files);
        let mover = self
            .files
            .get(&node)
            .and_then(|files| files.leaves.get(&member));
        let free = match mover {
            Some(leaf) => held.free(name, leaf.name(), leaf.wants()),
            None => held.free_beside(name, std::iter::empty()),
        };
        #[cfg(test)]
        assert_eq!(free, self.free_at(name, (node, member)), "{name}");
        free
    }

    // What `free_for_new` answers, by a scan of every file: what each new
    // file read before. Tests hold it to that.
    #[cfg(test)]
    fn free_at(&self, name: &str, mover: (NodeId, MemberId)) -> bool {
        if self.name_shut(name) {
            return false;
        }
        let directory = Leaf::is_directory(name);
        let name = key(name);
        let above: Vec<&str> = name.match_indices('/').map(|(at, _)| &name[..at]).collect();
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

// The moves a pass had not finished when a move failed, in their order:
// those it kept, the one that failed, and those it had not reached.
fn restore_moves(
    mut kept: Vec<Waiting>,
    failed: Waiting,
    rest: impl Iterator<Item = Waiting>,
) -> Vec<Waiting> {
    kept.push(failed);
    kept.extend(rest);
    kept
}

// How many times each path is held by the job's files, as a file and as a
// directory: once for the name each file sits at, and once for the name it
// waits to move to. One pass of the moves reads it in place of a scan of
// every file for each move it weighs.
#[derive(Debug, Default, PartialEq, Eq)]
struct Held(std::collections::HashMap<String, (usize, usize)>);

impl Held {
    fn of(writer: &NamespaceWriter) -> Self {
        let mut held = Self::default();
        for files in writer.files.values() {
            for leaf in files.leaves.values() {
                #[cfg(test)]
                count(|tally| tally.weighed += 1);
                for name in std::iter::once(leaf.name()).chain(leaf.wants()) {
                    held.add(name, 1);
                }
            }
        }
        held
    }

    // A path held `by` more times, or fewer; one held no more is dropped.
    fn add(&mut self, name: &str, by: isize) {
        let path = key(name);
        let held = self.0.entry(path.clone()).or_default();
        let count = if Leaf::is_directory(name) {
            &mut held.1
        } else {
            &mut held.0
        };
        *count = count.checked_add_signed(by).expect("a held path's count");
        if *held == (0, 0) {
            self.0.remove(&path);
        }
    }

    // How many times `path`, a key, is held as a file and as a directory.
    fn at(&self, path: &str) -> (usize, usize) {
        self.0.get(path).copied().unwrap_or_default()
    }

    // A file that wanted the name it sat at moved off `left`: the name it
    // waited for is the one it sits at now, held once less.
    fn moved(&mut self, left: &str) {
        self.add(left, -1);
    }

    // Whether no file but the mover's, which sits at `sits` and waits for
    // `waits`, sits at `name` or waits to move to it, and no file sits at a
    // directory `name` runs through or waits to move to one. A directory
    // `name` shares its path with other directories. The same answer as the
    // scan in `free_at` gives once a move waits and nothing is shut.
    fn free(&self, name: &str, sits: &str, waits: Option<&str>) -> bool {
        self.free_beside(name, std::iter::once(sits).chain(waits))
    }

    // `free` for a mover holding the names `mover`: none for a member with
    // no file yet.
    fn free_beside<'a>(&self, name: &str, mover: impl Iterator<Item = &'a str>) -> bool {
        let directory = Leaf::is_directory(name);
        let name = key(name);
        let mover: Vec<(String, bool)> = mover
            .map(|held| (key(held), !Leaf::is_directory(held)))
            .collect();
        // Held at `path` by others than the mover, as a file and as a
        // directory.
        let others = |path: &str| {
            let (mut files, mut dirs) = self.at(path);
            for (at, file) in &mover {
                if at == path {
                    if *file {
                        files -= 1;
                    } else {
                        dirs -= 1;
                    }
                }
            }
            (files, dirs)
        };
        let (files, dirs) = others(&name);
        if files > 0 || (!directory && dirs > 0) {
            return false;
        }
        !name
            .match_indices('/')
            .any(|(at, _)| others(&name[..at]).0 > 0)
    }
}

// Where every file of the job sits and waits to move, as `Held` counts it,
// kept from one new file to the next: each read takes in only the files
// marked since the last, so a new file asks it whether its name is free
// instead of a scan of every file. It keeps the names it counted for each
// file, to take them back when the file changes or goes; only the files the
// job has open are in it.
#[derive(Debug, Default)]
pub(super) struct Kept {
    // The generation of the namespaces' marks it read up to.
    at: u64,
    nodes: std::collections::HashMap<NodeId, KeptFiles>,
    held: Held,
}

#[derive(Debug, Default)]
struct KeptFiles {
    // Which namespace's files it read (`Files::born`), and the generation
    // of their marks it read up to.
    born: u64,
    at: u64,
    names: std::collections::HashMap<MemberId, (String, Option<String>)>,
}

impl Kept {
    fn read(&mut self, files: &weaver_graph::Marked<NodeId, super::Files>) -> &Held {
        let nodes: Vec<NodeId> = files.marks().since(self.at).collect();
        self.at = files.marks().at();
        for node in nodes {
            let kept = self.nodes.entry(node).or_default();
            let now = files.get(&node);
            if now.is_none_or(|now| now.born != kept.born) {
                for (_, (name, wants)) in kept.names.drain() {
                    self.held.add(&name, -1);
                    if let Some(wants) = wants {
                        self.held.add(&wants, -1);
                    }
                }
                kept.at = 0;
            }
            let Some(now) = now else {
                self.nodes.remove(&node);
                continue;
            };
            kept.born = now.born;
            let members: Vec<MemberId> = now.leaves.marks().since(kept.at).collect();
            kept.at = now.leaves.marks().at();
            for member in members {
                let leaf = now
                    .leaves
                    .get(&member)
                    .map(|leaf| (leaf.name(), leaf.wants()));
                let before = kept
                    .names
                    .get(&member)
                    .map(|(name, wants)| (name.as_str(), wants.as_deref()));
                if leaf == before {
                    continue;
                }
                #[cfg(test)]
                count(|tally| tally.kept += 1);
                if let Some((name, wants)) = kept.names.remove(&member) {
                    self.held.add(&name, -1);
                    if let Some(wants) = wants {
                        self.held.add(&wants, -1);
                    }
                }
                if let Some((name, wants)) = leaf {
                    self.held.add(name, 1);
                    if let Some(wants) = wants {
                        self.held.add(wants, 1);
                    }
                    kept.names
                        .insert(member, (name.to_owned(), wants.map(str::to_owned)));
                }
            }
        }
        &self.held
    }
}

// The directories `name` runs through.
fn above(name: &str) -> impl Iterator<Item = String> + '_ {
    name.match_indices('/').map(|(at, _)| name[..at].to_owned())
}

// Where a member's file waits for its name, under the directory finalize
// never delivers: a name no file sits at, as one that goes may still sit at
// the member's last. A directory member waits as a directory.
fn waiting_name(
    given: &crate::pipeline::graph::envelope::Given,
    to: Option<&std::sync::Arc<crate::operations::disk::writer::Submitter>>,
    dir: &std::path::Path,
    node: NodeId,
    member: MemberId,
    name: &str,
) -> String {
    let directory = if Leaf::is_directory(name) { "/" } else { "" };
    let waiting = given.free(
        to,
        dir,
        &format!("{INTERNAL_DIR}/graph-waiting-{}-{}", node.0, member.0),
    );
    format!("{waiting}{directory}")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use weaver_graph::holds::{HoldBudgets, HoldsAccountant};

    use super::*;
    use crate::operations::disk::CapacityReader;

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
}
