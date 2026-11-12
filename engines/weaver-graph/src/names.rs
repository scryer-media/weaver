// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Output names. Every namespace of a job writes into the one working
// directory, so the tree holds one ledger for all of them and allocates
// every name a member is written under; no object writes a path it chose
// itself. Two members that want one name both get written: the one higher
// in the tree keeps it and the other gets a suffix, the same way the
// download folder suffixes a job whose folder already exists. Which one is
// higher is a matter of position alone, never of arrival, so a restart or
// a different fetch order gives the same names. A directory is no member's
// alone: every member that names it, as itself or as a directory its path
// runs through, shares it, and a file that wants its path takes a suffix.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use unicase::UniCase;
use unicode_normalization::UnicodeNormalization;

use crate::member::MemberId;
use crate::tree::NodeId;

// A member of a namespace of the tree: who holds a name.
pub type Holder = (NodeId, MemberId);

// Where a member sits: the member ids from the root down to it. A shallower
// member ranks first; at equal depth the lower path does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position(pub Vec<MemberId>);

impl Ord for Position {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.0.len(), &self.0).cmp(&(other.0.len(), &other.0))
    }
}

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// A holder's name changed from `old` to `new`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Moved {
    pub holder: Holder,
    pub old: String,
    pub new: String,
}

#[derive(Debug)]
struct Entry {
    // The holder's position, and the holder itself to order two members
    // that sit at one position, a window and the owner beside it.
    rank: (Position, Holder),
    wanted: String,
    name: String,
    // The path it waits for: the one it wants, when it wants a file and
    // holds another path. It is listed under that path in `wanting`.
    waits: Option<PathId>,
}

// A path on disk the ledger holds something at, in `Paths`.
type PathId = u32;

// The working directory itself, which is no name's path.
const TOP: PathId = 0;

#[derive(Debug)]
struct PathNode {
    // The directory the path is in.
    dir: PathId,
    // Its key in `Paths::ids`: the directory's id, then the path's last part.
    key: Arc<[u8]>,
    // How many paths sit directly in it.
    children: usize,
    // How many names hold it as a directory, running through it or being
    // it. No file takes such a path: a file that holds one when a name
    // comes to run through it moves on.
    dirs: usize,
}

// Every path the ledger holds anything at, as a tree of its parts, each
// one under the directory it is in. A name holds one path for each of its
// parts and finds all of them in one read of the name, however deep it is.
#[derive(Debug)]
struct Paths {
    ids: HashMap<Arc<[u8]>, PathId>,
    nodes: Vec<Option<PathNode>>,
    free: Vec<PathId>,
    // Bytes of every part looked up, the directory's id with each.
    #[cfg(test)]
    probed: std::sync::atomic::AtomicUsize,
}

impl Default for Paths {
    fn default() -> Self {
        Self {
            ids: HashMap::new(),
            nodes: vec![Some(PathNode {
                dir: TOP,
                key: Arc::from(&[][..]),
                children: 0,
                dirs: 0,
            })],
            free: Vec::new(),
            #[cfg(test)]
            probed: Default::default(),
        }
    }
}

impl Paths {
    // The path `part` in the directory `dir`, if it is held.
    fn child(&self, dir: PathId, part: &str, probe: &mut Vec<u8>) -> Option<PathId> {
        probe.clear();
        probe.extend_from_slice(&dir.to_le_bytes());
        probe.extend_from_slice(part.as_bytes());
        #[cfg(test)]
        self.probed
            .fetch_add(probe.len(), std::sync::atomic::Ordering::Relaxed);
        self.ids.get(&probe[..]).copied()
    }

    fn child_or_insert(&mut self, dir: PathId, part: &str, probe: &mut Vec<u8>) -> PathId {
        if let Some(id) = self.child(dir, part, probe) {
            return id;
        }
        let key: Arc<[u8]> = Arc::from(&probe[..]);
        let node = PathNode {
            dir,
            key: key.clone(),
            children: 0,
            dirs: 0,
        };
        let id = match self.free.pop() {
            Some(id) => {
                self.nodes[id as usize] = Some(node);
                id
            }
            None => {
                self.nodes.push(Some(node));
                PathId::try_from(self.nodes.len() - 1).expect("fewer paths than ids")
            }
        };
        self.ids.insert(key, id);
        self.node_mut(dir).children += 1;
        id
    }

    // The path `path`, a key, if it is held.
    fn find(&self, path: &str) -> Option<PathId> {
        let mut probe = Vec::new();
        path.split('/')
            .try_fold(TOP, |dir, part| self.child(dir, part, &mut probe))
    }

    // The path `path`, a key, held from here: the caller holds it at once.
    fn insert(&mut self, path: &str) -> PathId {
        let mut probe = Vec::new();
        path.split('/')
            .fold(TOP, |dir, part| self.child_or_insert(dir, part, &mut probe))
    }

    fn get(&self, id: PathId) -> Option<&PathNode> {
        self.nodes.get(id as usize).and_then(Option::as_ref)
    }

    fn node_mut(&mut self, id: PathId) -> &mut PathNode {
        self.nodes[id as usize].as_mut().expect("a held path")
    }

    // How many names hold the path as a directory.
    fn dirs(&self, id: PathId) -> usize {
        self.get(id).map_or(0, |node| node.dirs)
    }

    // The path nothing holds any more goes; its directory is returned.
    fn remove(&mut self, id: PathId) -> PathId {
        let node = self.nodes[id as usize].take().expect("a held path");
        self.ids.remove(&node.key[..]);
        self.free.push(id);
        self.node_mut(node.dir).children -= 1;
        node.dir
    }

    fn clear(&mut self) {
        self.ids.clear();
        self.nodes.truncate(1);
        self.free.clear();
        self.node_mut(TOP).children = 0;
    }
}

#[derive(Debug, Default)]
pub struct NameLedger {
    // Every path below, by its parts.
    paths: Paths,
    // Allocated name, as its path on disk, to the member holding it.
    taken: HashMap<PathId, Holder>,
    // A name, as its path on disk, a member was moved off whose file still
    // sits there: it is free again only once that file has moved, so no
    // other member is given a name a file is still written under.
    leaving: HashMap<PathId, Holder>,
    // Every name in `leaving`, as its path on disk, by the holder it is
    // left by.
    left_by: HashMap<Holder, HashSet<String>>,
    entries: HashMap<Holder, Entry>,
    // A path on disk to the rank of every holder waiting for it, highest
    // first: the first takes the path when it comes free.
    wanting: HashMap<PathId, BTreeSet<(Position, Holder)>>,
    // How many holders a free name was offered to.
    #[cfg(test)]
    offered: usize,
    // How many steps run at once, and the most that ever did.
    #[cfg(test)]
    nesting: usize,
    #[cfg(test)]
    deepest: usize,
    // The most members one change moved on, and how many changes moved a
    // file off a directory's path and that file moved another on.
    #[cfg(test)]
    longest: usize,
    #[cfg(test)]
    through_directory: usize,
}

impl NameLedger {
    pub fn new() -> Self {
        Self::default()
    }

    // The name `holder` may write under for `wanted`. A name held by a
    // member that ranks below it is taken over, and that member moves to
    // its next free name. Every holder whose name changed, `holder` among
    // them when it had one before, is pushed to `moved` in an order that
    // is safe to apply on disk: a member moves out of a name before the
    // member taking it moves in. Idempotent: asking again for the same
    // name, in any spelling of it, returns the one held.
    pub fn allocate(
        &mut self,
        holder: Holder,
        position: Position,
        wanted: &str,
        moved: &mut Vec<Moved>,
    ) -> String {
        let wanted = confined(wanted);
        let old = match self.entries.get(&holder) {
            Some(entry)
                if key(&entry.wanted) == key(&wanted)
                    && is_directory(&entry.wanted) == is_directory(&wanted) =>
            {
                return entry.name.clone();
            }
            Some(entry) => Some(entry.name.clone()),
            None => None,
        };
        let rank = (position, holder);
        // The old name stays the holder's until it has a new one, so no
        // member it moves can move into a file it has not yet left; it then
        // stays shut until the holder's file has left it.
        let name = self.place(holder, &rank, &wanted, moved);
        self.claim(holder, &name, moved);
        if let Some(old) = old.as_ref().filter(|old| **old != name) {
            if !is_directory(old) {
                let (old_path, path) = (key(old), key(&name));
                if old_path != path {
                    self.leave(old_path.clone(), holder);
                }
                if old_path != path || is_directory(&name) {
                    self.untake(&old_path);
                }
            }
            moved.push(Moved {
                holder,
                old: old.clone(),
                new: name.clone(),
            });
        }
        self.enter(holder, rank, wanted, name.clone());
        if let Some(old) = old {
            self.unclaim(&old, moved);
        }
        name
    }

    // A holder comes back with the name it had, as when a job is rebuilt.
    // Restored in rank order, a set of names that were distinct are all
    // given back as they were; a name that is not free is allocated, and
    // whatever moves is not reported, since nothing was written under it.
    pub fn restore(&mut self, holder: Holder, position: Position, wanted: &str, held: &str) {
        let held = confined(held);
        let rank = (position, holder);
        let path = key(&held);
        let name = if is_directory(&held) {
            held
        } else if self
            .paths
            .find(&path)
            .is_some_and(|at| self.taken.contains_key(&at) || self.paths.dirs(at) > 0)
        {
            self.place(holder, &rank, &confined(wanted), &mut Vec::new())
        } else {
            self.take(&path, holder);
            held
        };
        self.claim(holder, &name, &mut Vec::new());
        self.enter(holder, rank, confined(wanted), name);
    }

    // A holder leaves the ledger. No name it held or was moving off is kept
    // for it any more: the holder that wanted each one, the highest ranked
    // when several did, takes it now, and its move is pushed to `moved`, as
    // when the name is vacated. Whether a file still sits at the name on
    // disk is not the ledger's to say; the move waits for that.
    pub fn release(&mut self, holder: Holder, moved: &mut Vec<Moved>) {
        let mut freed: Vec<String> = self
            .left_by
            .remove(&holder)
            .map(|names| names.into_iter().collect())
            .unwrap_or_default();
        for name in &freed {
            if let Some(at) = self.paths.find(name) {
                self.leaving.remove(&at);
                self.prune(at);
            }
        }
        let entry = self.exit(holder);
        if let Some(entry) = entry.as_ref().filter(|entry| !is_directory(&entry.name)) {
            let name = key(&entry.name);
            self.untake(&name);
            freed.push(name);
        }
        freed.sort();
        freed.dedup();
        for name in freed {
            self.take_free(&name, moved);
        }
        if let Some(entry) = entry {
            self.unclaim(&entry.name, moved);
        }
    }

    // The file written under `name` has moved off it: the name is free.
    // The holder that wanted it and was given another while it was left,
    // the highest ranked when several were, takes it now, and its move is
    // pushed to `moved`: a name shut while a file leaves it delays a
    // member's name, never changes it.
    pub fn vacated(&mut self, name: &str, moved: &mut Vec<Moved>) {
        let name = key(name);
        if self.unleave(&name).is_some()
            && let Some(at) = self.paths.find(&name)
        {
            self.take_wanted(at, moved);
        }
    }

    // Restored, a holder given another name while the one it wanted was
    // left takes it when nothing holds or leaves it any more.
    pub fn settle(&mut self, moved: &mut Vec<Moved>) {
        let mut wanted: Vec<String> = self
            .entries
            .values()
            .filter(|entry| key(&entry.wanted) != key(&entry.name))
            .map(|entry| key(&entry.wanted))
            .collect();
        wanted.sort();
        wanted.dedup();
        for name in wanted {
            self.take_free(&name, moved);
        }
    }

    // The path `name`, a key, is given to the file that wants it, unless a
    // file is still leaving it.
    fn take_free(&mut self, name: &str, moved: &mut Vec<Moved>) {
        if let Some(at) = self.paths.find(name)
            && !self.leaving.contains_key(&at)
        {
            self.take_wanted(at, moved);
        }
    }

    // The highest ranked file that wants the free name `name` and holds
    // another takes it.
    fn take_wanted(&mut self, name: PathId, moved: &mut Vec<Moved>) {
        self.run(Step::TakeWanted(name), moved);
    }

    // A restored holder's file still sits at `name`, which it is moving
    // off: the name stays shut until it has, unless another holder holds it.
    pub fn leaving(&mut self, holder: Holder, name: &str) {
        let name = key(name);
        if !self
            .paths
            .find(&name)
            .is_some_and(|at| self.taken.contains_key(&at))
        {
            self.leave(name, holder);
        }
    }

    // Whether the holder was given another name only while the one it wants
    // is leaving: once that one is vacated the holder takes it, so the name
    // it holds now is not the one it keeps.
    pub fn held_off(&self, holder: Holder) -> bool {
        self.entries.get(&holder).is_some_and(|entry| {
            let wanted = key(&entry.wanted);
            wanted != key(&entry.name)
                && self
                    .paths
                    .find(&wanted)
                    .is_some_and(|at| self.leaving.contains_key(&at))
        })
    }

    pub fn given(&self, holder: Holder) -> Option<&str> {
        self.entries.get(&holder).map(|entry| entry.name.as_str())
    }

    pub fn clear(&mut self) {
        self.paths.clear();
        self.taken.clear();
        self.leaving.clear();
        self.left_by.clear();
        self.entries.clear();
        self.wanting.clear();
    }

    // `holder` takes the path `path`, a key, as its file's.
    fn take(&mut self, path: &str, holder: Holder) {
        let at = self.paths.insert(path);
        self.taken.insert(at, holder);
    }

    // No file holds the path `path`, a key, any more.
    fn untake(&mut self, path: &str) {
        if let Some(at) = self.paths.find(path)
            && self.taken.remove(&at).is_some()
        {
            self.prune(at);
        }
    }

    // `at` and every directory above it that nothing holds any more leave
    // `paths`.
    fn prune(&mut self, mut at: PathId) {
        while at != TOP {
            let Some(node) = self.paths.get(at) else {
                return;
            };
            if node.children > 0
                || node.dirs > 0
                || self.taken.contains_key(&at)
                || self.leaving.contains_key(&at)
                || self.wanting.contains_key(&at)
            {
                return;
            }
            at = self.paths.remove(at);
        }
    }

    // `holder` holds `name` for `wanted` at `rank`, in place of whatever
    // it held before; waiting for the name it wants, it is listed under it.
    fn enter(&mut self, holder: Holder, rank: (Position, Holder), wanted: String, name: String) {
        self.exit(holder);
        let path = key(&wanted);
        let waits =
            (!is_directory(&wanted) && path != key(&name)).then(|| self.paths.insert(&path));
        if let Some(at) = waits {
            self.wanting.entry(at).or_default().insert(rank.clone());
        }
        self.entries.insert(
            holder,
            Entry {
                rank,
                wanted,
                name,
                waits,
            },
        );
    }

    // `holder` holds nothing and waits for nothing any more.
    fn exit(&mut self, holder: Holder) -> Option<Entry> {
        let entry = self.entries.remove(&holder)?;
        if let Some(at) = entry.waits
            && let Some(waiting) = self.wanting.get_mut(&at)
        {
            waiting.remove(&entry.rank);
            if waiting.is_empty() {
                self.wanting.remove(&at);
                self.prune(at);
            }
        }
        Some(entry)
    }

    // `holder`'s file sits at `name`, a key, which it is moving off.
    fn leave(&mut self, name: String, holder: Holder) {
        let at = self.paths.insert(&name);
        if let Some(before) = self.leaving.insert(at, holder) {
            self.forget_left(before, &name);
        }
        self.left_by.entry(holder).or_default().insert(name);
    }

    // The file that sat at `name`, a key, has moved off it; the holder it
    // was is returned.
    fn unleave(&mut self, name: &str) -> Option<Holder> {
        let at = self.paths.find(name)?;
        let holder = self.leaving.remove(&at)?;
        self.prune(at);
        self.forget_left(holder, name);
        Some(holder)
    }

    fn forget_left(&mut self, holder: Holder, name: &str) {
        if let Some(names) = self.left_by.get_mut(&holder) {
            names.remove(name);
            if names.is_empty() {
                self.left_by.remove(&holder);
            }
        }
    }

    // The first of `wanted` and its suffixed forms that `holder` may take:
    // one that is free, its own, or held by a member it outranks, which is
    // moved on to a name of its own. A directory is shared, so it takes
    // the name it wants; a file never takes a directory's path.
    fn place(
        &mut self,
        holder: Holder,
        rank: &(Position, Holder),
        wanted: &str,
        moved: &mut Vec<Moved>,
    ) -> String {
        let (name, outranked) = self.seat(holder, rank, wanted);
        if let Some(other) = outranked {
            self.run(Step::Displace(other), moved);
        }
        name
    }

    // `holder` takes the name `place` gives it; the member it outranks
    // there, if any, is returned to be moved on.
    fn seat(
        &mut self,
        holder: Holder,
        rank: &(Position, Holder),
        wanted: &str,
    ) -> (String, Option<Holder>) {
        if is_directory(wanted) {
            return (wanted.to_owned(), None);
        }
        let (stem, extension) = split_extension(wanted);
        for n in 0u32.. {
            let candidate = match n {
                0 => wanted.to_owned(),
                n => format!("{stem} ({n}){extension}"),
            };
            let key = key(&candidate);
            let at = self.paths.find(&key);
            if let Some(at) = at {
                if self.paths.dirs(at) > 0 {
                    continue;
                }
                match self.leaving.get(&at) {
                    Some(leaving) if *leaving != holder => continue,
                    Some(_) => {
                        self.unleave(&key);
                    }
                    None => {}
                }
            }
            // A path the unleave let go is held by no file either.
            let outranked = match at.and_then(|at| self.taken.get(&at)) {
                None => None,
                Some(other) if *other == holder => None,
                Some(other) => match self.entries.get(other) {
                    Some(entry) if *rank < entry.rank => Some(*other),
                    _ => continue,
                },
            };
            self.take(&key, holder);
            return (candidate, outranked);
        }
        unreachable!("an unbounded counter finds a free name")
    }

    // `holder`'s name `name` holds every directory it runs through or is.
    // Another member's file at one of those paths moves on.
    // A directory below the one it is in stays held while this runs: the
    // name holds it, so nothing a moved member does lets it go.
    fn claim(&mut self, holder: Holder, name: &str, moved: &mut Vec<Moved>) {
        self.run(Step::Claim(holder, Walk::new(name)), moved);
    }

    // `name` no longer holds its directories. A path no name holds any more
    // is free to the file that wanted it. The directories below the one
    // given up stay held by the name until it reaches them.
    fn unclaim(&mut self, name: &str, moved: &mut Vec<Moved>) {
        self.run(Step::Unclaim(Walk::new(name)), moved);
    }

    // Runs `first` and every step it sets off, each to its end before the
    // one that set it off goes on, in a loop: a chain of members that move
    // one another on runs at one depth however long it is.
    fn run(&mut self, first: Step, moved: &mut Vec<Moved>) {
        let mut steps = Vec::new();
        let mut next = Some(first);
        #[cfg(test)]
        let (mut displaced, mut by_directory) = (0, false);
        while let Some(step) = next.take().or_else(|| steps.pop()) {
            #[cfg(test)]
            let claim = matches!(step, Step::Claim(..));
            #[cfg(test)]
            {
                displaced += usize::from(matches!(step, Step::Displace(_)));
                self.nesting += 1;
                self.deepest = self.deepest.max(self.nesting);
            }
            self.step(step, &mut steps, moved);
            #[cfg(test)]
            {
                self.nesting -= 1;
                by_directory |= claim && matches!(steps.last(), Some(Step::Displace(_)));
            }
        }
        #[cfg(test)]
        {
            self.longest = self.longest.max(displaced);
            self.through_directory += usize::from(by_directory && displaced >= 2);
        }
    }

    // One step. The steps it sets off are pushed to `steps` with the one
    // to run first last.
    fn step(&mut self, step: Step, steps: &mut Vec<Step>, moved: &mut Vec<Moved>) {
        match step {
            // `other` lost its name to a member that outranks it and takes
            // the next name it may. Its own move is reported after whatever
            // it displaced in turn, so the moves apply in order.
            Step::Displace(other) => {
                let Some(entry) = self.exit(other) else {
                    return;
                };
                let (name, outranked) = self.seat(other, &entry.rank, &entry.wanted);
                steps.push(Step::Unclaim(Walk::new(&entry.name)));
                steps.push(Step::Claim(other, Walk::new(&name)));
                steps.push(Step::Enter {
                    holder: other,
                    rank: entry.rank,
                    wanted: entry.wanted,
                    old: entry.name,
                    name,
                });
                if let Some(next) = outranked {
                    steps.push(Step::Displace(next));
                }
            }
            Step::Enter {
                holder,
                rank,
                wanted,
                old,
                name,
            } => {
                moved.push(Moved {
                    holder,
                    old,
                    new: name.clone(),
                });
                self.enter(holder, rank, wanted, name);
            }
            Step::Report(report) => moved.push(report),
            Step::Claim(holder, mut walk) => {
                let mut probe = Vec::new();
                while let Some(part) = walk.next() {
                    walk.at = self
                        .paths
                        .child_or_insert(walk.at, &walk.path[part], &mut probe);
                    self.paths.node_mut(walk.at).dirs += 1;
                    if let Some(file) = self
                        .taken
                        .get(&walk.at)
                        .copied()
                        .filter(|file| *file != holder)
                    {
                        self.taken.remove(&walk.at);
                        steps.push(Step::Claim(holder, walk));
                        steps.push(Step::Displace(file));
                        return;
                    }
                }
            }
            Step::Unclaim(mut walk) => {
                let mut probe = Vec::new();
                while let Some(part) = walk.next() {
                    let Some(next) = self.paths.child(walk.at, &walk.path[part], &mut probe) else {
                        break;
                    };
                    walk.at = next;
                    let node = self.paths.node_mut(next);
                    if node.dirs == 0 {
                        continue;
                    }
                    node.dirs -= 1;
                    if node.dirs == 0 && !self.leaving.contains_key(&next) {
                        steps.push(Step::Unclaim(walk));
                        steps.push(Step::TakeWanted(next));
                        return;
                    }
                }
                self.prune(walk.at);
            }
            // The highest ranked file that wants the free name `name` and
            // holds another takes it.
            Step::TakeWanted(name) => {
                if self.taken.contains_key(&name) || self.paths.dirs(name) > 0 {
                    return;
                }
                let Some(&(_, holder)) = self.wanting.get(&name).and_then(BTreeSet::first) else {
                    return;
                };
                #[cfg(test)]
                {
                    self.offered += 1;
                }
                // Held from here, the path stays while the holder stops waiting.
                self.taken.insert(name, holder);
                let entry = self.exit(holder).expect("a waiting holder has an entry");
                let old = entry.name;
                let wanted = entry.wanted;
                let old_path = key(&old);
                self.leave(old_path.clone(), holder);
                self.untake(&old_path);
                let claim = Walk::new(&wanted);
                self.enter(holder, entry.rank, wanted.clone(), wanted.clone());
                steps.push(Step::Unclaim(Walk::of(old_path, is_directory(&old))));
                steps.push(Step::Report(Moved {
                    holder,
                    old,
                    new: wanted,
                }));
                steps.push(Step::Claim(holder, claim));
            }
        }
    }
}

// The work one change to the ledger sets off, a step at a time: a member
// that loses its name moves on, and may move another on in turn.
#[derive(Debug)]
enum Step {
    // A member lost its name to one that outranks it.
    Displace(Holder),
    // A moved member holds its new name, and its move is reported.
    Enter {
        holder: Holder,
        rank: (Position, Holder),
        wanted: String,
        old: String,
        name: String,
    },
    Report(Moved),
    // A name holds its directories, from where the walk is on.
    Claim(Holder, Walk),
    // A name gives up its directories, from where the walk is on.
    Unclaim(Walk),
    // A path came free to the file that wants it.
    TakeWanted(PathId),
}

// A walk down the directories a name holds, from the top, as the parts of
// its path on disk: every one the path runs through, and the path itself
// when the name is a directory. "a/b/c.bin" holds "a" and "a/b"; "a/b/"
// holds both too.
#[derive(Debug)]
struct Walk {
    path: String,
    // Where the next part starts.
    from: usize,
    // Where the last part ends, or none when the name runs through none.
    end: Option<usize>,
    // The directory the walk has reached.
    at: PathId,
}

impl Walk {
    fn new(name: &str) -> Self {
        Self::of(key(name), is_directory(name))
    }

    // The walk down `path`, a key, the path of a `directory` or a file.
    fn of(path: String, directory: bool) -> Self {
        let end = if directory {
            Some(path.len())
        } else {
            path.rfind('/')
        };
        Self {
            path,
            from: 0,
            end,
            at: TOP,
        }
    }

    // The next part, as a range of the path.
    fn next(&mut self) -> Option<std::ops::Range<usize>> {
        let end = self.end.filter(|end| self.from <= *end)?;
        let stop = self.path[self.from..end]
            .find('/')
            .map_or(end, |slash| self.from + slash);
        let part = self.from..stop;
        self.from = stop + 1;
        Some(part)
    }
}

// A directory member is a name ending in a separator.
fn is_directory(name: &str) -> bool {
    name.ends_with('/')
}

// The directory of the working directory the job's own files live in:
// scratch, repair and envelope files. No output is written under it.
pub const INTERNAL_DIR: &str = ".weaver-chunks";

// The directory beside it that a process's download caches live in for the
// run they serve. No checkpoint names what is there, and no output is
// written under it either.
pub const CACHE_DIR: &str = ".weaver-cache";

// Every name at the top of the working directory that the job keeps for
// itself and never publishes: the two directories above, the staging
// directory, and the working directory's marker and the replacement written
// for it. Completion leaves each out of the delivery, so no output is
// written under one.
const RESERVED: [&str; 5] = [
    INTERNAL_DIR,
    CACHE_DIR,
    ".weaver-staging",
    ".weaver-job-dir",
    ".weaver-job-dir.upgrade",
];

// A name an archive gave, kept inside the folder it is written under. This
// is the one confinement every output name passes: the leaf that writes it
// only asserts what is decided here. Either separator splits the name, and
// empty, `.` and `..` parts are dropped, so a path can neither climb out nor
// start at the root. A NUL, which no file system takes in a name, becomes
// `_`. A part spelled like a drive (`C:`) is an ordinary directory where
// paths have no drives; on Windows, where a colon in any part names a drive
// (`C:`, `C:foo`) or a file's stream, every colon becomes `_`. A path that
// starts at a reserved name starts at that name's first suffixed form
// instead. A trailing separator, which names a directory, is kept.
fn confined(wanted: &str) -> String {
    let mut name = String::with_capacity(wanted.len() + 1);
    for part in wanted
        .split(['/', '\\'])
        .filter(|part| !part.is_empty() && *part != "." && *part != "..")
    {
        let first = name.is_empty();
        if !first {
            name.push('/');
        }
        name.extend(part.chars().map(confined_char));
        if first
            && let Some(reserved) = RESERVED
                .into_iter()
                .find(|reserved| name.eq_ignore_ascii_case(reserved))
        {
            name = format!("{reserved} (1)");
        }
    }
    if name.is_empty() {
        return "_".to_owned();
    }
    if wanted.ends_with(['/', '\\']) {
        name.push('/');
    }
    name
}

// One character of a part of a confined name, as the file system takes it.
fn confined_char(c: char) -> char {
    match c {
        '\0' => '_',
        ':' if cfg!(windows) => '_',
        c => c,
    }
}

// The path a name takes on disk, as a case-insensitive, normalising file
// system sees it: composed (NFC), so a composed and a decomposed spelling of
// one name are one path, then case-folded character by character. A
// directory and a file of one name are one path, so they share a key and the
// ledger gives only one of them the name. The writer asks it too, of the
// names files sit at on disk.
pub fn key(name: &str) -> String {
    name.strip_suffix('/')
        .unwrap_or(name)
        .nfc()
        .map(fold_case)
        .collect()
}

// The Unicode simple case folding of one character, which never changes a
// name's length in characters: "É" folds to "é", while "ß" stays "ß" and
// so never meets "ss". The full folding is taken where it is one character;
// where it expands, the character's own lower case is taken if that is one
// character ("ẞ" to "ß"), and otherwise the character is its own fold.
fn fold_case(c: char) -> char {
    if c.is_ascii() {
        return c.to_ascii_lowercase();
    }
    let mut utf8 = [0; 4];
    let full = UniCase::unicode(&*c.encode_utf8(&mut utf8)).to_folded_case();
    let mut folded = full.chars();
    if let (Some(single), None) = (folded.next(), folded.next()) {
        return single;
    }
    let mut lower = c.to_lowercase();
    match (lower.next(), lower.next()) {
        (Some(single), None) => single,
        _ => c,
    }
}

// "subs.srt" -> ("subs", ".srt"); "archive.tar.gz" -> ("archive.tar", ".gz");
// ".hidden" -> (".hidden", ""); "noext" -> ("noext", "");
// "v1.0/" -> ("v1.0", "/"), since a directory carries no extension and its
// suffix goes before the separator; "a.d/noext" -> ("a.d/noext", "").
fn split_extension(name: &str) -> (&str, &str) {
    if let Some(directory) = name.strip_suffix('/') {
        return (directory, "/");
    }
    let leaf = name.rfind('/').map_or(0, |slash| slash + 1);
    match name[leaf..].rfind('.') {
        Some(dot) if dot > 0 => name.split_at(leaf + dot),
        _ => (name, ""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: NodeId = NodeId::ROOT;
    const STORE: NodeId = NodeId(1);

    fn at(path: &[u32]) -> Position {
        Position(path.iter().copied().map(MemberId).collect())
    }

    fn root(ledger: &mut NameLedger, member: u32, wanted: &str) -> String {
        let mut moved = Vec::new();
        let name = ledger.allocate((ROOT, MemberId(member)), at(&[member]), wanted, &mut moved);
        assert_eq!(moved, Vec::new());
        name
    }

    #[test]
    fn the_second_member_that_wants_a_name_gets_a_suffix() {
        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 1, "subs.srt"), "subs.srt");
        assert_eq!(root(&mut ledger, 2, "subs.srt"), "subs (1).srt");
        assert_eq!(root(&mut ledger, 3, "SUBS.SRT"), "SUBS (2).SRT");
        assert_eq!(root(&mut ledger, 1, "subs.srt"), "subs.srt");
    }

    #[test]
    fn a_released_name_is_free_again() {
        let mut ledger = NameLedger::new();
        root(&mut ledger, 1, "a.bin");
        ledger.release((ROOT, MemberId(1)), &mut Vec::new());
        assert_eq!(root(&mut ledger, 2, "a.bin"), "a.bin");
    }

    // Two stores over one set at one position: the first holds the name, the
    // second is given a suffix. When the first leaves, the second takes the
    // name it wanted, as when a name is vacated.
    #[test]
    fn a_holder_leaving_hands_its_name_to_the_member_that_wanted_it() {
        let mut ledger = NameLedger::new();
        let (first, second) = ((STORE, MemberId(0)), (NodeId(3), MemberId(0)));
        let mut moved = Vec::new();
        assert_eq!(
            ledger.allocate(first, at(&[0, 0]), "harbour/lantern.mkv", &mut moved),
            "harbour/lantern.mkv"
        );
        assert_eq!(
            ledger.allocate(second, at(&[0, 0]), "harbour/lantern.mkv", &mut moved),
            "harbour/lantern (1).mkv"
        );
        assert_eq!(moved, Vec::new());
        ledger.release(first, &mut moved);
        assert_eq!(ledger.given(second), Some("harbour/lantern.mkv"));
        assert_eq!(
            moved,
            vec![Moved {
                holder: second,
                old: "harbour/lantern (1).mkv".into(),
                new: "harbour/lantern.mkv".into(),
            }]
        );
        // The suffixed name it left is shut until its file has moved.
        assert_eq!(
            root(&mut ledger, 1, "harbour/lantern (1).mkv"),
            "harbour/lantern (1) (1).mkv"
        );
    }

    #[test]
    fn a_rename_moves_the_allocation_and_is_reported() {
        let mut ledger = NameLedger::new();
        root(&mut ledger, 1, "3f9a.bin");
        let mut moved = Vec::new();
        let holder = (ROOT, MemberId(1));
        let name = ledger.allocate(holder, at(&[1]), "show.part01.rar", &mut moved);
        assert_eq!(name, "show.part01.rar");
        assert_eq!(
            moved,
            vec![Moved {
                holder,
                old: "3f9a.bin".into(),
                new: "show.part01.rar".into(),
            }]
        );
        // The file still sits at the name it left until it has moved; the
        // member that wanted it then takes it.
        assert_eq!(root(&mut ledger, 2, "3f9a.bin"), "3f9a (1).bin");
        let mut moved = Vec::new();
        ledger.vacated("3f9a.bin", &mut moved);
        assert_eq!(ledger.given((ROOT, MemberId(2))), Some("3f9a.bin"));
        assert_eq!(
            moved,
            vec![Moved {
                holder: (ROOT, MemberId(2)),
                old: "3f9a (1).bin".into(),
                new: "3f9a.bin".into(),
            }]
        );
    }

    // One member moves off a name and another wants it in the same step:
    // the second is given the name only once the first has left it, so a
    // file never moves onto one that has not moved off.
    #[test]
    fn a_name_left_is_shut_to_others_until_its_file_has_moved() {
        let mut ledger = NameLedger::new();
        let (a, b) = ((ROOT, MemberId(1)), (ROOT, MemberId(2)));
        root(&mut ledger, 1, "old.bin");
        root(&mut ledger, 2, "taken.bin");
        let mut moved = Vec::new();
        assert_eq!(
            ledger.allocate(b, at(&[2]), "later.bin", &mut moved),
            "later.bin"
        );
        assert_eq!(
            ledger.allocate(a, at(&[1]), "taken.bin", &mut moved),
            "taken (1).bin"
        );
        // Its own name left is the holder's to take back.
        assert_eq!(
            ledger.allocate(b, at(&[2]), "taken.bin", &mut moved),
            "taken.bin"
        );
        ledger.allocate(b, at(&[2]), "later.bin", &mut moved);
        moved.clear();
        ledger.vacated("taken.bin", &mut moved);
        assert_eq!(ledger.given(a), Some("taken.bin"));
        assert_eq!(
            moved,
            vec![Moved {
                holder: a,
                old: "taken (1).bin".into(),
                new: "taken.bin".into(),
            }]
        );
        // A released holder leaves nothing shut.
        ledger.allocate(a, at(&[1]), "else.bin", &mut moved);
        ledger.release(a, &mut Vec::new());
        assert_eq!(root(&mut ledger, 3, "taken.bin"), "taken.bin");
    }

    #[test]
    fn a_name_cannot_climb_out_or_start_at_the_root() {
        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 1, "../../etc/a.bin"), "etc/a.bin");
        assert_eq!(root(&mut ledger, 2, "/abs\\b.bin"), "abs/b.bin");
        assert_eq!(root(&mut ledger, 3, "deep/./dir\\"), "deep/dir/");
        assert_eq!(root(&mut ledger, 4, ".."), "_");
        assert_eq!(root(&mut ledger, 5, "plain.bin"), "plain.bin");
    }

    // A drive-shaped part is a directory like any other where paths have no
    // drives, and a NUL never reaches the file system.
    #[cfg(not(windows))]
    #[test]
    fn a_drive_letter_part_stays_a_directory_and_a_nul_is_replaced() {
        let mut ledger = NameLedger::new();
        assert_eq!(
            root(&mut ledger, 1, "C:/adv-escape-drive.txt"),
            "C:/adv-escape-drive.txt"
        );
        assert_eq!(root(&mut ledger, 2, "D:\\x\\y.bin"), "D:/x/y.bin");
        assert_eq!(root(&mut ledger, 3, "dir/c:/z.bin"), "dir/c:/z.bin");
        assert_eq!(root(&mut ledger, 4, "a\0b.bin"), "a_b.bin");
    }

    // On Windows the same part would name the drive, so its colon goes.
    #[cfg(windows)]
    #[test]
    fn a_drive_letter_part_never_names_a_drive() {
        let mut ledger = NameLedger::new();
        assert_eq!(
            root(&mut ledger, 1, "C:/adv-escape-drive.txt"),
            "C_/adv-escape-drive.txt"
        );
        assert_eq!(root(&mut ledger, 2, "a\0b.bin"), "a_b.bin");
        assert_eq!(root(&mut ledger, 3, "C:foo/bar.bin"), "C_foo/bar.bin");
        assert_eq!(root(&mut ledger, 4, "dir/x:y.bin"), "dir/x_y.bin");
    }

    #[test]
    fn no_name_lands_in_the_internal_directory() {
        let mut ledger = NameLedger::new();
        assert_eq!(
            root(&mut ledger, 1, ".weaver-chunks/a.bin"),
            ".weaver-chunks (1)/a.bin"
        );
        assert_eq!(
            root(&mut ledger, 2, "./.WEAVER-CHUNKS\\b.bin"),
            ".weaver-chunks (1)/b.bin"
        );
        // A file of the name the directory above takes is suffixed again.
        assert_eq!(
            root(&mut ledger, 3, ".weaver-chunks"),
            ".weaver-chunks (1) (1)"
        );
        assert_eq!(
            root(&mut ledger, 4, "dir/.weaver-chunks/c.bin"),
            "dir/.weaver-chunks/c.bin"
        );
        assert_eq!(
            root(&mut ledger, 5, ".Weaver-Cache/d.bin"),
            ".weaver-cache (1)/d.bin"
        );
    }

    // Completion publishes nothing at a name the job keeps for itself, so
    // an archive's member that wants one is written beside it instead.
    #[test]
    fn no_name_lands_where_completion_publishes_nothing() {
        let mut ledger = NameLedger::new();
        assert_eq!(
            root(&mut ledger, 1, ".weaver-staging/payload.bin"),
            ".weaver-staging (1)/payload.bin"
        );
        assert_eq!(
            root(&mut ledger, 2, ".weaver-job-dir"),
            ".weaver-job-dir (1)"
        );
        assert_eq!(
            root(&mut ledger, 3, ".weaver-job-dir.upgrade"),
            ".weaver-job-dir.upgrade (1)"
        );
        assert_eq!(
            root(&mut ledger, 4, "dir/.weaver-job-dir"),
            "dir/.weaver-job-dir"
        );
    }

    #[test]
    fn names_without_an_extension_suffix_at_the_end() {
        let mut ledger = NameLedger::new();
        root(&mut ledger, 1, ".hidden");
        root(&mut ledger, 2, "noext");
        assert_eq!(root(&mut ledger, 3, ".hidden"), ".hidden (1)");
        assert_eq!(root(&mut ledger, 4, "noext"), "noext (1)");
    }

    // A directory is shared: every member that names it writes into the
    // one directory, so a second is never suffixed. A file still is.
    #[test]
    fn a_directory_of_one_name_is_shared() {
        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 1, "docs/"), "docs/");
        assert_eq!(root(&mut ledger, 2, "docs/"), "docs/");
        assert_eq!(root(&mut ledger, 3, "v1.0\\"), "v1.0/");
        assert_eq!(root(&mut ledger, 4, "v1.0/"), "v1.0/");
        assert_eq!(root(&mut ledger, 5, "a.d/noext"), "a.d/noext");
        assert_eq!(root(&mut ledger, 6, "a.d/noext"), "a.d/noext (1)");
        // One leaving keeps the directory for the other; once neither
        // holds it, the file that wanted its path takes it.
        ledger.release((ROOT, MemberId(1)), &mut Vec::new());
        assert_eq!(root(&mut ledger, 7, "docs"), "docs (1)");
        let mut moved = Vec::new();
        ledger.release((ROOT, MemberId(2)), &mut moved);
        assert_eq!(ledger.given((ROOT, MemberId(7))), Some("docs"));
        assert_eq!(
            moved,
            vec![Moved {
                holder: (ROOT, MemberId(7)),
                old: "docs (1)".into(),
                new: "docs".into(),
            }]
        );
    }

    // A file never takes a directory's path: whichever arrives first, and
    // whichever ranks higher, the directory keeps it and the file moves on.
    #[test]
    fn a_directory_and_a_file_of_one_name_never_share_its_path() {
        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 2, "folder/"), "folder/");
        assert_eq!(root(&mut ledger, 1, "folder"), "folder (1)");

        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 1, "folder"), "folder");
        let mut moved = Vec::new();
        let directory = (ROOT, MemberId(2));
        assert_eq!(
            ledger.allocate(directory, at(&[2]), "folder/", &mut moved),
            "folder/"
        );
        assert_eq!(
            moved,
            vec![Moved {
                holder: (ROOT, MemberId(1)),
                old: "folder".into(),
                new: "folder (1)".into(),
            }]
        );
    }

    // Two archives, one with a file "folder" and one with "folder/b.bin"
    // and no directory entry: both paths cannot be on disk at once, so the
    // file moves on, whichever arrives first. Once nothing runs through
    // the directory, the file takes the name it wanted.
    #[test]
    fn a_file_never_takes_the_path_of_a_directory_a_name_runs_through() {
        let (file, nested) = ((ROOT, MemberId(1)), (STORE, MemberId(0)));
        let mut ledger = NameLedger::new();
        let mut moved = Vec::new();
        assert_eq!(
            ledger.allocate(file, at(&[1]), "folder", &mut moved),
            "folder"
        );
        assert_eq!(
            ledger.allocate(nested, at(&[0, 0]), "Folder/b.bin", &mut moved),
            "Folder/b.bin"
        );
        assert_eq!(
            moved,
            vec![Moved {
                holder: file,
                old: "folder".into(),
                new: "folder (1)".into(),
            }]
        );

        let mut ledger = NameLedger::new();
        let mut moved = Vec::new();
        ledger.allocate(nested, at(&[0, 0]), "folder/b.bin", &mut moved);
        assert_eq!(
            ledger.allocate(file, at(&[1]), "folder", &mut moved),
            "folder (1)"
        );
        assert_eq!(moved, Vec::new());
        ledger.release(nested, &mut moved);
        assert_eq!(ledger.given(file), Some("folder"));
        assert_eq!(
            moved,
            vec![Moved {
                holder: file,
                old: "folder (1)".into(),
                new: "folder".into(),
            }]
        );
    }

    // The members of the rulings below: a root member, a member two levels
    // down, and one at the same depth with a lower path.
    fn shallow() -> (Holder, Position) {
        ((ROOT, MemberId(5)), at(&[5]))
    }

    fn deep() -> (Holder, Position) {
        ((NodeId(2), MemberId(0)), at(&[0, 0, 0]))
    }

    fn deep_lower() -> (Holder, Position) {
        ((NodeId(3), MemberId(4)), at(&[0, 0, 1]))
    }

    // Allocate every member in order and return each one's final name.
    fn settle(order: &[(Holder, Position, &str)]) -> (Vec<String>, Vec<Moved>) {
        let mut ledger = NameLedger::new();
        let mut moved = Vec::new();
        for (holder, position, wanted) in order {
            ledger.allocate(*holder, position.clone(), wanted, &mut moved);
        }
        let mut names: Vec<(Holder, String)> = order
            .iter()
            .map(|(holder, _, _)| (*holder, ledger.given(*holder).unwrap().to_owned()))
            .collect();
        names.sort();
        (names.into_iter().map(|(_, name)| name).collect(), moved)
    }

    #[test]
    fn the_shallower_member_keeps_the_name_whichever_arrives_first() {
        let ((top, top_at), (low, low_at)) = (shallow(), deep());
        let first = settle(&[
            (top, top_at.clone(), "subs.srt"),
            (low, low_at.clone(), "subs.srt"),
        ]);
        let second = settle(&[(low, low_at, "subs.srt"), (top, top_at, "subs.srt")]);
        assert_eq!(first.0, vec!["subs.srt", "subs (1).srt"]);
        assert_eq!(second.0, first.0);
        assert_eq!(first.1, Vec::new());
        assert_eq!(
            second.1,
            vec![Moved {
                holder: low,
                old: "subs.srt".into(),
                new: "subs (1).srt".into(),
            }]
        );
    }

    #[test]
    fn at_equal_depth_the_lower_path_keeps_the_name() {
        let ((a, a_at), (b, b_at)) = (deep(), deep_lower());
        let forward = settle(&[(a, a_at.clone(), "x.bin"), (b, b_at.clone(), "x.bin")]);
        let backward = settle(&[(b, b_at, "x.bin"), (a, a_at, "x.bin")]);
        // Holders sort with the lower node first: `a`, then `b`.
        assert_eq!(forward.0, vec!["x.bin", "x (1).bin"]);
        assert_eq!(backward.0, forward.0);
    }

    #[test]
    fn a_displaced_holder_moves_on_and_displaces_in_turn() {
        // `deep_lower` holds the first suffix; the deep member, outranked
        // by the root one, takes it over, and every move comes out in an
        // order that never moves a file onto one still in place.
        let ((top, top_at), (mid, mid_at), (low, low_at)) = (shallow(), deep(), deep_lower());
        let (names, moved) = settle(&[
            (low, low_at, "subs.srt"),
            (mid, mid_at, "subs.srt"),
            (top, top_at, "subs.srt"),
        ]);
        assert_eq!(names, vec!["subs.srt", "subs (1).srt", "subs (2).srt"]);
        let moves: Vec<(Holder, &str, &str)> = moved
            .iter()
            .map(|m| (m.holder, m.old.as_str(), m.new.as_str()))
            .collect();
        assert_eq!(
            moves,
            vec![
                (low, "subs.srt", "subs (1).srt"),
                (low, "subs (1).srt", "subs (2).srt"),
                (mid, "subs.srt", "subs (1).srt"),
            ]
        );
    }

    #[test]
    fn case_folding_applies_across_namespaces() {
        let ((top, top_at), (low, low_at)) = (shallow(), deep());
        let (names, _) = settle(&[(low, low_at, "Subs.SRT"), (top, top_at, "subs.srt")]);
        assert_eq!(names, vec!["subs.srt", "Subs (1).SRT"]);
    }

    #[test]
    fn a_composed_and_a_decomposed_spelling_are_one_name() {
        let composed = "Caf\u{e9}.mkv";
        let decomposed = "Cafe\u{301}.mkv";
        assert_eq!(key(composed), key(decomposed));
        // The shallower member keeps the name, whichever spelling it wanted
        // and whichever arrives first.
        let ((top, top_at), (low, low_at)) = (shallow(), deep());
        let first = settle(&[
            (top, top_at.clone(), composed),
            (low, low_at.clone(), decomposed),
        ]);
        let second = settle(&[(low, low_at, decomposed), (top, top_at, composed)]);
        assert_eq!(
            first.0,
            vec![composed.to_owned(), "Cafe\u{301} (1).mkv".to_owned()]
        );
        assert_eq!(second.0, first.0);
    }

    // A name asked for again in another spelling of it is the same name:
    // the holder keeps the one it holds and nothing moves.
    #[test]
    fn a_name_asked_for_again_in_another_spelling_moves_nothing() {
        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 1, "Caf\u{e9}.mkv"), "Caf\u{e9}.mkv");
        assert_eq!(root(&mut ledger, 1, "Cafe\u{301}.mkv"), "Caf\u{e9}.mkv");
        assert_eq!(root(&mut ledger, 1, "CAF\u{c9}.MKV"), "Caf\u{e9}.mkv");
        assert_eq!(root(&mut ledger, 2, "harbour/"), "harbour/");
        assert_eq!(root(&mut ledger, 2, "Harbour/"), "harbour/");
    }

    #[test]
    fn case_folding_covers_letters_beyond_ascii() {
        assert_eq!(key("\u{c9}t\u{e9}.mkv"), key("\u{e9}t\u{e9}.mkv"));
        let mut ledger = NameLedger::new();
        assert_eq!(
            root(&mut ledger, 1, "\u{c9}t\u{e9}.mkv"),
            "\u{c9}t\u{e9}.mkv"
        );
        assert_eq!(
            root(&mut ledger, 2, "\u{e9}t\u{e9}.mkv"),
            "\u{e9}t\u{e9} (1).mkv"
        );
    }

    #[test]
    fn case_folding_never_changes_a_name_length() {
        // Simple folding: the sharp s is its own fold, so it never meets
        // "ss"; the capital sharp s folds to it.
        assert_ne!(key("stra\u{df}e"), key("STRASSE"));
        assert_eq!(key("STRA\u{1e9e}E"), key("stra\u{df}e"));
        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 1, "stra\u{df}e"), "stra\u{df}e");
        assert_eq!(root(&mut ledger, 2, "STRASSE"), "STRASSE");
    }

    #[test]
    fn a_directory_and_a_file_of_one_name_share_a_key() {
        assert_eq!(key("\u{c9}t\u{e9}/"), key("e\u{301}te\u{301}"));
        assert_eq!(key("Folder/"), key("folder"));
    }

    #[test]
    fn restored_in_any_order_the_names_come_back_as_they_were() {
        let ((top, top_at), (low, low_at)) = (shallow(), deep());
        let restored = |order: &[(Holder, &Position, &str)]| {
            let mut ledger = NameLedger::new();
            for (holder, position, held) in order {
                ledger.restore(*holder, (*position).clone(), "subs.srt", held);
            }
            (
                ledger.given(top).unwrap().to_owned(),
                ledger.given(low).unwrap().to_owned(),
            )
        };
        let forward = restored(&[(top, &top_at, "subs.srt"), (low, &low_at, "subs (1).srt")]);
        let backward = restored(&[(low, &low_at, "subs (1).srt"), (top, &top_at, "subs.srt")]);
        assert_eq!(forward, ("subs.srt".into(), "subs (1).srt".into()));
        assert_eq!(backward, forward);
        // A restored holder still wants its first name: moved again, it
        // starts from there.
        let mut ledger = NameLedger::new();
        ledger.restore(low, low_at.clone(), "subs.srt", "subs (1).srt");
        let mut moved = Vec::new();
        ledger.allocate(
            (STORE, MemberId(9)),
            at(&[0, 9]),
            "subs (1).srt",
            &mut moved,
        );
        assert_eq!(ledger.given(low), Some("subs.srt"));
    }

    // Releasing members that no other member waits for offers their names
    // to nobody, however many there are; where every member wants one name,
    // each release offers it to one member.
    #[test]
    fn a_release_offers_its_names_only_to_the_members_waiting_for_them() {
        const MEMBERS: u32 = 2_000;
        let mut ledger = NameLedger::new();
        for member in 0..MEMBERS {
            root(&mut ledger, member, &format!("member-{member}.bin"));
        }
        let mut moved = Vec::new();
        for member in 0..MEMBERS {
            ledger.release((ROOT, MemberId(member)), &mut moved);
        }
        assert_eq!(moved, Vec::new());
        assert_eq!(ledger.offered, 0);
        assert!(ledger.entries.is_empty() && ledger.wanting.is_empty());

        // Fewer here: giving one name to every member probes each suffix
        // the ones before it took.
        const SAME: u32 = 300;
        let mut ledger = NameLedger::new();
        for member in 0..SAME {
            root(&mut ledger, member, "same.bin");
        }
        for member in 0..SAME - 1 {
            ledger.release((ROOT, MemberId(member)), &mut moved);
        }
        assert_eq!(ledger.given((ROOT, MemberId(SAME - 1))), Some("same.bin"));
        assert_eq!(moved.len(), SAME as usize - 1);
        assert_eq!(ledger.offered, SAME as usize - 1);
    }

    // The ledger as it was before it kept its waiting and leaving indexes
    // and its paths by their parts: every free name is offered by a scan of
    // every entry, a release scans every name being left, and every
    // directory is kept as its whole path. The ledger must answer as this
    // does.
    mod reference {
        use std::collections::HashMap;

        use super::super::{Holder, Moved, Position, confined, is_directory, key};

        // The directories a name holds, each as its whole path on disk.
        pub fn directories(name: &str) -> Vec<String> {
            let path = key(name);
            let mut dirs: Vec<String> = path
                .match_indices('/')
                .map(|(at, _)| path[..at].to_owned())
                .collect();
            if is_directory(name) {
                dirs.push(path);
            }
            dirs
        }

        struct Entry {
            rank: (Position, Holder),
            wanted: String,
            name: String,
        }

        #[derive(Default)]
        pub struct Ledger {
            taken: HashMap<String, Holder>,
            dirs: HashMap<String, usize>,
            leaving: HashMap<String, Holder>,
            entries: HashMap<Holder, Entry>,
        }

        impl Ledger {
            pub fn allocate(
                &mut self,
                holder: Holder,
                position: Position,
                wanted: &str,
                moved: &mut Vec<Moved>,
            ) -> String {
                let wanted = confined(wanted);
                let old = match self.entries.get(&holder) {
                    Some(entry)
                        if key(&entry.wanted) == key(&wanted)
                            && is_directory(&entry.wanted) == is_directory(&wanted) =>
                    {
                        return entry.name.clone();
                    }
                    Some(entry) => Some(entry.name.clone()),
                    None => None,
                };
                let rank = (position, holder);
                let name = self.place(holder, &rank, &wanted, moved);
                self.claim(holder, &name, moved);
                if let Some(old) = old.as_ref().filter(|old| **old != name) {
                    if !is_directory(old) && (key(old) != key(&name) || is_directory(&name)) {
                        self.taken.remove(&key(old));
                    }
                    if !is_directory(old) && key(old) != key(&name) {
                        self.leaving.insert(key(old), holder);
                    }
                    moved.push(Moved {
                        holder,
                        old: old.clone(),
                        new: name.clone(),
                    });
                }
                self.entries.insert(
                    holder,
                    Entry {
                        rank,
                        wanted,
                        name: name.clone(),
                    },
                );
                if let Some(old) = old {
                    self.unclaim(&old, moved);
                }
                name
            }

            pub fn restore(
                &mut self,
                holder: Holder,
                position: Position,
                wanted: &str,
                held: &str,
            ) {
                let held = confined(held);
                let rank = (position, holder);
                let path = key(&held);
                let name = if is_directory(&held) {
                    held
                } else if self.taken.contains_key(&path) || self.dirs.contains_key(&path) {
                    self.place(holder, &rank, &confined(wanted), &mut Vec::new())
                } else {
                    self.taken.insert(path, holder);
                    held
                };
                self.claim(holder, &name, &mut Vec::new());
                self.entries.insert(
                    holder,
                    Entry {
                        rank,
                        wanted: confined(wanted),
                        name,
                    },
                );
            }

            pub fn release(&mut self, holder: Holder, moved: &mut Vec<Moved>) {
                let mut freed: Vec<String> = self
                    .leaving
                    .iter()
                    .filter(|(_, leaving)| **leaving == holder)
                    .map(|(name, _)| name.clone())
                    .collect();
                for name in &freed {
                    self.leaving.remove(name);
                }
                let entry = self.entries.remove(&holder);
                if let Some(entry) = entry.as_ref().filter(|entry| !is_directory(&entry.name)) {
                    let name = key(&entry.name);
                    self.taken.remove(&name);
                    freed.push(name);
                }
                freed.sort();
                freed.dedup();
                for name in freed {
                    if !self.leaving.contains_key(&name) {
                        self.take_wanted(name, moved);
                    }
                }
                if let Some(entry) = entry {
                    self.unclaim(&entry.name, moved);
                }
            }

            pub fn vacated(&mut self, name: &str, moved: &mut Vec<Moved>) {
                let name = key(name);
                if self.leaving.remove(&name).is_some() {
                    self.take_wanted(name, moved);
                }
            }

            pub fn settle(&mut self, moved: &mut Vec<Moved>) {
                let mut wanted: Vec<String> = self
                    .entries
                    .values()
                    .filter(|entry| key(&entry.wanted) != key(&entry.name))
                    .map(|entry| key(&entry.wanted))
                    .collect();
                wanted.sort();
                wanted.dedup();
                for name in wanted {
                    if !self.leaving.contains_key(&name) {
                        self.take_wanted(name, moved);
                    }
                }
            }

            fn take_wanted(&mut self, name: String, moved: &mut Vec<Moved>) {
                if self.taken.contains_key(&name) || self.dirs.contains_key(&name) {
                    return;
                }
                let Some((holder, wanted)) = self
                    .entries
                    .iter()
                    .filter(|(_, entry)| {
                        !is_directory(&entry.wanted)
                            && key(&entry.wanted) == name
                            && key(&entry.name) != name
                    })
                    .min_by(|(_, one), (_, other)| one.rank.cmp(&other.rank))
                    .map(|(holder, entry)| (*holder, entry.wanted.clone()))
                else {
                    return;
                };
                let entry = self.entries.get_mut(&holder).expect("found above");
                let old = std::mem::replace(&mut entry.name, wanted.clone());
                self.taken.remove(&key(&old));
                self.leaving.insert(key(&old), holder);
                self.taken.insert(name, holder);
                self.claim(holder, &wanted, moved);
                moved.push(Moved {
                    holder,
                    old: old.clone(),
                    new: wanted,
                });
                self.unclaim(&old, moved);
            }

            pub fn leaving(&mut self, holder: Holder, name: &str) {
                let name = key(name);
                if !self.taken.contains_key(&name) {
                    self.leaving.insert(name, holder);
                }
            }

            pub fn held_off(&self, holder: Holder) -> bool {
                self.entries.get(&holder).is_some_and(|entry| {
                    let wanted = key(&entry.wanted);
                    wanted != key(&entry.name) && self.leaving.contains_key(&wanted)
                })
            }

            pub fn given(&self, holder: Holder) -> Option<&str> {
                self.entries.get(&holder).map(|entry| entry.name.as_str())
            }

            pub fn dirs(&self) -> &HashMap<String, usize> {
                &self.dirs
            }

            fn place(
                &mut self,
                holder: Holder,
                rank: &(Position, Holder),
                wanted: &str,
                moved: &mut Vec<Moved>,
            ) -> String {
                if is_directory(wanted) {
                    return wanted.to_owned();
                }
                let (stem, extension) = super::super::split_extension(wanted);
                for n in 0u32.. {
                    let candidate = match n {
                        0 => wanted.to_owned(),
                        n => format!("{stem} ({n}){extension}"),
                    };
                    let key = key(&candidate);
                    if self.dirs.contains_key(&key) {
                        continue;
                    }
                    match self.leaving.get(&key) {
                        Some(leaving) if *leaving != holder => continue,
                        Some(_) => {
                            self.leaving.remove(&key);
                        }
                        None => {}
                    }
                    let outranked = match self.taken.get(&key) {
                        None => None,
                        Some(other) if *other == holder => None,
                        Some(other) => match self.entries.get(other) {
                            Some(entry) if *rank < entry.rank => Some(*other),
                            _ => continue,
                        },
                    };
                    self.taken.insert(key, holder);
                    if let Some(other) = outranked {
                        self.displace(other, moved);
                    }
                    return candidate;
                }
                unreachable!("an unbounded counter finds a free name")
            }

            fn displace(&mut self, other: Holder, moved: &mut Vec<Moved>) {
                let Some(entry) = self.entries.remove(&other) else {
                    return;
                };
                let name = self.place(other, &entry.rank, &entry.wanted, moved);
                let old = entry.name.clone();
                moved.push(Moved {
                    holder: other,
                    old: old.clone(),
                    new: name.clone(),
                });
                self.entries.insert(
                    other,
                    Entry {
                        name: name.clone(),
                        ..entry
                    },
                );
                self.claim(other, &name, moved);
                self.unclaim(&old, moved);
            }

            fn claim(&mut self, holder: Holder, name: &str, moved: &mut Vec<Moved>) {
                for dir in directories(name) {
                    *self.dirs.entry(dir.clone()).or_default() += 1;
                    if let Some(file) = self.taken.get(&dir).copied().filter(|file| *file != holder)
                    {
                        self.taken.remove(&dir);
                        self.displace(file, moved);
                    }
                }
            }

            fn unclaim(&mut self, name: &str, moved: &mut Vec<Moved>) {
                for dir in directories(name) {
                    let Some(count) = self.dirs.get_mut(&dir) else {
                        continue;
                    };
                    *count -= 1;
                    if *count == 0 {
                        self.dirs.remove(&dir);
                        if !self.leaving.contains_key(&dir) {
                            self.take_wanted(dir, moved);
                        }
                    }
                }
            }
        }
    }

    impl Paths {
        // The path `at` is, as its key.
        fn path(&self, mut at: PathId) -> String {
            let mut parts = Vec::new();
            while at != TOP {
                let node = self.get(at).expect("a held path");
                parts.push(std::str::from_utf8(&node.key[4..]).expect("a part of a key"));
                at = node.dir;
            }
            parts.reverse();
            parts.join("/")
        }

        // Every held path, the working directory aside.
        fn held(&self) -> impl Iterator<Item = (PathId, &PathNode)> {
            (0..self.nodes.len())
                .filter_map(|at| Some((at as PathId, self.nodes[at].as_ref()?)))
                .filter(|(at, _)| *at != TOP)
        }

        // The bytes the held paths keep: their keys.
        fn bytes(&self) -> usize {
            self.held().map(|(_, node)| node.key.len()).sum()
        }

        fn probed(&self) -> usize {
            self.probed.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    // The waiting and leaving indexes say exactly what a scan of the
    // entries and the names being left would, the paths are exactly the
    // ones something holds, and every directory is held as many times as
    // the reference holds its whole path.
    fn assert_indexes_hold(ledger: &NameLedger, dirs: &HashMap<String, usize>) {
        let paths = &ledger.paths;
        let mut wanting: HashMap<String, BTreeSet<(Position, Holder)>> = HashMap::new();
        for entry in ledger.entries.values() {
            if !is_directory(&entry.wanted) && key(&entry.wanted) != key(&entry.name) {
                wanting
                    .entry(key(&entry.wanted))
                    .or_default()
                    .insert(entry.rank.clone());
            }
        }
        let ours: HashMap<String, BTreeSet<(Position, Holder)>> = ledger
            .wanting
            .iter()
            .map(|(at, waiting)| (paths.path(*at), waiting.clone()))
            .collect();
        assert_eq!(ours, wanting);
        assert_eq!(ours.len(), ledger.wanting.len());
        let mut left_by: HashMap<Holder, HashSet<String>> = HashMap::new();
        for (at, holder) in &ledger.leaving {
            left_by.entry(*holder).or_default().insert(paths.path(*at));
        }
        assert_eq!(ledger.left_by, left_by);
        let ours: HashMap<String, usize> = paths
            .held()
            .filter(|(_, node)| node.dirs > 0)
            .map(|(at, node)| (paths.path(at), node.dirs))
            .collect();
        assert_eq!(&ours, dirs);
        let mut children: HashMap<PathId, usize> = HashMap::new();
        for (_, node) in paths.held() {
            *children.entry(node.dir).or_default() += 1;
        }
        for (at, node) in paths.nodes.iter().enumerate() {
            let Some(node) = node else { continue };
            let at = at as PathId;
            assert_eq!(node.children, children.get(&at).copied().unwrap_or(0));
            assert!(
                at == TOP
                    || node.children > 0
                    || node.dirs > 0
                    || ledger.taken.contains_key(&at)
                    || ledger.leaving.contains_key(&at)
                    || ledger.wanting.contains_key(&at),
                "a path nothing holds: {}",
                paths.path(at)
            );
            if at != TOP {
                assert_eq!(paths.ids.get(&node.key[..]), Some(&at));
            }
        }
        assert_eq!(paths.ids.len(), paths.held().count());
    }

    // A small deterministic generator, so every seed replays one sequence.
    struct Draw(u64);

    impl Draw {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: usize) -> usize {
            (self.next() % bound as u64) as usize
        }

        fn pick<'a, T>(&mut self, from: &'a [T]) -> &'a T {
            &from[self.below(from.len())]
        }
    }

    // Names that collide in case, in normalisation, as a file and a
    // directory of one path, through a directory a path runs through, and
    // with the suffixed forms the ledger hands out.
    const NAMES: [&str; 24] = [
        "a.bin",
        "A.BIN",
        "a (1).bin",
        "a (2).bin",
        "Caf\u{e9}.mkv",
        "Cafe\u{301}.mkv",
        "CAF\u{c9} (1).mkv",
        "dir",
        "Dir/",
        "dir/",
        "dir (1)",
        "dir/a.bin",
        "DIR/A.BIN",
        "dir/sub/",
        "dir/sub/x.bin",
        "dir/sub",
        "x.bin",
        "x (1).bin",
        "sub/x.bin",
        "sub/",
        "SUB",
        "noext",
        "noext (1)",
        "other.bin",
    ];

    // Names that share directories at every depth, are one another's
    // directories as files, and differ only in case or normalisation.
    const DEEP_NAMES: [&str; 24] = [
        "d/e/f/g.bin",
        "D/E/F/G.BIN",
        "d/e/f/g.bin/",
        "d/e/f/g.bin/h",
        "d/e/f/",
        "d/e/f",
        "d/e/f (1)",
        "d/e",
        "d/e/",
        "d/e (1)/f/g.bin",
        "d",
        "d/",
        "d (1)",
        "d/x/y/z/w/v/u/t/s.bin",
        "d/x/y/z/w/v/u/t/s.bin/r",
        "d/x/y/z/w/v/u/t",
        "d/x/y/z/w/",
        "D/X/Y/Z/W/V/U/T/S (1).BIN",
        "d/caf\u{e9}/a.bin",
        "D/CAFE\u{301}/A.BIN",
        "d/caf\u{e9}",
        "d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z.bin",
        "d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z.bin/",
        "d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/",
    ];

    #[test]
    fn the_ledger_answers_as_a_scan_of_every_entry_would() {
        answers_as_the_reference(&NAMES);
    }

    #[test]
    fn the_ledger_answers_as_the_reference_for_deep_names() {
        answers_as_the_reference(&DEEP_NAMES);
    }

    fn answers_as_the_reference(names: &[&str]) {
        answers_as_the_reference_among(names, 4);
    }

    // The most members one change moved on, how many changes moved a file
    // off a directory's path and that file moved another on, and the most
    // steps that ran at once, over every seed.
    #[derive(Default)]
    struct Chains {
        longest: usize,
        through_directory: usize,
        deepest: usize,
    }

    fn answers_as_the_reference_among(names: &[&str], members: u32) -> Chains {
        let mut chains = Chains::default();
        let holders: Vec<Holder> = [ROOT, STORE, NodeId(2), NodeId(3)]
            .into_iter()
            .flat_map(|node| (0..members).map(move |member| (node, MemberId(member))))
            .collect();
        for seed in [1u64, 7, 42, 1_234_567, 0x9e37_79b9_7f4a_7c15, 31_337] {
            let mut draw = Draw(seed);
            let mut ledger = NameLedger::new();
            let mut reference = reference::Ledger::default();
            // Names members moved off, which the writer later vacates.
            let mut left: Vec<String> = Vec::new();
            let mut moves = 0;
            for step in 0..4_000 {
                let holder = *draw.pick(&holders);
                let position = at(&(0..1 + draw.below(3))
                    .map(|_| draw.below(3) as u32)
                    .collect::<Vec<_>>());
                let (mut ours, mut theirs) = (Vec::new(), Vec::new());
                let op = draw.below(16);
                match op {
                    0..=5 => {
                        let wanted = *draw.pick(names);
                        assert_eq!(
                            ledger.allocate(holder, position.clone(), wanted, &mut ours),
                            reference.allocate(holder, position, wanted, &mut theirs),
                            "seed {seed} step {step}"
                        );
                    }
                    6 | 7 if ledger.given(holder).is_none() => {
                        let (wanted, held) = (*draw.pick(names), *draw.pick(names));
                        ledger.restore(holder, position.clone(), wanted, held);
                        reference.restore(holder, position, wanted, held);
                    }
                    6 | 7 => {
                        ledger.release(holder, &mut ours);
                        reference.release(holder, &mut theirs);
                    }
                    8 => {
                        let name = *draw.pick(names);
                        ledger.leaving(holder, name);
                        reference.leaving(holder, name);
                    }
                    9..=11 if !left.is_empty() => {
                        let name = left.swap_remove(draw.below(left.len()));
                        ledger.vacated(&name, &mut ours);
                        reference.vacated(&name, &mut theirs);
                    }
                    9 | 10 => {
                        let name = *draw.pick(names);
                        ledger.vacated(name, &mut ours);
                        reference.vacated(name, &mut theirs);
                    }
                    11 => {
                        ledger.settle(&mut ours);
                        reference.settle(&mut theirs);
                    }
                    _ => {
                        ledger.release(holder, &mut ours);
                        reference.release(holder, &mut theirs);
                    }
                }
                assert_eq!(ours, theirs, "seed {seed} step {step} op {op}");
                assert_indexes_hold(&ledger, reference.dirs());
                moves += ours.len();
                left.extend(ours.into_iter().map(|moved| moved.old));
                for holder in &holders {
                    assert_eq!(
                        ledger.given(*holder),
                        reference.given(*holder),
                        "seed {seed} step {step} op {op} holder {holder:?}"
                    );
                    assert_eq!(
                        ledger.held_off(*holder),
                        reference.held_off(*holder),
                        "seed {seed} step {step} op {op} holder {holder:?}"
                    );
                }
            }
            // The sequence moved members and handed freed names on.
            assert!(moves > 0 && ledger.offered > 0, "seed {seed}");
            chains.longest = chains.longest.max(ledger.longest);
            chains.through_directory += ledger.through_directory;
            chains.deepest = chains.deepest.max(ledger.deepest);
        }
        chains
    }

    // Names that one stem's suffixed forms collide on, as files, as
    // directories and as directories other names run through, so a member
    // that takes one moves others on in chains.
    const CHAIN_NAMES: [&str; 24] = [
        "c.bin",
        "C.BIN",
        "c.bin",
        "c (1).bin",
        "c (2).bin",
        "c (3).bin",
        "c (4).bin",
        "c",
        "c/",
        "c (1)",
        "c (1)/",
        "c (2)",
        "c (2)/",
        "c/c.bin",
        "c (1)/c.bin",
        "c (2)/c (1).bin",
        "c.bin/",
        "c.bin/c",
        "c (1).bin/",
        "c (1).bin/c (1)",
        "c (2).bin/x",
        "d/c.bin",
        "d/c (1).bin/",
        "d",
    ];

    #[test]
    fn the_ledger_answers_as_the_reference_for_chains_of_moves() {
        let chains = answers_as_the_reference_among(&CHAIN_NAMES, 16);
        // Members moved one another on in chains, and a directory moved a
        // file on whose next name moved another, at one depth throughout.
        assert!(chains.longest >= 4, "{}", chains.longest);
        assert!(chains.through_directory > 0);
        assert_eq!(chains.deepest, 1);
    }

    // A directory takes the path of a file, which moves on to its first
    // suffix and moves the member holding it on in turn.
    #[test]
    fn a_directory_moves_a_file_on_whose_next_name_moves_another() {
        let (first, second, directory) = (
            (ROOT, MemberId(1)),
            (STORE, MemberId(0)),
            (NodeId(2), MemberId(0)),
        );
        let order = [
            (first, at(&[1]), "c"),
            (second, at(&[0, 0]), "c"),
            (directory, at(&[0, 0, 0]), "c/"),
        ];
        let mut ledger = NameLedger::new();
        let mut reference = reference::Ledger::default();
        let (mut ours, mut theirs) = (Vec::new(), Vec::new());
        for (holder, position, wanted) in &order {
            ledger.allocate(*holder, position.clone(), wanted, &mut ours);
            reference.allocate(*holder, position.clone(), wanted, &mut theirs);
        }
        assert_eq!(ours, theirs);
        assert_eq!(
            ours,
            vec![
                Moved {
                    holder: second,
                    old: "c (1)".into(),
                    new: "c (2)".into(),
                },
                Moved {
                    holder: first,
                    old: "c".into(),
                    new: "c (1)".into(),
                },
            ]
        );
        assert_eq!(ledger.given(first), Some("c (1)"));
        assert_eq!(ledger.given(second), Some("c (2)"));
        assert_eq!(ledger.given(directory), Some("c/"));
        assert_eq!(ledger.deepest, 1);
    }

    // Both ledgers, driven alike.
    trait Drive {
        fn allocate(&mut self, holder: Holder, at: Position, wanted: &str, moved: &mut Vec<Moved>);
        fn restore(&mut self, holder: Holder, at: Position, wanted: &str, held: &str);
        fn release(&mut self, holder: Holder, moved: &mut Vec<Moved>);
    }

    impl Drive for NameLedger {
        fn allocate(&mut self, holder: Holder, at: Position, wanted: &str, moved: &mut Vec<Moved>) {
            NameLedger::allocate(self, holder, at, wanted, moved);
        }
        fn restore(&mut self, holder: Holder, at: Position, wanted: &str, held: &str) {
            NameLedger::restore(self, holder, at, wanted, held);
        }
        fn release(&mut self, holder: Holder, moved: &mut Vec<Moved>) {
            NameLedger::release(self, holder, moved);
        }
    }

    impl Drive for reference::Ledger {
        fn allocate(&mut self, holder: Holder, at: Position, wanted: &str, moved: &mut Vec<Moved>) {
            reference::Ledger::allocate(self, holder, at, wanted, moved);
        }
        fn restore(&mut self, holder: Holder, at: Position, wanted: &str, held: &str) {
            reference::Ledger::restore(self, holder, at, wanted, held);
        }
        fn release(&mut self, holder: Holder, moved: &mut Vec<Moved>) {
            reference::Ledger::release(self, holder, moved);
        }
    }

    // The `k`th member of a chain, ranking below every one before it and
    // below the root member that sets the chain off.
    fn link(k: usize) -> (Holder, Position) {
        let k = u32::try_from(k).unwrap();
        ((STORE, MemberId(k)), at(&[0, k]))
    }

    const TOP_MEMBER: Holder = (ROOT, MemberId(0));

    // How a chain of `n` members is set off, and the moves it makes.
    #[derive(Clone, Copy)]
    enum Chain {
        // Every member wants one name and takes a suffix of it; a member
        // that outranks them all then takes the name.
        OneName,
        // Each member holds the name the one before it wants; a member that
        // outranks them all takes the first one's name.
        Outranked,
        // Each member's file holds the path of the directory the one
        // before it wants; a directory takes the first one's path.
        ThroughDirectories,
        // Each member waits for a path the name of the one before it runs
        // through; the name that holds the first path is released.
        Waiting,
    }

    fn run_chain(ledger: &mut impl Drive, chain: Chain, n: usize) -> Vec<Moved> {
        let mut moved = Vec::new();
        for k in 0..n {
            let (holder, position) = link(k);
            match chain {
                Chain::OneName => ledger.allocate(holder, position, "x.bin", &mut moved),
                Chain::Outranked => ledger.restore(
                    holder,
                    position,
                    &format!("f{}.bin", k + 1),
                    &format!("f{k}.bin"),
                ),
                Chain::ThroughDirectories => ledger.restore(
                    holder,
                    position,
                    &format!("f{}/x.bin", k + 1),
                    &format!("f{k}"),
                ),
                Chain::Waiting => ledger.restore(
                    holder,
                    position,
                    &format!("w{k}"),
                    &format!("w{}/x.bin", k + 1),
                ),
            }
        }
        // Building it moved each member on to the next suffix as it came.
        assert_eq!(moved, Vec::new());
        match chain {
            Chain::OneName => ledger.allocate(TOP_MEMBER, at(&[0]), "x.bin", &mut moved),
            Chain::Outranked => ledger.allocate(TOP_MEMBER, at(&[0]), "f0.bin", &mut moved),
            Chain::ThroughDirectories => {
                ledger.allocate(TOP_MEMBER, at(&[0]), "f0/", &mut moved);
            }
            Chain::Waiting => {
                ledger.restore(TOP_MEMBER, at(&[0]), "w0/x.bin", "w0/x.bin");
                ledger.release(TOP_MEMBER, &mut moved);
            }
        }
        moved
    }

    // The name the `k`th member of a chain ends with, and the one it left.
    fn chain_names(chain: Chain, k: usize) -> (String, String) {
        let suffix = |k: usize| match k {
            0 => "x.bin".to_owned(),
            k => format!("x ({k}).bin"),
        };
        match chain {
            Chain::OneName => (suffix(k + 1), suffix(k)),
            Chain::Outranked => (format!("f{}.bin", k + 1), format!("f{k}.bin")),
            Chain::ThroughDirectories => (format!("f{}/x.bin", k + 1), format!("f{k}")),
            Chain::Waiting => (format!("w{k}"), format!("w{}/x.bin", k + 1)),
        }
    }

    const CHAINS: [Chain; 4] = [
        Chain::OneName,
        Chain::Outranked,
        Chain::ThroughDirectories,
        Chain::Waiting,
    ];

    // However many members a chain moves on, it runs at one depth, and
    // gives every member the name and the move the reference does.
    #[test]
    fn a_chain_of_members_moved_on_runs_at_one_depth() {
        const N: usize = 1_000;
        for chain in CHAINS {
            let mut ledger = NameLedger::new();
            let mut reference = reference::Ledger::default();
            let ours = run_chain(&mut ledger, chain, N);
            let theirs = run_chain(&mut reference, chain, N);
            assert_eq!(ours, theirs);
            for k in 0..N {
                let (holder, _) = link(k);
                assert_eq!(ledger.given(holder), reference.given(holder));
            }
            assert_chain(&ledger, chain, &ours, N);
            assert_eq!(
                ledger.longest,
                N * usize::from(!matches!(chain, Chain::Waiting))
            );
        }
    }

    // Every member of the chain moved once, to the name it ends with, in an
    // order that moves a file off a name before another moves onto it.
    fn assert_chain(ledger: &NameLedger, chain: Chain, moved: &[Moved], n: usize) {
        assert_eq!(moved.len(), n);
        for (at, report) in moved.iter().enumerate() {
            // A member moved on by the one above it is reported first.
            let k = match chain {
                Chain::OneName | Chain::Outranked => n - 1 - at,
                Chain::ThroughDirectories | Chain::Waiting => at,
            };
            let (name, old) = chain_names(chain, k);
            assert_eq!(ledger.given(link(k).0), Some(name.as_str()));
            assert_eq!(
                report,
                &Moved {
                    holder: link(k).0,
                    old,
                    new: name,
                }
            );
        }
        assert_eq!(ledger.deepest, 1);
    }

    // A chain far longer than any stack could hold a call per member of
    // moves every member on in one pass at one depth. Every member wanting
    // one name probes each suffix the ones before it hold, so that chain
    // stays at the size above.
    #[test]
    fn a_long_chain_of_members_moved_on_runs_at_one_depth() {
        const N: usize = 100_000;
        for chain in [Chain::Outranked, Chain::ThroughDirectories, Chain::Waiting] {
            let mut ledger = NameLedger::new();
            let moved = run_chain(&mut ledger, chain, N);
            assert_chain(&ledger, chain, &moved, N);
        }
    }

    // A name runs through as many directories as a long name can hold, each
    // one part deep. The ledger keeps one path per part, the directory's id
    // and the part, and reads each part a bounded number of times: what it
    // keeps and reads grows with the name, never with the name times its
    // depth, which as whole paths would be 64 GiB here.
    #[test]
    fn a_deep_name_keeps_and_reads_each_part_once_per_pass() {
        const DEPTH: usize = 262_144;
        let deep = "a/".repeat(DEPTH);
        let name = format!("{deep}x");
        // A part's key: the id of its directory, then the part.
        let part = size_of::<PathId>() + 1;
        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 0, &name), name);
        assert_eq!(ledger.paths.held().count(), DEPTH + 1);
        assert_eq!(ledger.paths.bytes(), (DEPTH + 1) * part);
        // Placing the name, holding it and running through its directories
        // each read every part once.
        let read = ledger.paths.probed();
        assert!(read <= 3 * (DEPTH + 1) * part, "{read}");

        // A file beside it shares every directory, and a file that wants
        // the path of one of them takes a suffix.
        assert_eq!(
            root(&mut ledger, 1, &format!("{deep}y")),
            format!("{deep}y")
        );
        assert_eq!(ledger.paths.held().count(), DEPTH + 2);
        assert_eq!(root(&mut ledger, 2, &deep[..5]), "a/a/a (1)");
        assert!(ledger.paths.probed() - read <= 8 * (DEPTH + 1) * part);
        let held = |path: &str| ledger.paths.find(path).map(|at| ledger.paths.dirs(at));
        assert_eq!(held("a/a"), Some(3));
        assert_eq!(held("a/a/a"), Some(2));
        assert_eq!(held(&deep[..deep.len() - 1]), Some(2));

        // Given up, the names let go of every path they alone held, and the
        // file that wanted a directory's path
        // takes it on the way.
        let read = ledger.paths.probed();
        let mut moved = Vec::new();
        ledger.release((ROOT, MemberId(1)), &mut moved);
        ledger.release((ROOT, MemberId(0)), &mut moved);
        assert_eq!(
            moved,
            vec![Moved {
                holder: (ROOT, MemberId(2)),
                old: "a/a/a (1)".into(),
                new: "a/a/a".into(),
            }]
        );
        // Each release finds its file's path, runs through its directories
        // and offers the freed path: three reads of each name, and a few
        // parts for the short name that moves.
        let released = ledger.paths.probed() - read;
        assert!(released <= (6 * (DEPTH + 1) + 32) * part, "{released}");
        // "a", "a/a", the name it holds and the one it leaves.
        assert_eq!(ledger.paths.held().count(), 4);
        ledger.release((ROOT, MemberId(2)), &mut moved);
        assert_eq!(ledger.paths.held().count(), 0);
        assert_eq!(ledger.paths.bytes(), 0);
        assert!(ledger.taken.is_empty() && ledger.leaving.is_empty());
        assert!(ledger.wanting.is_empty() && ledger.left_by.is_empty());
        assert_eq!(ledger.paths.ids.len(), 0);
    }
}
