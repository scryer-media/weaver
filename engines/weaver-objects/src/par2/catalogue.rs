// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the usable sets describe, across all of them: each described file's
// front, its name folded to one case, and its length, under the place of
// the set that describes it. Binding asks it which sets a member could be a
// file of, so a set that describes nothing like the member is never asked.
// A set is filed when its index becomes usable and unfiled before its index
// changes, so the catalogue costs what changed, never what stands.

use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hash, Hasher, RandomState};

use par2_rs::{FileDescription, FileId, Par2FileSet};
use weaver_graph::VolumeIndex;

use super::sets::{Front, Length};

// A set's place in the order: where its first packet was found, then its
// key.
pub(super) type Place = ((VolumeIndex, u64), [u8; 16]);

const FIRST: Place = ((VolumeIndex(0), 0), [0; 16]);
const LAST: Place = ((VolumeIndex(u32::MAX), u64::MAX), [u8::MAX; 16]);

#[derive(Default)]
pub(super) struct Catalogue {
    // How many usable sets are filed, any of whose files a member's front
    // may name.
    usable: usize,
    // Each entry counts the set's files under it.
    fronts: BTreeMap<(Front, Place), u32>,
    // How many sets describe each front, and how many fronts more than one
    // set describes: only then can a member move from one set to another.
    sharing: BTreeMap<Front, u32>,
    shared: usize,
    names: BTreeMap<(u64, Place), u32>,
    lengths: BTreeMap<(u64, Place), u32>,
    // Folded names are filed by a keyed hash, so posted names cannot be
    // chosen to crowd one entry; a set a hash finds is still asked whether
    // the name is one it describes.
    hasher: RandomState,
    // Whether a set was filed or unfiled, or the catalogue cleared, since
    // binding last asked: a posting may then answer to other sets.
    changed: bool,
}

impl Catalogue {
    pub(super) fn file(&mut self, place: Place, index: &Par2FileSet) {
        self.changed = true;
        self.usable += 1;
        for description in index.files.values() {
            self.file_description(place, description);
        }
        for description in index
            .recovery_file_ids
            .iter()
            .filter_map(|file| index.file_description(file))
        {
            *self.lengths.entry((description.length, place)).or_default() += 1;
        }
    }

    // A filed set came to describe `files` besides, none of them one it
    // protects: as unfiling it and filing it again, at their cost alone.
    pub(super) fn describe(&mut self, place: Place, index: &Par2FileSet, files: &[FileId]) {
        self.changed = true;
        for description in files.iter().filter_map(|file| index.file_description(file)) {
            self.file_description(place, description);
        }
    }

    fn file_description(&mut self, place: Place, description: &FileDescription) {
        let front = Front::described(description.length, description.hash_16k);
        let count = self.fronts.entry((front, place)).or_default();
        *count += 1;
        if *count == 1 {
            let sets = self.sharing.entry(front).or_default();
            *sets += 1;
            self.shared += usize::from(*sets == 2);
        }
        let name = self.folded(&description.filename);
        *self.names.entry((name, place)).or_default() += 1;
    }

    pub(super) fn unfile(&mut self, place: Place, index: &Par2FileSet) {
        debug_assert!(self.usable > 0);
        self.changed = true;
        self.usable = self.usable.saturating_sub(1);
        for description in index.files.values() {
            let front = Front::described(description.length, description.hash_16k);
            if release(&mut self.fronts, (front, place))
                && let Some(sets) = self.sharing.get_mut(&front)
            {
                self.shared -= usize::from(*sets == 2);
                *sets -= 1;
                if *sets == 0 {
                    self.sharing.remove(&front);
                }
            }
            let name = self.folded(&description.filename);
            release(&mut self.names, (name, place));
        }
        for description in index
            .recovery_file_ids
            .iter()
            .filter_map(|file| index.file_description(file))
        {
            release(&mut self.lengths, (description.length, place));
        }
    }

    pub(super) fn clear(&mut self) {
        self.changed = true;
        self.usable = 0;
        self.fronts.clear();
        self.sharing.clear();
        self.shared = 0;
        self.names.clear();
        self.lengths.clear();
    }

    // Whether a set was filed or unfiled, or the catalogue cleared, since
    // this was last asked.
    pub(super) fn take_changed(&mut self) -> bool {
        std::mem::take(&mut self.changed)
    }

    // Whether two usable sets describe a file of one front.
    pub(super) fn shares(&self) -> bool {
        self.shared > 0
    }

    // Whether any usable set is filed.
    pub(super) fn any(&self) -> bool {
        self.usable > 0
    }

    // The sets that describe a file whose front is `front`, or whose name
    // is `name` in either case, each once, in order.
    pub(super) fn candidates<'c>(
        &'c self,
        front: Option<Front>,
        name: &str,
    ) -> impl Iterator<Item = Place> + use<'c> {
        let mut by_front = front
            .into_iter()
            .flat_map(|front| self.fronts.range((front, FIRST)..=(front, LAST)))
            .map(|((_, place), _)| *place)
            .peekable();
        let folded = self.folded(name);
        let mut by_name = self
            .names
            .range((folded, FIRST)..=(folded, LAST))
            .map(|((_, place), _)| *place)
            .peekable();
        std::iter::from_fn(move || {
            let next = match (by_front.peek(), by_name.peek()) {
                (Some(front), Some(name)) => *front.min(name),
                (Some(front), None) => *front,
                (None, Some(name)) => *name,
                (None, None) => return None,
            };
            by_front.next_if_eq(&next);
            by_name.next_if_eq(&next);
            Some(next)
        })
    }

    // The sets that describe a file whose front is `front`, each once, in
    // order.
    pub(super) fn fronted(&self, front: Front) -> impl Iterator<Item = Place> + '_ {
        self.fronts
            .range((front, FIRST)..=(front, LAST))
            .map(|((_, place), _)| *place)
    }

    // Every set with a protected file an unnamed member of `length` could
    // be, once or more each.
    pub(super) fn fitting(&self, length: Length, mut each: impl FnMut(Place)) {
        let (from, to) = match length {
            Length::Exact(length) => (length, length),
            Length::AtMost(bound) => (0, bound),
        };
        for ((_, place), _) in self.lengths.range((from, FIRST)..=(to, LAST)) {
            each(*place);
        }
    }

    fn folded(&self, name: &str) -> u64 {
        self.hasher.hash_one(Folded(name))
    }
}

// Count one fewer under `key`; whether that was its last.
fn release<K: Ord>(entries: &mut BTreeMap<K, u32>, key: K) -> bool {
    let Some(count) = entries.get_mut(&key) else {
        return false;
    };
    *count -= 1;
    if *count == 0 {
        entries.remove(&key);
        return true;
    }
    false
}

// A name as `eq_ignore_ascii_case` compares it.
struct Folded<'a>(&'a str);

impl Hash for Folded<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
    }
}
