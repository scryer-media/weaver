// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Output names. Every namespace of a job writes into the one working
// directory, so the tree holds one ledger for all of them and allocates
// every name a member is written under; no object writes a path it chose
// itself. Two members that want one name both get written: the one higher
// in the tree keeps it and the other gets a suffix, the same way the
// download folder suffixes a job whose folder already exists. Which one is
// higher is a matter of position alone, never of arrival, so a restart or
// a different fetch order gives the same names.

use std::cmp::Ordering;
use std::collections::HashMap;

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
}

#[derive(Debug, Default)]
pub struct NameLedger {
    // Allocated name, as its path on disk, to the member holding it.
    taken: HashMap<String, Holder>,
    // A name, as its path on disk, a member was moved off whose file still
    // sits there: it is free again only once that file has moved, so no
    // other member is given a name a file is still written under.
    leaving: HashMap<String, Holder>,
    entries: HashMap<Holder, Entry>,
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
    // name returns the one held.
    pub fn allocate(
        &mut self,
        holder: Holder,
        position: Position,
        wanted: &str,
        moved: &mut Vec<Moved>,
    ) -> String {
        let wanted = confined(wanted);
        let old = match self.entries.get(&holder) {
            Some(entry) if entry.wanted.eq_ignore_ascii_case(&wanted) => {
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
        if let Some(old) = old.filter(|old| *old != name) {
            if key(&old) != key(&name) {
                self.taken.remove(&key(&old));
                self.leaving.insert(key(&old), holder);
            }
            moved.push(Moved {
                holder,
                old,
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
        name
    }

    // A holder comes back with the name it had, as when a job is rebuilt.
    // Restored in rank order, a set of names that were distinct are all
    // given back as they were; a name that is not free is allocated, and
    // whatever moves is not reported, since nothing was written under it.
    pub fn restore(&mut self, holder: Holder, position: Position, wanted: &str, held: &str) {
        let held = confined(held);
        let rank = (position, holder);
        let name = match self.taken.entry(key(&held)) {
            std::collections::hash_map::Entry::Occupied(_) => {
                self.place(holder, &rank, &confined(wanted), &mut Vec::new())
            }
            std::collections::hash_map::Entry::Vacant(free) => {
                free.insert(holder);
                held
            }
        };
        self.entries.insert(
            holder,
            Entry {
                rank,
                wanted: confined(wanted),
                name,
            },
        );
    }

    // A holder leaves the ledger. No name it held or was moving off is kept
    // for it any more: the holder that wanted each one, the highest ranked
    // when several did, takes it now, and its move is pushed to `moved`, as
    // when the name is vacated. Whether a file still sits at the name on
    // disk is not the ledger's to say; the move waits for that.
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
        if let Some(entry) = self.entries.remove(&holder) {
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
    }

    // The file written under `name` has moved off it: the name is free.
    // The holder that wanted it and was given another while it was left,
    // the highest ranked when several were, takes it now, and its move is
    // pushed to `moved`: a name shut while a file leaves it delays a
    // member's name, never changes it.
    pub fn vacated(&mut self, name: &str, moved: &mut Vec<Moved>) {
        let name = key(name);
        if self.leaving.remove(&name).is_some() {
            self.take_wanted(name, moved);
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
            if !self.leaving.contains_key(&name) {
                self.take_wanted(name, moved);
            }
        }
    }

    // The highest ranked holder that wants the free name `name` and holds
    // another takes it.
    fn take_wanted(&mut self, name: String, moved: &mut Vec<Moved>) {
        if self.taken.contains_key(&name) {
            return;
        }
        let Some((holder, wanted)) = self
            .entries
            .iter()
            .filter(|(_, entry)| key(&entry.wanted) == name && key(&entry.name) != name)
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
        moved.push(Moved {
            holder,
            old,
            new: wanted,
        });
    }

    // A restored holder's file still sits at `name`, which it is moving
    // off: the name stays shut until it has, unless another holder holds it.
    pub fn leaving(&mut self, holder: Holder, name: &str) {
        let name = key(name);
        if !self.taken.contains_key(&name) {
            self.leaving.insert(name, holder);
        }
    }

    pub fn given(&self, holder: Holder) -> Option<&str> {
        self.entries.get(&holder).map(|entry| entry.name.as_str())
    }

    pub fn clear(&mut self) {
        self.taken.clear();
        self.leaving.clear();
        self.entries.clear();
    }

    // The first of `wanted` and its suffixed forms that `holder` may take:
    // one that is free, its own, or held by a member it outranks, which is
    // moved on to a name of its own.
    fn place(
        &mut self,
        holder: Holder,
        rank: &(Position, Holder),
        wanted: &str,
        moved: &mut Vec<Moved>,
    ) -> String {
        let (stem, extension) = split_extension(wanted);
        for n in 0u32.. {
            let candidate = match n {
                0 => wanted.to_owned(),
                n => format!("{stem} ({n}){extension}"),
            };
            let key = key(&candidate);
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

    // `other` lost its name to a member that outranks it and takes the next
    // name it may. Its own move is reported after whatever it displaced in
    // turn, so the moves apply in order.
    fn displace(&mut self, other: Holder, moved: &mut Vec<Moved>) {
        let Some(entry) = self.entries.remove(&other) else {
            return;
        };
        let name = self.place(other, &entry.rank, &entry.wanted, moved);
        moved.push(Moved {
            holder: other,
            old: entry.name,
            new: name.clone(),
        });
        self.entries.insert(other, Entry { name, ..entry });
    }
}

// The directory of the working directory the job's own files live in:
// scratch, repair and envelope files. No output is written under it.
pub const INTERNAL_DIR: &str = ".weaver-chunks";

// The directory beside it that a process's download caches live in for the
// run they serve. No checkpoint names what is there, and no output is
// written under it either.
pub const CACHE_DIR: &str = ".weaver-cache";

// A name an archive gave, kept inside the folder it is written under. This
// is the one confinement every output name passes: the leaf that writes it
// only asserts what is decided here. Either separator splits the name, and
// empty, `.` and `..` parts are dropped, so a path can neither climb out nor
// start at the root. A NUL, which no file system takes in a name, becomes
// `_`. A part spelled like a drive (`C:`) is an ordinary directory where
// paths have no drives; on Windows, where a colon in any part names a drive
// (`C:`, `C:foo`) or a file's stream, every colon becomes `_`. A path that starts at the internal directory, or at the
// cache directory beside it, starts at that directory's first suffixed name
// instead. A trailing separator, which names a directory, is kept.
fn confined(wanted: &str) -> String {
    let mut parts: Vec<String> = wanted
        .split(['/', '\\'])
        .filter(|part| !part.is_empty() && *part != "." && *part != "..")
        .map(confined_part)
        .collect();
    match parts.first_mut() {
        None => return "_".to_owned(),
        Some(first) => {
            if let Some(reserved) = [INTERNAL_DIR, CACHE_DIR]
                .into_iter()
                .find(|reserved| first.eq_ignore_ascii_case(reserved))
            {
                *first = format!("{reserved} (1)");
            }
        }
    }
    let mut name = parts.join("/");
    if wanted.ends_with(['/', '\\']) {
        name.push('/');
    }
    name
}

// One part of a confined name, as the file system takes it.
fn confined_part(part: &str) -> String {
    let part = part.replace('\0', "_");
    if cfg!(windows) {
        part.replace(':', "_")
    } else {
        part
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
        assert_eq!(root(&mut ledger, 3, ".weaver-chunks"), ".weaver-chunks (1)");
        assert_eq!(
            root(&mut ledger, 4, "dir/.weaver-chunks/c.bin"),
            "dir/.weaver-chunks/c.bin"
        );
        assert_eq!(
            root(&mut ledger, 5, ".Weaver-Cache/d.bin"),
            ".weaver-cache (1)/d.bin"
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

    #[test]
    fn a_second_directory_of_one_name_is_suffixed_before_its_separator() {
        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 1, "docs/"), "docs/");
        assert_eq!(root(&mut ledger, 2, "docs/"), "docs (1)/");
        assert_eq!(root(&mut ledger, 3, "v1.0\\"), "v1.0/");
        assert_eq!(root(&mut ledger, 4, "v1.0/"), "v1.0 (1)/");
        assert_eq!(root(&mut ledger, 5, "a.d/noext"), "a.d/noext");
        assert_eq!(root(&mut ledger, 6, "a.d/noext"), "a.d/noext (1)");
    }

    #[test]
    fn a_directory_and_a_file_of_one_name_never_share_its_path() {
        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 1, "folder"), "folder");
        assert_eq!(root(&mut ledger, 2, "folder/"), "folder (1)/");

        // Whichever arrives first, the one that ranks higher keeps the name.
        let mut ledger = NameLedger::new();
        assert_eq!(root(&mut ledger, 2, "folder/"), "folder/");
        let mut moved = Vec::new();
        let first = (ROOT, MemberId(1));
        assert_eq!(
            ledger.allocate(first, at(&[1]), "folder", &mut moved),
            "folder"
        );
        assert_eq!(
            moved,
            vec![Moved {
                holder: (ROOT, MemberId(2)),
                old: "folder/".into(),
                new: "folder (1)/".into(),
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
}
