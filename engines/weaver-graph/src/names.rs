// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Output names. The namespace allocates every name a member is written
// under; no object writes a path it chose itself. Two members that want one
// name both get written: the first keeps it, the second gets a suffix, the
// same way the download folder suffixes a job whose folder already exists.

use std::collections::HashMap;

use crate::member::MemberId;

#[derive(Debug, Default)]
pub struct NameLedger {
    // Allocated name, lower-cased for case-insensitive file systems, to the
    // member holding it.
    taken: HashMap<String, MemberId>,
    // Member to the name it was given.
    given: HashMap<MemberId, String>,
}

impl NameLedger {
    pub fn new() -> Self {
        Self::default()
    }

    // The name `member` may write under for `wanted`. Idempotent: asking
    // again for the same member returns the same name. A member that asks
    // for a different name moves to it and releases the old one.
    pub fn allocate(&mut self, member: MemberId, wanted: &str) -> String {
        if let Some(held) = self.given.get(&member)
            && held.eq_ignore_ascii_case(wanted)
        {
            return held.clone();
        }
        self.release(member);
        let name = self.first_free(wanted, member);
        self.taken.insert(name.to_ascii_lowercase(), member);
        self.given.insert(member, name.clone());
        name
    }

    pub fn release(&mut self, member: MemberId) {
        if let Some(name) = self.given.remove(&member) {
            self.taken.remove(&name.to_ascii_lowercase());
        }
    }

    pub fn given(&self, member: MemberId) -> Option<&str> {
        self.given.get(&member).map(String::as_str)
    }

    fn first_free(&self, wanted: &str, member: MemberId) -> String {
        let free = |candidate: &str| {
            self.taken
                .get(&candidate.to_ascii_lowercase())
                .is_none_or(|holder| *holder == member)
        };
        if free(wanted) {
            return wanted.to_owned();
        }
        let (stem, extension) = split_extension(wanted);
        (1u32..)
            .map(|n| format!("{stem} ({n}){extension}"))
            .find(|candidate| free(candidate))
            .expect("an unbounded counter finds a free name")
    }
}

// "subs.srt" -> ("subs", ".srt"); "archive.tar.gz" -> ("archive.tar", ".gz");
// ".hidden" -> (".hidden", ""); "noext" -> ("noext", "").
fn split_extension(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(dot) if dot > 0 => name.split_at(dot),
        _ => (name, ""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_second_member_that_wants_a_name_gets_a_suffix() {
        let mut ledger = NameLedger::new();
        assert_eq!(ledger.allocate(MemberId(1), "subs.srt"), "subs.srt");
        assert_eq!(ledger.allocate(MemberId(2), "subs.srt"), "subs (1).srt");
        assert_eq!(ledger.allocate(MemberId(3), "SUBS.SRT"), "SUBS (2).SRT");
        assert_eq!(ledger.allocate(MemberId(1), "subs.srt"), "subs.srt");
    }

    #[test]
    fn a_released_name_is_free_again() {
        let mut ledger = NameLedger::new();
        ledger.allocate(MemberId(1), "a.bin");
        ledger.release(MemberId(1));
        assert_eq!(ledger.allocate(MemberId(2), "a.bin"), "a.bin");
    }

    #[test]
    fn a_rename_moves_the_allocation() {
        let mut ledger = NameLedger::new();
        ledger.allocate(MemberId(1), "3f9a.bin");
        assert_eq!(
            ledger.allocate(MemberId(1), "show.part01.rar"),
            "show.part01.rar"
        );
        assert_eq!(ledger.allocate(MemberId(2), "3f9a.bin"), "3f9a.bin");
    }

    #[test]
    fn names_without_an_extension_suffix_at_the_end() {
        let mut ledger = NameLedger::new();
        ledger.allocate(MemberId(1), ".hidden");
        ledger.allocate(MemberId(2), "noext");
        assert_eq!(ledger.allocate(MemberId(3), ".hidden"), ".hidden (1)");
        assert_eq!(ledger.allocate(MemberId(4), "noext"), "noext (1)");
    }
}
