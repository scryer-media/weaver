// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file cut into numbered pieces, the way a split 7z or a split stream is
// posted: `name.001`, `name.002`, … The number is the name's last
// extension, all digits. A piece's place counts from one: the split store
// names `.001` its first piece, and `.10` or `.100` is never a first piece.
// Case never matters.

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Piece {
    stem: String,
    number: u32,
}

impl Piece {
    // Where this piece falls in the set `first` opens, counted from zero:
    // `None` for a piece of another set, or one numbered before it.
    pub(crate) fn index_from(&self, first: &Piece) -> Option<u32> {
        (self.stem == first.stem)
            .then(|| self.number.checked_sub(first.number))
            .flatten()
    }

    // The piece of my set that carries `number`.
    pub(crate) fn numbered(&self, number: u32) -> Piece {
        Piece {
            stem: self.stem.clone(),
            number,
        }
    }
}

pub(crate) fn parse(name: &str) -> Option<Piece> {
    let lower = name.to_ascii_lowercase();
    let (stem, digits) = lower.rsplit_once('.')?;
    if stem.is_empty() || digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(Piece {
        stem: stem.to_owned(),
        number: digits.parse().ok()?,
    })
}

// The name with its piece number taken off, as it was spelled: what the
// pieces were cut from. `None` for a name that numbers no piece.
pub(crate) fn whole(name: &str) -> Option<&str> {
    parse(name)?;
    name.rsplit_once('.').map(|(stem, _)| stem)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(first: &str, name: &str) -> Option<u32> {
        parse(name)?.index_from(&parse(first)?)
    }

    // Counted from the first piece the helper is given: the split store
    // gives it `.001`, so `.10` is its tenth piece.
    #[test]
    fn a_piece_counts_from_the_first_piece_its_owner_names() {
        assert_eq!(index("set.7z.001", "set.7z.001"), Some(0));
        assert_eq!(index("set.7z.001", "SET.7Z.003"), Some(2));
        assert_eq!(index("c0ffee.001", "c0ffee.10"), Some(9));
        assert_eq!(index("c0ffee.001", "c0ffee.100"), Some(99));
        assert_eq!(index("c0ffee.10", "c0ffee.12"), Some(2));
        assert_eq!(index("c0ffee.100", "c0ffee.101"), Some(1));
        assert_eq!(index("c0ffee.10", "c0ffee.9"), None);
        assert_eq!(index("c0ffee.10", "beef.11"), None);
    }

    #[test]
    fn a_name_without_a_numbered_extension_is_no_piece() {
        assert_eq!(parse("set.7z"), None);
        assert_eq!(parse(".001"), None);
        assert_eq!(parse("set.7z.0a1"), None);
        assert_eq!(whole("Ember.bin.xz.001"), Some("Ember.bin.xz"));
        assert_eq!(whole("ember.bin.xz"), None);
    }
}
