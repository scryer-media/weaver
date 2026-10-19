// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file cut into numbered pieces, the way a split 7z or a split stream is
// posted: `name.001`, `name.002`, … The number is the name's last
// extension, all digits, and a set counts from its first piece's own
// number, whatever that is: `.001`, `.10` and `.100` alike, so a set renamed
// to numbers of its own chains as a plain one does. Only the first piece
// says what the set is; the others carry no signature, so their names are
// all that places them. Case never matters.

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

    pub(crate) fn same_set(&self, other: &Piece) -> bool {
        self.stem == other.stem
    }

    pub(crate) fn number(&self) -> u32 {
        self.number
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

    #[test]
    fn a_piece_counts_from_the_first_pieces_own_number() {
        assert_eq!(index("set.7z.001", "set.7z.001"), Some(0));
        assert_eq!(index("set.7z.001", "SET.7Z.003"), Some(2));
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
