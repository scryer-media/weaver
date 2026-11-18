// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file cut into numbered pieces, the way a split 7z or a split stream is
// posted: `name.001`, `name.002`, …, or `name.01`, `name.02`, … The number
// is the name's last extension, two digits or more. A piece's place counts
// from the set's first: the split store names `.001` its first piece, or
// `.000` for a set numbered from zero, and `.100` is never a first piece. A
// set numbered in two digits is a set of its own: `.01` is never a piece of
// the set `.001` opens. Case never matters.

use weaver_graph::sniff::split_piece;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Piece {
    stem: String,
    number: u32,
    // Numbered in two digits, `.01` onward.
    short: bool,
}

impl Piece {
    // Where this piece falls in the set `first` opens, counted from zero:
    // `None` for a piece of another set, or one numbered before it.
    pub(crate) fn index_from(&self, first: &Piece) -> Option<u32> {
        (self.stem == first.stem && self.short == first.short)
            .then(|| self.number.checked_sub(first.number))
            .flatten()
    }

    pub(crate) fn number(&self) -> u32 {
        self.number
    }

    // The piece of my set that carries `number`.
    pub(crate) fn numbered(&self, number: u32) -> Piece {
        Piece {
            stem: self.stem.clone(),
            number,
            short: self.short,
        }
    }
}

// A name numbers a piece as the sniff names one
// (`weaver_graph::sniff::split_piece`), so every piece the store is founded
// on (`split_piece_among`) is one it numbers.
pub(crate) fn parse(name: &str) -> Option<Piece> {
    let (stem, digits) = split_piece(name)?;
    Some(Piece {
        stem: stem.to_ascii_lowercase(),
        number: digits.parse().ok()?,
        short: digits.len() == 2,
    })
}

// The name with its piece number taken off, as it was spelled: what the
// pieces were cut from. `None` for a name that numbers no piece.
pub(crate) fn whole(name: &str) -> Option<&str> {
    split_piece(name).map(|(stem, _)| stem)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(first: &str, name: &str) -> Option<u32> {
        parse(name)?.index_from(&parse(first)?)
    }

    // Counted from the first piece the helper is given: the split store
    // gives it `.001`, so `.010` is its tenth piece, and `.1000` follows
    // `.999` as 7-Zip numbers on past it.
    #[test]
    fn a_piece_counts_from_the_first_piece_its_owner_names() {
        assert_eq!(index("set.7z.001", "set.7z.001"), Some(0));
        assert_eq!(index("set.7z.001", "SET.7Z.003"), Some(2));
        assert_eq!(index("c0ffee.001", "c0ffee.010"), Some(9));
        assert_eq!(index("c0ffee.001", "c0ffee.100"), Some(99));
        assert_eq!(index("c0ffee.001", "c0ffee.1000"), Some(999));
        assert_eq!(index("c0ffee.010", "c0ffee.012"), Some(2));
        assert_eq!(index("c0ffee.100", "c0ffee.101"), Some(1));
        assert_eq!(index("c0ffee.010", "c0ffee.009"), None);
        assert_eq!(index("c0ffee.010", "beef.011"), None);
        assert_eq!(index("[71ECB6A7].mkv.001_", "[71ecb6a7].MKV.002_"), Some(1));
        assert_eq!(index("movie.mkv.01", "movie.mkv.03"), Some(2));
        assert_eq!(index("movie.mkv.00", "Movie.MKV.01"), Some(1));
    }

    // A set numbered in two digits and one numbered in three are two sets,
    // whatever their stems.
    #[test]
    fn a_two_digit_piece_is_of_no_three_digit_set() {
        assert_eq!(index("movie.mkv.001", "movie.mkv.02"), None);
        assert_eq!(index("movie.mkv.01", "movie.mkv.002"), None);
    }

    #[test]
    fn a_name_without_a_numbered_extension_is_no_piece() {
        assert_eq!(parse("set.7z"), None);
        assert_eq!(parse(".001"), None);
        assert_eq!(parse("set.7z.0a1"), None);
        assert_eq!(parse("c0ffee.1"), None);
        assert_eq!(parse("Microsoft.WindowsAppRuntime.Release%211.4.0"), None);
        assert_eq!(whole("[71ECB6A7].mkv.001_"), Some("[71ECB6A7].mkv"));
        assert_eq!(whole("Ember.bin.xz.001"), Some("Ember.bin.xz"));
        assert_eq!(whole("ember.bin.xz"), None);
    }
}
