// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use core::{cmp::Ordering, fmt, ops::Range};
use std::hash::{Hash, Hasher};

/// An uncompressed DNS name, retaining the received bytes and their case.
///
/// Names compare, hash and order without regard to ASCII case, in DNSSEC
/// canonical order (RFC 4034 section 6.1).
///
/// `Display` writes presentation form with a final dot; the root is `.`.
/// Inside a label it escapes:
///
/// - `.` and `\` as `\.` and `\\`;
/// - space, the characters `"` `(` `)` `;` `@`, and every byte outside
///   printable ASCII, as `\DDD` in decimal (a space is `\032`).
///
/// Every other byte is written as it is.
#[derive(Clone)]
pub struct Name {
    wire: Vec<u8>,
    labels: Vec<Range<usize>>,
}

/// An invalid presentation name or uncompressed wire name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, thiserror::Error)]
#[non_exhaustive]
pub enum NameError {
    /// The text was empty; the root is written `.`.
    #[error("an empty name is not the root; use a dot")]
    EmptyName,
    /// A label other than the root's is empty.
    #[error("a non-root label is empty")]
    EmptyLabel,
    /// A label is longer than 63 bytes.
    #[error("a label exceeds 63 octets")]
    LabelTooLong,
    /// The name is longer than 255 bytes in wire form.
    #[error("a name exceeds 255 wire octets")]
    NameTooLong,
    /// A backslash escape is neither one character nor three decimal digits
    /// up to 255.
    #[error("an escape must contain one character or three decimal digits in 0..=255")]
    InvalidEscape,
    /// The wire bytes are not exactly one uncompressed name.
    #[error("an uncompressed wire name is malformed")]
    InvalidWire,
}

impl Name {
    /// The root name, `.`.
    pub fn root() -> Name {
        Self {
            wire: vec![0],
            labels: Vec::new(),
        }
    }

    /// Parses presentation text, including `\DDD` and `\X` escapes, without IDNA.
    pub fn from_text(text: &str) -> Result<Name, NameError> {
        if text == "." {
            return Ok(Self::root());
        }
        if text.is_empty() {
            return Err(NameError::EmptyName);
        }
        let mut wire = Vec::new();
        let mut labels = Vec::new();
        let mut label = Vec::new();
        let mut bytes = text.bytes().peekable();
        while let Some(byte) = bytes.next() {
            if byte == b'.' {
                Self::append_label(&mut wire, &mut labels, &label)?;
                label.clear();
                if bytes.peek().is_none() {
                    wire.push(0);
                    return Ok(Self { wire, labels });
                }
                continue;
            }
            let byte = if byte == b'\\' {
                let first = bytes.next().ok_or(NameError::InvalidEscape)?;
                if first.is_ascii_digit() {
                    let second = bytes.next().ok_or(NameError::InvalidEscape)?;
                    let third = bytes.next().ok_or(NameError::InvalidEscape)?;
                    let mut value = 0_u16;
                    for digit in [first, second, third] {
                        if !digit.is_ascii_digit() {
                            return Err(NameError::InvalidEscape);
                        }
                        value = value
                            .checked_mul(10)
                            .and_then(|value| {
                                digit
                                    .checked_sub(b'0')
                                    .and_then(|digit| value.checked_add(u16::from(digit)))
                            })
                            .ok_or(NameError::InvalidEscape)?;
                    }
                    u8::try_from(value).map_err(|_| NameError::InvalidEscape)?
                } else {
                    first
                }
            } else {
                byte
            };
            if label.len() >= 63 {
                return Err(NameError::LabelTooLong);
            }
            let projected = wire
                .len()
                .checked_add(label.len())
                .and_then(|length| length.checked_add(3))
                .ok_or(NameError::NameTooLong)?;
            if projected > 255 {
                return Err(NameError::NameTooLong);
            }
            label.push(byte);
        }
        Self::append_label(&mut wire, &mut labels, &label)?;
        wire.push(0);
        Ok(Self { wire, labels })
    }

    fn append_label(
        wire: &mut Vec<u8>,
        labels: &mut Vec<Range<usize>>,
        label: &[u8],
    ) -> Result<(), NameError> {
        if label.is_empty() {
            return Err(NameError::EmptyLabel);
        }
        if label.len() > 63 {
            return Err(NameError::LabelTooLong);
        }
        let start = wire.len().checked_add(1).ok_or(NameError::NameTooLong)?;
        let end = start
            .checked_add(label.len())
            .ok_or(NameError::NameTooLong)?;
        if end.checked_add(1).ok_or(NameError::NameTooLong)? > 255 {
            return Err(NameError::NameTooLong);
        }
        let length = u8::try_from(label.len()).map_err(|_| NameError::LabelTooLong)?;
        wire.push(length);
        wire.extend_from_slice(label);
        labels.push(start..end);
        Ok(())
    }

    /// Parses exactly one uncompressed name, with its final root octet.
    pub fn from_wire(bytes: &[u8]) -> Result<Name, NameError> {
        if bytes.len() > 255 {
            return Err(NameError::NameTooLong);
        }
        let mut remaining = bytes;
        let mut offset = 0_usize;
        let mut labels = Vec::new();
        loop {
            let (&length, rest) = remaining.split_first().ok_or(NameError::InvalidWire)?;
            if length == 0 {
                if !rest.is_empty() {
                    return Err(NameError::InvalidWire);
                }
                return Ok(Self {
                    wire: bytes.to_vec(),
                    labels,
                });
            }
            if length > 63 {
                return Err(NameError::InvalidWire);
            }
            let length = usize::from(length);
            let start = offset.checked_add(1).ok_or(NameError::InvalidWire)?;
            let end = start.checked_add(length).ok_or(NameError::InvalidWire)?;
            remaining = rest.get(length..).ok_or(NameError::InvalidWire)?;
            labels.push(start..end);
            offset = end;
        }
    }

    /// The name in uncompressed wire form, with its final root byte, in the
    /// case it was given.
    pub fn as_wire(&self) -> &[u8] {
        &self.wire
    }

    /// The number of labels, not counting the root: 0 for `.`, 2 for
    /// `example.com.`.
    pub fn label_count(&self) -> usize {
        self.labels.len()
    }

    /// The name with its first label removed, or `None` for the root.
    pub fn parent(&self) -> Option<Name> {
        self.suffix(self.label_count().checked_sub(1)?)
    }

    /// The name made of its last `count` labels: `suffix(0)` is the root, and
    /// `suffix(label_count())` is the name itself. `None` when `count` is
    /// more than the name has.
    pub fn suffix(&self, count: usize) -> Option<Name> {
        if count == 0 {
            return Some(Self::root());
        }
        let label = self.labels.iter().rev().nth(count.checked_sub(1)?)?;
        let start = label.start.checked_sub(1)?;
        Self::from_wire(self.wire.get(start..)?).ok()
    }

    /// Whether this name is `other` or a name under it, ignoring ASCII case.
    pub fn is_at_or_below(&self, other: &Name) -> bool {
        if self.label_count() < other.label_count() {
            return false;
        }
        self.labels()
            .rev()
            .zip(other.labels().rev())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
    }

    /// Compares labels from the root toward the leaf, folding ASCII case only.
    pub fn canonical_cmp(&self, other: &Name) -> Ordering {
        for (left, right) in self.labels().rev().zip(other.labels().rev()) {
            let order = left
                .iter()
                .map(u8::to_ascii_lowercase)
                .cmp(right.iter().map(u8::to_ascii_lowercase));
            if order != Ordering::Equal {
                return order;
            }
        }
        self.label_count().cmp(&other.label_count())
    }

    /// The same name with ASCII letters in lower case.
    pub fn to_lowercase(&self) -> Name {
        Self {
            wire: self.wire.iter().map(u8::to_ascii_lowercase).collect(),
            labels: self.labels.clone(),
        }
    }

    pub(crate) fn labels(&self) -> impl DoubleEndedIterator<Item = &[u8]> {
        self.labels
            .iter()
            .filter_map(|range| self.wire.get(range.clone()))
    }
}

impl PartialEq for Name {
    fn eq(&self, other: &Self) -> bool {
        self.wire.eq_ignore_ascii_case(&other.wire)
    }
}

impl Eq for Name {}

impl Hash for Name {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.wire.len().hash(state);
        for byte in &self.wire {
            byte.to_ascii_lowercase().hash(state);
        }
    }
}

impl Ord for Name {
    fn cmp(&self, other: &Self) -> Ordering {
        self.canonical_cmp(other)
    }
}

impl PartialOrd for Name {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The name in presentation form, as `Display` writes it.
impl fmt::Debug for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Name(\"{self}\")")
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.label_count() == 0 {
            return f.write_str(".");
        }
        for label in self.labels() {
            for &byte in label {
                match byte {
                    b'.' | b'\\' => write!(f, "\\{}", char::from(byte))?,
                    b'!'..=b'~' if !b"\"();@".contains(&byte) => {
                        write!(f, "{}", char::from(byte))?;
                    }
                    _ => write!(f, "\\{byte:03}")?,
                }
            }
            f.write_str(".")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Name, NameError};
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    #[test]
    fn presentation_and_wire_round_trips() {
        for text in [
            ".",
            "example.com",
            "MiXeD.Example.",
            "a\\.b.\\\\.\\000\\255.",
        ] {
            let name = Name::from_text(text).unwrap();
            assert_eq!(
                Name::from_wire(name.as_wire()).unwrap().as_wire(),
                name.as_wire()
            );
            assert_eq!(
                Name::from_text(&name.to_string()).unwrap().as_wire(),
                name.as_wire()
            );
            assert!(name.to_string().ends_with('.'));
        }
        assert_eq!(
            Name::from_text("example.com").unwrap().as_wire(),
            b"\x07example\x03com\0"
        );
        assert_eq!(
            Name::from_text("\\065\\046\\255").unwrap().as_wire(),
            b"\x03A.\xff\0"
        );
        assert_eq!(Name::from_text("\\a\\.").unwrap().to_string(), "a\\..");
    }

    #[test]
    fn every_label_octet_survives_display() {
        for byte in 0_u8..=255 {
            let wire = [1, byte, 0];
            let name = Name::from_wire(&wire).unwrap();
            assert_eq!(Name::from_text(&name.to_string()).unwrap().as_wire(), wire);
        }
    }

    #[test]
    fn label_and_name_wire_limits_are_exact() {
        assert!(Name::from_text(&"a".repeat(63)).is_ok());
        assert_eq!(
            Name::from_text(&"a".repeat(64)),
            Err(NameError::LabelTooLong)
        );
        let maximal = [
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61),
        ]
        .join(".");
        let name = Name::from_text(&maximal).unwrap();
        assert_eq!(name.as_wire().len(), 255);
        assert!(Name::from_wire(name.as_wire()).is_ok());
        assert_eq!(
            Name::from_text(&format!("{maximal}d")),
            Err(NameError::NameTooLong)
        );
        let mut overlong_wire = vec![63];
        overlong_wire.extend_from_slice(&[b'a'; 63]);
        overlong_wire.extend_from_slice(&[63]);
        overlong_wire.extend_from_slice(&[b'b'; 63]);
        overlong_wire.extend_from_slice(&[63]);
        overlong_wire.extend_from_slice(&[b'c'; 63]);
        overlong_wire.extend_from_slice(&[62]);
        overlong_wire.extend_from_slice(&[b'd'; 62]);
        overlong_wire.push(0);
        assert_eq!(overlong_wire.len(), 256);
        assert_eq!(Name::from_wire(&overlong_wire), Err(NameError::NameTooLong));
        let mut label64 = vec![64];
        label64.extend_from_slice(&[b'a'; 64]);
        label64.push(0);
        assert_eq!(Name::from_wire(&label64), Err(NameError::InvalidWire));
    }

    #[test]
    fn rejects_malformed_presentation_and_wire_names() {
        for text in [
            "", ".a", "a..", "a..b", "a\\", "a\\1", "a\\12", "a\\12x", "a\\256", "a\\999",
        ] {
            assert!(Name::from_text(text).is_err(), "{text:?}");
        }
        for wire in [
            &[][..],
            &[1][..],
            &[1, b'a'][..],
            &[0, 0][..],
            &[1, b'a', 0, 0][..],
            &[0xc0, 0][..],
            &[0x40, 0][..],
            &[0x80, 0][..],
        ] {
            assert!(Name::from_wire(wire).is_err(), "{wire:?}");
        }
        let name = Name::from_text("a.b.example").unwrap();
        for length in 0..name.as_wire().len() {
            assert!(Name::from_wire(name.as_wire().get(..length).unwrap()).is_err());
        }
    }

    #[test]
    fn equality_hash_order_and_lowercase_fold_only_ascii_letters() {
        let upper = Name::from_text("MiXeD.\\192\\255.Example").unwrap();
        let lower = Name::from_text("mixed.\\192\\255.example").unwrap();
        assert_eq!(upper, lower);
        assert_eq!(upper.cmp(&lower), core::cmp::Ordering::Equal);
        let hash = |name: &Name| {
            let mut hasher = DefaultHasher::new();
            name.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(&upper), hash(&lower));
        assert_eq!(upper.to_lowercase().as_wire(), lower.as_wire());
        assert_ne!(
            Name::from_text("\\192").unwrap(),
            Name::from_text("\\224").unwrap()
        );
        assert_eq!(upper.as_wire(), b"\x05MiXeD\x02\xc0\xff\x07Example\0");
    }

    #[test]
    fn ancestry_and_suffixes_respect_label_boundaries() {
        let name = Name::from_text("A.b.Example.").unwrap();
        assert_eq!(name.label_count(), 3);
        assert_eq!(name.parent().unwrap().to_string(), "b.Example.");
        assert_eq!(name.suffix(0), Some(Name::root()));
        assert_eq!(name.suffix(1).unwrap().to_string(), "Example.");
        assert_eq!(name.suffix(2).unwrap().to_string(), "b.Example.");
        assert_eq!(name.suffix(3), Some(name.clone()));
        assert_eq!(name.suffix(4), None);
        assert_eq!(name.suffix(usize::MAX), None);
        assert!(name.is_at_or_below(&Name::from_text("B.eXAMPLE").unwrap()));
        assert!(name.is_at_or_below(&name));
        assert!(name.is_at_or_below(&Name::root()));
        assert!(!name.is_at_or_below(&Name::from_text("ample").unwrap()));
        assert!(!name.is_at_or_below(&Name::from_text("c.a.b.example").unwrap()));
        assert_eq!(Name::root().parent(), None);
        assert_eq!(Name::root().label_count(), 0);
        assert_eq!(Name::root().suffix(0), Some(Name::root()));
        assert_eq!(Name::root().suffix(1), None);
    }

    #[test]
    fn rfc4034_section_6_1_canonical_name_order() {
        // rfc4034.txt lines 1027–1035: the complete canonical ordering example.
        let expected: Vec<Name> = [
            "example",
            "a.example",
            "yljkjljk.a.example",
            "Z.a.example",
            "zABC.a.EXAMPLE",
            "z.example",
            "\\001.z.example",
            "*.z.example",
            "\\200.z.example",
        ]
        .into_iter()
        .map(|text| Name::from_text(text).unwrap())
        .collect();
        let mut scrambled = expected.iter().rev().cloned().collect::<Vec<_>>();
        scrambled.sort();
        assert_eq!(scrambled, expected);
        for (actual, expected) in scrambled.iter().zip(&expected) {
            assert_eq!(actual.as_wire(), expected.as_wire());
        }
        assert!(Name::root() < expected.first().unwrap().clone());
    }

    #[test]
    fn canonical_order_compares_octets_before_label_length() {
        let names = ["a", "a\\000", "aa", "b", "\\128", "\\255"];
        let ordered = names
            .into_iter()
            .map(|text| Name::from_text(text).unwrap())
            .collect::<Vec<_>>();
        assert!(ordered.windows(2).all(|pair| pair.first() < pair.get(1)));
        assert!(Name::from_text("z.a").unwrap() < Name::from_text("a.b").unwrap());
    }
}
