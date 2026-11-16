// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Bounded RDATA parsing and the RFC 4034 / RFC 6840 canonical layouts.

use crate::Name;
use crate::wire::{ParseError, parse_name};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Rdata {
    pub(crate) uncompressed: Vec<u8>,
    pub(crate) canonical: Vec<u8>,
    pub(crate) kind: RdataKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RdataKind {
    Dnskey(Dnskey),
    Ds(Ds),
    Rrsig(Rrsig),
    Nsec(Nsec),
    Nsec3(Nsec3),
    Soa(Soa),
    Name(Name),
    Opaque,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Dnskey {
    pub(crate) flags: u16,
    pub(crate) protocol: u8,
    pub(crate) algorithm: u8,
    pub(crate) public_key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Ds {
    pub(crate) key_tag: u16,
    pub(crate) algorithm: u8,
    pub(crate) digest_type: u8,
    pub(crate) digest: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Rrsig {
    /// Received RR TTL; filled by the message parser, outside signed RDATA.
    pub(crate) received_ttl: u32,
    pub(crate) type_covered: u16,
    pub(crate) algorithm: u8,
    pub(crate) labels: u8,
    pub(crate) original_ttl: u32,
    pub(crate) expiration: u32,
    pub(crate) inception: u32,
    pub(crate) key_tag: u16,
    pub(crate) signer: Name,
    pub(crate) signature: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Nsec {
    pub(crate) next: Name,
    pub(crate) types: TypeBitmap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Nsec3 {
    pub(crate) hash_algorithm: u8,
    pub(crate) flags: u8,
    pub(crate) iterations: u16,
    pub(crate) salt: Vec<u8>,
    pub(crate) next_hashed_owner: Vec<u8>,
    pub(crate) types: TypeBitmap,
}

/// What an SOA is read for: the last of its five numbers, when all twenty
/// of their bytes are there after its two names. Bytes after the numbers
/// are passed over, and an SOA short of them reads with none, as dnsmasq
/// reads it (rfc1035.c, find_soa). The names and the other numbers are
/// kept only in the record's wire and signed forms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Soa {
    minimum: Option<u32>,
}

impl Soa {
    /// The zone's negative TTL (RFC 2308 section 4), when the record holds it.
    pub(crate) fn minimum(&self) -> Option<u32> {
        self.minimum
    }
}

/// RFC 4034 section 4.1.2 windows, kept sorted for logarithmic lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TypeBitmap {
    windows: Vec<(u8, Vec<u8>)>,
}

impl TypeBitmap {
    pub(crate) fn parse(mut bytes: &[u8]) -> Result<Self, ParseError> {
        let mut windows = Vec::new();
        let mut previous = None;
        while !bytes.is_empty() {
            let (&window, rest) = bytes.split_first().ok_or(ParseError::MalformedReply)?;
            let (&length, rest) = rest.split_first().ok_or(ParseError::MalformedReply)?;
            if !(1..=32).contains(&length) || previous.is_some_and(|value| value >= window) {
                return Err(ParseError::MalformedReply);
            }
            let (bitmap, rest) = rest
                .split_at_checked(usize::from(length))
                .ok_or(ParseError::MalformedReply)?;
            windows.push((window, bitmap.to_vec()));
            previous = Some(window);
            bytes = rest;
        }
        Ok(Self { windows })
    }

    pub(crate) fn has(&self, rtype: u16) -> bool {
        let [window, low] = rtype.to_be_bytes();
        let Some(bitmap) = self
            .windows
            .binary_search_by_key(&window, |entry| entry.0)
            .ok()
            .and_then(|index| self.windows.get(index))
            .map(|entry| &entry.1)
        else {
            return false;
        };
        bitmap
            .get(usize::from(low / 8))
            .is_some_and(|byte| byte & (0x80 >> (low % 8)) != 0)
    }
}

/// Decode the unpadded, case-insensitive base32hex used by NSEC3 owner labels.
pub(crate) fn decode_base32hex(text: &[u8]) -> Result<Vec<u8>, ParseError> {
    if !matches!(text.len() % 8, 0 | 2 | 4 | 5 | 7) {
        return Err(ParseError::MalformedReply);
    }
    let mut output = Vec::new();
    let mut accumulator = 0_u16;
    let mut bits = 0_u32;
    for byte in text {
        let value = match byte.to_ascii_uppercase() {
            digit @ b'0'..=b'9' => digit.checked_sub(b'0'),
            digit @ b'A'..=b'V' => digit.checked_sub(b'A').and_then(|n| n.checked_add(10)),
            _ => None,
        }
        .ok_or(ParseError::MalformedReply)?;
        accumulator = accumulator
            .checked_shl(5)
            .ok_or(ParseError::MalformedReply)?
            | u16::from(value);
        bits = bits.checked_add(5).ok_or(ParseError::MalformedReply)?;
        if bits >= 8 {
            bits = bits.checked_sub(8).ok_or(ParseError::MalformedReply)?;
            let decoded = accumulator
                .checked_shr(bits)
                .ok_or(ParseError::MalformedReply)?;
            output.push(u8::try_from(decoded).map_err(|_| ParseError::MalformedReply)?);
            let mask = 1_u16
                .checked_shl(bits)
                .and_then(|n| n.checked_sub(1))
                .ok_or(ParseError::MalformedReply)?;
            accumulator &= mask;
        }
    }
    if accumulator != 0 {
        return Err(ParseError::MalformedReply);
    }
    Ok(output)
}

struct Cursor<'a> {
    message: &'a [u8],
    offset: usize,
    end: usize,
    uncompressed: Vec<u8>,
    canonical: Vec<u8>,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], ParseError> {
        let next = self
            .offset
            .checked_add(count)
            .ok_or(ParseError::MalformedReply)?;
        if next > self.end {
            return Err(ParseError::MalformedReply);
        }
        let bytes = self
            .message
            .get(self.offset..next)
            .ok_or(ParseError::MalformedReply)?;
        self.offset = next;
        self.uncompressed.extend_from_slice(bytes);
        self.canonical.extend_from_slice(bytes);
        Ok(bytes)
    }

    fn byte(&mut self) -> Result<u8, ParseError> {
        self.take(1)?
            .first()
            .copied()
            .ok_or(ParseError::MalformedReply)
    }

    fn u16(&mut self) -> Result<u16, ParseError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| ParseError::MalformedReply)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, ParseError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| ParseError::MalformedReply)?,
        ))
    }

    fn name(&mut self, lowercase: bool) -> Result<Name, ParseError> {
        let name = parse_name(self.message, &mut self.offset, self.end)?;
        self.uncompressed.extend_from_slice(name.as_wire());
        if lowercase {
            self.canonical
                .extend_from_slice(name.to_lowercase().as_wire());
        } else {
            self.canonical.extend_from_slice(name.as_wire());
        }
        Ok(name)
    }

    fn rest(&mut self, nonempty: bool) -> Result<Vec<u8>, ParseError> {
        let count = self
            .end
            .checked_sub(self.offset)
            .ok_or(ParseError::MalformedReply)?;
        if nonempty && count == 0 {
            return Err(ParseError::MalformedReply);
        }
        Ok(self.take(count)?.to_vec())
    }
}

/// Parse exactly one RDLENGTH-bounded value, retaining delivery and signed forms.
pub(crate) fn parse(
    message: &[u8],
    start: usize,
    end: usize,
    rtype: u16,
) -> Result<Rdata, ParseError> {
    if start > end || end > message.len() {
        return Err(ParseError::MalformedReply);
    }
    let mut cursor = Cursor {
        message,
        offset: start,
        end,
        uncompressed: Vec::new(),
        canonical: Vec::new(),
    };
    let kind = match rtype {
        48 => RdataKind::Dnskey(Dnskey {
            flags: cursor.u16()?,
            protocol: cursor.byte()?,
            algorithm: cursor.byte()?,
            public_key: cursor.rest(true)?,
        }),
        43 => RdataKind::Ds(Ds {
            key_tag: cursor.u16()?,
            algorithm: cursor.byte()?,
            digest_type: cursor.byte()?,
            digest: cursor.rest(true)?,
        }),
        46 => RdataKind::Rrsig(Rrsig {
            received_ttl: u32::MAX,
            type_covered: cursor.u16()?,
            algorithm: cursor.byte()?,
            labels: cursor.byte()?,
            original_ttl: cursor.u32()?,
            expiration: cursor.u32()?,
            inception: cursor.u32()?,
            key_tag: cursor.u16()?,
            signer: cursor.name(true)?,
            signature: cursor.rest(true)?,
        }),
        47 => {
            let next = cursor.name(false)?;
            let types = TypeBitmap::parse(&cursor.rest(false)?)?;
            RdataKind::Nsec(Nsec { next, types })
        }
        50 => {
            let hash_algorithm = cursor.byte()?;
            let flags = cursor.byte()?;
            let iterations = cursor.u16()?;
            let salt_length = cursor.byte()?;
            let salt = cursor.take(usize::from(salt_length))?.to_vec();
            let hash_length = cursor.byte()?;
            let next_hashed_owner = cursor.take(usize::from(hash_length))?.to_vec();
            let types = TypeBitmap::parse(&cursor.rest(false)?)?;
            RdataKind::Nsec3(Nsec3 {
                hash_algorithm,
                flags,
                iterations,
                salt,
                next_hashed_owner,
                types,
            })
        }
        6 => {
            cursor.name(true)?;
            cursor.name(true)?;
            // Serial, refresh, retry and expire, then the minimum.
            let minimum = if end.saturating_sub(cursor.offset) >= 20 {
                cursor.take(16)?;
                Some(cursor.u32()?)
            } else {
                None
            };
            cursor.rest(false)?;
            RdataKind::Soa(Soa { minimum })
        }
        2 | 5 | 12 | 39 => RdataKind::Name(cursor.name(true)?),
        // RFC 4034 section 6.2, corrected by RFC 6840 section 5.1.
        3 | 4 | 7 | 8 | 9 => {
            cursor.name(true)?;
            RdataKind::Opaque
        }
        14 | 17 => {
            cursor.name(true)?;
            cursor.name(true)?;
            RdataKind::Opaque
        }
        15 | 18 | 21 | 36 => {
            cursor.take(2)?;
            cursor.name(true)?;
            RdataKind::Opaque
        }
        26 => {
            cursor.take(2)?;
            cursor.name(true)?;
            cursor.name(true)?;
            RdataKind::Opaque
        }
        33 => {
            cursor.take(6)?;
            cursor.name(true)?;
            RdataKind::Opaque
        }
        35 => {
            cursor.take(4)?;
            for _ in 0..3 {
                let length = cursor.byte()?;
                cursor.take(usize::from(length))?;
            }
            cursor.name(true)?;
            RdataKind::Opaque
        }
        24 => {
            cursor.take(18)?;
            cursor.name(true)?;
            cursor.rest(false)?;
            RdataKind::Opaque
        }
        30 => {
            cursor.name(true)?;
            cursor.rest(false)?;
            RdataKind::Opaque
        }
        // Unknown formats (including HINFO and A6) must remain byte-identical.
        _ => {
            cursor.rest(false)?;
            RdataKind::Opaque
        }
    };
    if cursor.offset != end {
        return Err(ParseError::MalformedReply);
    }
    Ok(Rdata {
        uncompressed: cursor.uncompressed,
        canonical: cursor.canonical,
        kind,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(text: &str) -> Vec<u8> {
        text.as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(core::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    // rfc4034.txt lines 797..815, section 4.3: A MX RRSIG NSEC TYPE1234.
    fn example_bitmap() -> Vec<u8> {
        let mut bitmap = vec![0, 6, 0x40, 1, 0, 0, 0, 3, 4, 27];
        bitmap.extend([0; 26]);
        bitmap.push(0x20);
        bitmap
    }

    // rfc6605.txt lines 207..209; base64 public key represented here as hex.
    fn dnskey() -> Vec<u8> {
        let mut bytes = vec![1, 1, 3, 13];
        bytes.extend(hex(concat!(
            "1a88c88615d437fbb8bf9e1942a1929f28562706ae6c2bd399e7b1bfb6d1e9e7",
            "5b92b4aa42917ae1c61b701ef035c3fe7be3009cbafe5a2f71316c902dcf0d00"
        )));
        bytes
    }

    // rfc6605.txt lines 211..213.
    fn ds() -> Vec<u8> {
        let mut bytes = vec![0xd9, 0x60, 13, 2];
        bytes.extend(hex(
            "b4c8c1fe2e7477127b27115656ad6256f424625bf5c1e2770ce6d6e37df61d17",
        ));
        bytes
    }

    // rfc6605.txt lines 216..219; date fields converted to DNS serial seconds.
    fn rrsig() -> Vec<u8> {
        let mut bytes = vec![0, 1, 13, 3];
        for value in [3600_u32, 1_284_026_679, 1_281_607_479] {
            bytes.extend(value.to_be_bytes());
        }
        bytes.extend(55648_u16.to_be_bytes());
        bytes.extend(Name::from_text("example.net.").unwrap().as_wire());
        bytes.extend(hex(concat!(
            "ab1eb02d8aa687e97da0229337aa8873e6f0eb26be289f28333d183f5d3b7a95c",
            "0c869adfb748daee3c5286eed6682c12e5533186baced9c26c167a9ebae950b"
        )));
        bytes
    }

    #[test]
    fn rfc4034_bitmap_has_every_listed_type_and_no_others() {
        let bitmap = TypeBitmap::parse(&example_bitmap()).unwrap();
        for rtype in 0..=u16::MAX {
            assert_eq!(bitmap.has(rtype), [1, 15, 46, 47, 1234].contains(&rtype));
        }
        assert!(!TypeBitmap::parse(&[]).unwrap().has(1));
    }

    #[test]
    fn bitmap_rejects_broken_headers_windows_lengths_and_truncation() {
        for malformed in [
            vec![0],
            vec![0, 0],
            vec![0, 33],
            vec![0, 1],
            vec![1, 1, 1, 0, 1, 1],
            vec![1, 1, 1, 1, 1, 1],
        ] {
            assert_eq!(
                TypeBitmap::parse(&malformed),
                Err(ParseError::MalformedReply)
            );
        }
        let bitmap = example_bitmap();
        for end in 1..bitmap.len() {
            // The first complete window alone is valid; every partial window fails.
            if end != 8 {
                assert_eq!(
                    TypeBitmap::parse(&bitmap[..end]),
                    Err(ParseError::MalformedReply)
                );
            }
        }
        let last = TypeBitmap::parse(&[255, 32].into_iter().chain([0xff; 32]).collect::<Vec<_>>())
            .unwrap();
        assert!(last.has(u16::MAX));
    }

    #[test]
    fn base32hex_decodes_rfc5155_hash_case_insensitively() {
        // rfc5155.txt line 1922, H(example); binary form obtained by decoding
        // that printed value, independently with the standard base32hex codec.
        let text = b"0p9mhaveqvm6t7vbl5lop2u3t2rp3tom";
        let expected = hex("065368abeed7ec6e9feba96b8c8bc3e8b791f716");
        assert_eq!(decode_base32hex(text).unwrap(), expected);
        assert_eq!(
            decode_base32hex(&text.to_ascii_uppercase()).unwrap(),
            expected
        );
    }

    #[test]
    fn base32hex_checks_alphabet_lengths_padding_and_unused_bits() {
        // Structural mutations of the RFC 5155 line 1922 alphabet and lengths.
        for invalid in [
            b"0".as_slice(),
            b"000",
            b"000000",
            b"0W",
            b"0/",
            b"0=",
            b"0 ",
            b"01",
            b"0001",
            b"00001",
            b"0000001",
        ] {
            assert_eq!(decode_base32hex(invalid), Err(ParseError::MalformedReply));
        }
        for valid in [
            b"".as_slice(),
            b"00",
            b"0000",
            b"00000",
            b"0000000",
            b"00000000",
        ] {
            assert!(
                decode_base32hex(valid)
                    .unwrap()
                    .iter()
                    .all(|byte| *byte == 0)
            );
        }
    }

    #[test]
    fn parses_rfc6605_dnskey_ds_and_rrsig_fields() {
        let key_bytes = dnskey();
        let key = parse(&key_bytes, 0, key_bytes.len(), 48).unwrap();
        let RdataKind::Dnskey(key) = key.kind else {
            panic!("DNSKEY type lost")
        };
        assert_eq!((key.flags, key.protocol, key.algorithm), (257, 3, 13));
        assert_eq!(key.public_key, dnskey()[4..]);
        let ds_bytes = ds();
        let data = parse(&ds_bytes, 0, ds_bytes.len(), 43).unwrap();
        let RdataKind::Ds(data) = data.kind else {
            panic!("DS type lost")
        };
        assert_eq!(
            (data.key_tag, data.algorithm, data.digest_type),
            (55648, 13, 2)
        );
        assert_eq!(data.digest, ds()[4..]);
        let signature = rrsig();
        let data = parse(&signature, 0, signature.len(), 46).unwrap();
        assert_eq!(data.canonical, signature);
        let RdataKind::Rrsig(data) = data.kind else {
            panic!("RRSIG type lost")
        };
        assert_eq!((data.type_covered, data.algorithm, data.labels), (1, 13, 3));
        assert_eq!(
            (data.original_ttl, data.expiration, data.inception),
            (3600, 1_284_026_679, 1_281_607_479)
        );
        assert_eq!(data.key_tag, 55648);
        assert_eq!(data.signer, Name::from_text("example.net.").unwrap());
        assert_eq!(data.signature.len(), 64);
    }

    #[test]
    fn typed_parsers_reject_missing_variable_data_and_short_fields() {
        for rtype in [43, 48] {
            for end in 0..=4 {
                assert_eq!(
                    parse(&[0; 4], 0, end, rtype),
                    Err(ParseError::MalformedReply)
                );
            }
        }
        let signature = rrsig();
        let prefix_length = signature.len() - 64;
        for end in 0..=prefix_length {
            assert_eq!(
                parse(&signature, 0, end, 46),
                Err(ParseError::MalformedReply)
            );
        }
        assert!(parse(&signature, 0, prefix_length + 1, 46).is_ok());
    }

    #[test]
    fn nsec_expands_a_pointer_and_preserves_next_name_case() {
        // Name and type bitmap from rfc4034.txt lines 797..815; case and
        // compression are deliberate mutations exercising section 6.2.
        let name = Name::from_text("HoSt.Example.Com.").unwrap();
        let mut message = name.as_wire().to_vec();
        let start = message.len();
        message.extend([0xc0, 0]);
        message.extend(example_bitmap());
        let parsed = parse(&message, start, message.len(), 47).unwrap();
        let mut expanded = name.as_wire().to_vec();
        expanded.extend(example_bitmap());
        assert_eq!(parsed.uncompressed, expanded);
        assert_eq!(parsed.canonical, expanded);
        let RdataKind::Nsec(data) = parsed.kind else {
            panic!("NSEC type lost")
        };
        assert_eq!(data.next.as_wire(), name.as_wire());
        assert!(data.types.has(1234));
    }

    #[test]
    fn nsec3_parses_rfc5155_fields_and_bounded_lengths() {
        // rfc5155.txt lines 1981..1983: 1 1 12 aabbccdd, next owner and types.
        let mut bytes = vec![1, 1, 0, 12, 4, 0xaa, 0xbb, 0xcc, 0xdd, 20];
        bytes.extend(decode_base32hex(b"2t7b4g4vsa5smi47k61mv5bv1a22bojr").unwrap());
        let bitmap = [0, 7, 0x22, 0x01, 0, 0, 0, 2, 0x90];
        bytes.extend(bitmap);
        let parsed = parse(&bytes, 0, bytes.len(), 50).unwrap();
        assert_eq!(parsed.canonical, bytes);
        let RdataKind::Nsec3(data) = parsed.kind else {
            panic!("NSEC3 type lost")
        };
        assert_eq!(
            (data.hash_algorithm, data.flags, data.iterations),
            (1, 1, 12)
        );
        assert_eq!(data.salt, [0xaa, 0xbb, 0xcc, 0xdd]);
        assert_eq!(data.next_hashed_owner.len(), 20);
        for rtype in [2, 6, 15, 46, 48, 51] {
            assert!(data.types.has(rtype));
        }
        for end in 0..30 {
            assert_eq!(parse(&bytes, 0, end, 50), Err(ParseError::MalformedReply));
        }
        assert!(parse(&bytes, 0, 30, 50).is_ok()); // Empty bitmap is legal.
        for end in 31..bytes.len() {
            assert_eq!(parse(&bytes, 0, end, 50), Err(ParseError::MalformedReply));
        }
    }

    #[test]
    fn soa_parses_rfc5155_passes_over_trailing_bytes_and_reads_short_numbers_as_none() {
        // rfc5155.txt lines 1935..1936, SOA example.
        let mut bytes = Name::from_text("ns1.example.").unwrap().as_wire().to_vec();
        bytes.extend(Name::from_text("bugs.x.w.example.").unwrap().as_wire());
        let names = bytes.len();
        for value in [1_u32, 3600, 300, 3_600_000, 3600] {
            bytes.extend(value.to_be_bytes());
        }
        let soa = |bytes: &[u8]| {
            let parsed = parse(bytes, 0, bytes.len(), 6).unwrap();
            assert_eq!(parsed.canonical, bytes);
            let RdataKind::Soa(data) = parsed.kind else {
                panic!("SOA type lost")
            };
            data
        };
        // The names and numbers stay in the signed form, checked above; the
        // minimum is the last number.
        assert_eq!(soa(&bytes).minimum(), Some(3600));
        let last = bytes.len() - 1;
        bytes[last] = 7;
        assert_eq!(soa(&bytes).minimum(), Some(3591));
        bytes[last] = 0x10;
        // A cut name breaks the layout; a cut number leaves no minimum.
        for end in 0..names {
            assert_eq!(parse(&bytes, 0, end, 6), Err(ParseError::MalformedReply));
        }
        for end in names..bytes.len() {
            assert_eq!(soa(&bytes[..end]).minimum(), None, "{end}");
        }
        // Bytes after the numbers are passed over and kept in the signed form.
        for extra in 1..=3 {
            bytes.push(0);
            assert_eq!(soa(&bytes).minimum(), Some(3600), "{extra}");
        }
    }

    #[test]
    fn every_canonical_layout_expands_names_and_changes_only_name_case() {
        // Structural layouts from handoff 6.2, using the next name in
        // rfc4034.txt lines 797..810 with deliberately mixed ASCII case.
        // Fixed prefixes and opaque suffixes are synthetic layout markers.
        let name = Name::from_text("HoSt.Example.Com.").unwrap();
        let lower = name.to_lowercase();
        let layouts: &[(u16, &[u8], usize, &[u8])] = &[
            (2, &[], 1, &[]),
            (3, &[], 1, &[]),
            (4, &[], 1, &[]),
            (5, &[], 1, &[]),
            (7, &[], 1, &[]),
            (8, &[], 1, &[]),
            (9, &[], 1, &[]),
            (12, &[], 1, &[]),
            (39, &[], 1, &[]),
            (14, &[], 2, &[]),
            (17, &[], 2, &[]),
            (6, &[], 2, &[0; 20]),
            (15, &[0, 10], 1, &[]),
            (18, &[0, 10], 1, &[]),
            (21, &[0, 10], 1, &[]),
            (36, &[0, 10], 1, &[]),
            (26, &[0, 10], 2, &[]),
            (33, &[0; 6], 1, &[]),
            (35, &[0, 0, 0, 0, 1, b'U', 2, b'S', b'I', 0], 1, &[]),
            (24, &[0; 18], 1, b"SIG"),
            (46, &[0; 18], 1, b"SIG"),
            (30, &[], 1, b"NXT"),
        ];
        for &(rtype, prefix, count, suffix) in layouts {
            let mut message = name.as_wire().to_vec();
            let start = message.len();
            message.extend(prefix);
            let mut expanded = prefix.to_vec();
            let mut canonical = prefix.to_vec();
            for _ in 0..count {
                message.extend([0xc0, 0]);
                expanded.extend(name.as_wire());
                canonical.extend(lower.as_wire());
            }
            message.extend(suffix);
            expanded.extend(suffix);
            canonical.extend(suffix);
            let parsed = parse(&message, start, message.len(), rtype).unwrap();
            assert_eq!(parsed.uncompressed, expanded, "type {rtype}");
            assert_eq!(parsed.canonical, canonical, "type {rtype}");
            // An absent or partial final compressed name always violates the layout.
            let final_name = start + prefix.len() + (count - 1) * 2;
            for end in start..=final_name + 1 {
                assert_eq!(
                    parse(&message, start, end, rtype),
                    Err(ParseError::MalformedReply),
                    "type {rtype} end {end}"
                );
            }
            // An SOA passes over bytes after its numbers; the opaque tails
            // take them in.
            if ![6, 24, 30, 46].contains(&rtype) {
                message.push(0);
                assert_eq!(
                    parse(&message, start, message.len(), rtype),
                    Err(ParseError::MalformedReply),
                    "type {rtype}"
                );
            }
        }
    }

    #[test]
    fn canonical_table_rejects_malformed_name_and_naptr_string_boundaries() {
        // Mutations of the handoff 6.2 layouts, not published RR vectors.
        for rtype in [2, 3, 4, 5, 7, 8, 9, 12, 14, 17, 39, 30, 47] {
            for malformed in [&[0x40][..], &[1][..], &[0xc0][..], &[0xc0, 0][..]] {
                assert_eq!(
                    parse(malformed, 0, malformed.len(), rtype),
                    Err(ParseError::MalformedReply)
                );
            }
        }
        for bytes in [
            vec![0; 4],
            vec![0, 0, 0, 0, 2, b'U'],
            vec![0, 0, 0, 0, 0, 0, 1],
            vec![0; 7],
        ] {
            assert_eq!(
                parse(&bytes, 0, bytes.len(), 35),
                Err(ParseError::MalformedReply)
            );
        }
        for rtype in [15, 18, 21, 36, 26, 33, 24] {
            assert_eq!(parse(&[], 0, 0, rtype), Err(ParseError::MalformedReply));
        }
    }

    #[test]
    fn unknown_hinfo_a6_and_new_types_remain_opaque() {
        // Deliberately invalid names/pointers prove excluded types are not parsed.
        let opaque = [0xc0, 0, b'A', b'B', 0x40];
        for rtype in [0, 1, 13, 28, 38, 41, 64, 65, 65000, u16::MAX] {
            let parsed = parse(&opaque, 0, opaque.len(), rtype).unwrap();
            assert_eq!(parsed.uncompressed, opaque);
            assert_eq!(parsed.canonical, opaque);
            assert_eq!(parsed.kind, RdataKind::Opaque);
        }
    }

    #[test]
    fn rdata_bounds_fail_without_panicking() {
        for (start, end) in [(1, 0), (0, 2), (usize::MAX, usize::MAX), (1, usize::MAX)] {
            assert_eq!(parse(&[0], start, end, 5), Err(ParseError::MalformedReply));
        }
        // The name from rfc4034.txt lines 797..810 cannot read beyond RDLENGTH.
        let name = Name::from_text("host.example.com.").unwrap();
        for end in 0..name.as_wire().len() {
            assert_eq!(
                parse(name.as_wire(), 0, end, 5),
                Err(ParseError::MalformedReply)
            );
        }
        assert!(parse(name.as_wire(), 0, name.as_wire().len(), 5).is_ok());
    }
}
