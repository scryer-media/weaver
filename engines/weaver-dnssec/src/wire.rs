// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};

use crate::limits::{
    MAX_DNSKEYS, MAX_EXTENDED_ERRORS, MAX_MESSAGE_BYTES, MAX_POINTER_HOPS, MAX_RRSET_RECORDS,
    MAX_RRSIGS_PER_RRSET,
};
use crate::rdata::{self, Rdata, RdataKind, Rrsig};
use crate::{Name, Query, RecordType};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParseError {
    MalformedReply,
    Truncated,
    // Only the unit tests' strict parse rejects an RCODE; validation reads every RCODE.
    #[cfg(test)]
    Rcode(u16),
}

#[derive(Clone, Debug)]
pub(crate) struct Rrset {
    pub(crate) owner: Name,
    pub(crate) rtype: RecordType,
    pub(crate) ttl: u32,
    pub(crate) records: Vec<Rdata>,
    pub(crate) rrsigs: Vec<Rrsig>,
}

#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub(crate) rcode: u16,
    pub(crate) answer: Vec<Rrset>,
    pub(crate) authority: Vec<Rrset>,
    /// RRSIG records received in the answer and authority sections.
    pub(crate) signatures: usize,
    /// Extended DNS Error info codes from the OPT record, in received order.
    pub(crate) extended_errors: Vec<u16>,
}

impl Query {
    /// Encode the section 4.3 recursive query, with CD and EDNS DO set.
    pub fn encode(&self, id: u16) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.extend_from_slice(&[0x01, 0x10, 0, 1, 0, 0, 0, 0, 0, 1]);
        bytes.extend_from_slice(self.name.as_wire());
        bytes.extend_from_slice(&self.rtype.0.to_be_bytes());
        bytes.extend_from_slice(&[0, 1]);
        bytes.extend_from_slice(&[0, 0, 41, 4, 208, 0, 0, 128, 0, 0, 0]);
        bytes
    }
}

impl Message {
    /// Parse a NOERROR or NXDOMAIN reply.
    #[cfg(test)]
    pub(crate) fn parse(message: &[u8], query: &Query) -> Result<Self, ParseError> {
        let parsed = Self::parse_any(message, query)?;
        if parsed.rcode != 0 && parsed.rcode != 3 {
            return Err(ParseError::Rcode(parsed.rcode));
        }
        Ok(parsed)
    }

    /// Parse a well-framed reply whatever its RCODE.
    pub(crate) fn parse_any(message: &[u8], query: &Query) -> Result<Self, ParseError> {
        if !(12..=MAX_MESSAGE_BYTES).contains(&message.len()) {
            return Err(ParseError::MalformedReply);
        }
        let mut reader = Reader::new(message, 0, message.len());
        let _id = reader.u16()?;
        let flags = reader.u16()?;
        if flags & 0x8000 == 0 || flags & 0x7800 != 0 {
            return Err(ParseError::MalformedReply);
        }
        if flags & 0x0200 != 0 {
            return Err(ParseError::Truncated);
        }
        let questions = reader.u16()?;
        let answers = reader.u16()?;
        let authorities = reader.u16()?;
        let additionals = reader.u16()?;
        if questions != 1 {
            return Err(ParseError::MalformedReply);
        }
        let name = parse_name(message, &mut reader.pos, message.len())?;
        let rtype = reader.u16()?;
        let class = reader.u16()?;
        if name != query.name || rtype != query.rtype.0 || class != 1 {
            return Err(ParseError::MalformedReply);
        }

        let mut answer = Section::default();
        let mut authority = Section::default();
        let mut opt_ttl = None;
        let mut extended_errors = Vec::new();
        let mut signatures = 0_usize;
        for (section, count) in [(0, answers), (1, authorities), (2, additionals)] {
            for _ in 0..count {
                let record = read_record(&mut reader)?;
                if record.rtype == RecordType::OPT.0 {
                    if section != 2 || opt_ttl.replace(record.ttl).is_some() {
                        return Err(ParseError::MalformedReply);
                    }
                    extended_errors = extended_error_codes(&record.rdata.uncompressed);
                    continue;
                }
                if record.class != 1 || section == 2 {
                    continue;
                }
                if record.rtype == RecordType::RRSIG.0 {
                    signatures = signatures.saturating_add(1);
                }
                if section == 0 {
                    answer.push(record)?;
                } else {
                    authority.push(record)?;
                }
            }
        }
        if reader.pos != message.len() {
            return Err(ParseError::MalformedReply);
        }
        let rcode = ((opt_ttl.unwrap_or(0) >> 24) as u16) << 4 | (flags & 0x000f);
        Ok(Self {
            rcode,
            answer: answer.finish(),
            authority: authority.finish(),
            signatures,
            extended_errors,
        })
    }
}

/// RFC 8914 section 2: option code 15 holds a 16-bit info code and text.
/// Options are informational, so a malformed list ends the scan instead of
/// failing the reply.
fn extended_error_codes(options: &[u8]) -> Vec<u16> {
    let mut codes = Vec::new();
    let mut rest = options;
    while let [code_high, code_low, length_high, length_low, tail @ ..] = rest {
        let length = usize::from(u16::from_be_bytes([*length_high, *length_low]));
        let Some(data) = tail.get(..length) else {
            break;
        };
        if u16::from_be_bytes([*code_high, *code_low]) == 15
            && let [high, low, ..] = data
            && codes.len() < MAX_EXTENDED_ERRORS
        {
            codes.push(u16::from_be_bytes([*high, *low]));
        }
        rest = tail.get(length..).unwrap_or_default();
    }
    codes
}

/// Expand one name while bounding its original encoding to `end`.
/// Pointer targets are in the whole message; only the original name advances `offset`.
pub(crate) fn parse_name(
    message: &[u8],
    offset: &mut usize,
    end: usize,
) -> Result<Name, ParseError> {
    if end > message.len() || *offset >= end {
        return Err(ParseError::MalformedReply);
    }
    let mut pos = *offset;
    let mut bound = end;
    let mut consumed = None;
    let mut hops = 0usize;
    let mut expanded = Vec::new();
    loop {
        let pointer_pos = pos;
        let mut reader = Reader::new(message, pos, bound);
        let length = reader.u8()?;
        match length {
            0 => {
                if expanded.len() >= 255 {
                    return Err(ParseError::MalformedReply);
                }
                expanded.push(0);
                let name = Name::from_wire(&expanded).map_err(|_| ParseError::MalformedReply)?;
                *offset = consumed.unwrap_or(reader.pos);
                return Ok(name);
            }
            1..=63 => {
                let label = reader.take(usize::from(length))?;
                let expanded_len = expanded
                    .len()
                    .checked_add(1)
                    .and_then(|size| size.checked_add(label.len()))
                    .ok_or(ParseError::MalformedReply)?;
                if expanded_len >= 255 {
                    return Err(ParseError::MalformedReply);
                }
                expanded.push(length);
                expanded.extend_from_slice(label);
                pos = reader.pos;
            }
            0xc0..=0xff => {
                let low = reader.u8()?;
                let target = usize::from(u16::from(length & 0x3f) << 8 | u16::from(low));
                if target >= pointer_pos {
                    return Err(ParseError::MalformedReply);
                }
                hops = hops.checked_add(1).ok_or(ParseError::MalformedReply)?;
                if hops > MAX_POINTER_HOPS {
                    return Err(ParseError::MalformedReply);
                }
                consumed.get_or_insert(reader.pos);
                pos = target;
                bound = message.len();
            }
            _ => return Err(ParseError::MalformedReply),
        }
    }
}

struct Reader<'a> {
    message: &'a [u8],
    pos: usize,
    end: usize,
}

impl<'a> Reader<'a> {
    fn new(message: &'a [u8], pos: usize, end: usize) -> Self {
        Self { message, pos, end }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], ParseError> {
        let end = self
            .pos
            .checked_add(count)
            .ok_or(ParseError::MalformedReply)?;
        if end > self.end {
            return Err(ParseError::MalformedReply);
        }
        let bytes = self
            .message
            .get(self.pos..end)
            .ok_or(ParseError::MalformedReply)?;
        self.pos = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, ParseError> {
        self.take(1)?
            .first()
            .copied()
            .ok_or(ParseError::MalformedReply)
    }

    fn u16(&mut self) -> Result<u16, ParseError> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| ParseError::MalformedReply)?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32, ParseError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| ParseError::MalformedReply)?;
        Ok(u32::from_be_bytes(bytes))
    }
}

struct Record {
    owner: Name,
    rtype: u16,
    class: u16,
    ttl: u32,
    rdata: Rdata,
}

fn read_record(reader: &mut Reader<'_>) -> Result<Record, ParseError> {
    let owner = parse_name(reader.message, &mut reader.pos, reader.end)?;
    let rtype = reader.u16()?;
    let class = reader.u16()?;
    let ttl = reader.u32()?;
    // RFC 2181 section 8: a received TTL with its high bit set is zero.
    // OPT uses these four octets for extended RCODE, version and flags.
    let ttl = if rtype != RecordType::OPT.0 && ttl & 0x8000_0000 != 0 {
        0
    } else {
        ttl
    };
    let length = usize::from(reader.u16()?);
    let start = reader.pos;
    reader.take(length)?;
    let rdata = rdata::parse(reader.message, start, reader.pos, rtype)?;
    Ok(Record {
        owner,
        rtype,
        class,
        ttl,
        rdata,
    })
}

#[derive(Default)]
struct Section {
    sets: BTreeMap<(Name, RecordType), SetBuilder>,
    signatures: Vec<(Name, u32, Rdata)>,
}

struct SetBuilder {
    set: Rrset,
    seen: BTreeSet<Vec<u8>>,
    signature_seen: BTreeMap<Vec<u8>, usize>,
}

impl Section {
    fn push(&mut self, record: Record) -> Result<(), ParseError> {
        if record.rtype == RecordType::RRSIG.0 {
            self.signatures
                .push((record.owner, record.ttl, record.rdata));
            return Ok(());
        }
        let rtype = RecordType(record.rtype);
        let builder = self
            .sets
            .entry((record.owner.clone(), rtype))
            .or_insert_with(|| SetBuilder {
                set: Rrset {
                    owner: record.owner,
                    rtype,
                    ttl: record.ttl,
                    records: Vec::new(),
                    rrsigs: Vec::new(),
                },
                seen: BTreeSet::new(),
                signature_seen: BTreeMap::new(),
            });
        builder.set.ttl = builder.set.ttl.min(record.ttl);
        if builder.seen.insert(record.rdata.canonical.clone()) {
            builder.set.records.push(record.rdata);
            if builder.set.records.len() > MAX_RRSET_RECORDS
                || (rtype == RecordType::DNSKEY && builder.set.records.len() > MAX_DNSKEYS)
            {
                return Err(ParseError::MalformedReply);
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Vec<Rrset> {
        for (owner, ttl, data) in self.signatures {
            let RdataKind::Rrsig(mut signature) = data.kind else {
                continue;
            };
            if let Some(builder) = self
                .sets
                .get_mut(&(owner, RecordType(signature.type_covered)))
            {
                if let Some(index) = builder.signature_seen.get(&data.canonical) {
                    if let Some(existing) = builder.set.rrsigs.get_mut(*index) {
                        existing.received_ttl = existing.received_ttl.min(ttl);
                    }
                } else if builder.set.rrsigs.len() < MAX_RRSIGS_PER_RRSET {
                    signature.received_ttl = ttl;
                    builder
                        .signature_seen
                        .insert(data.canonical, builder.set.rrsigs.len());
                    builder.set.rrsigs.push(signature);
                }
            }
        }
        self.sets.into_values().map(|builder| builder.set).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // RFC 4035, rfc4035.txt lines 2285-2290: x.w.example MX 1 xx.example.
    // These replies are hand-built wire encodings of that example. Malformed
    // variants deliberately alter its framing, as required by WP1.
    fn query() -> Query {
        Query {
            name: Name::from_text("x.w.example.").unwrap(),
            rtype: RecordType::MX,
        }
    }

    fn header(flags: u16, answer: u16, authority: u16, additional: u16) -> Vec<u8> {
        let mut bytes = vec![0x12, 0x34];
        for value in [flags, 1, answer, authority, additional] {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        bytes.extend_from_slice(query().name.as_wire());
        bytes.extend_from_slice(&[0, 15, 0, 1]);
        bytes
    }

    fn rr(bytes: &mut Vec<u8>, owner: &[u8], rtype: u16, class: u16, ttl: u32, data: &[u8]) {
        bytes.extend_from_slice(owner);
        bytes.extend_from_slice(&rtype.to_be_bytes());
        bytes.extend_from_slice(&class.to_be_bytes());
        bytes.extend_from_slice(&ttl.to_be_bytes());
        bytes.extend_from_slice(&u16::try_from(data.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(data);
    }

    fn mx_reply() -> Vec<u8> {
        let mut bytes = header(0x8000, 1, 0, 0);
        rr(
            &mut bytes,
            &[0xc0, 12],
            15,
            1,
            3600,
            &[0, 1, 2, b'x', b'x', 0xc0, 16],
        );
        bytes
    }

    fn opt(bytes: &mut Vec<u8>, ttl: u32) {
        rr(bytes, &[0], 41, 1232, ttl, &[]);
    }

    fn malformed(bytes: &[u8]) {
        assert!(matches!(
            Message::parse(bytes, &query()),
            Err(ParseError::MalformedReply)
        ));
    }

    #[test]
    fn query_encoding_matches_contract() {
        let mut expected = vec![0x12, 0x34, 1, 16, 0, 1, 0, 0, 0, 0, 0, 1];
        expected.extend_from_slice(b"\x01x\x01w\x07example\0\0\x0f\0\x01");
        expected.extend_from_slice(&[0, 0, 41, 4, 208, 0, 0, 128, 0, 0, 0]);
        assert_eq!(query().encode(0x1234), expected);
        let mixed = Query {
            name: Name::from_text("X.W.EXAMPLE.").unwrap(),
            rtype: RecordType::MX,
        };
        assert_eq!(&mixed.encode(0)[12..25], b"\x01X\x01W\x07EXAMPLE\0");
    }

    #[test]
    fn compressed_reply_expands_rdata() {
        let message = Message::parse(&mx_reply(), &query()).unwrap();
        assert_eq!(message.rcode, 0);
        assert_eq!(message.answer.len(), 1);
        let set = &message.answer[0];
        assert_eq!(set.owner, query().name);
        assert_eq!(set.rtype, RecordType::MX);
        assert_eq!(set.ttl, 3600);
        assert_eq!(set.records[0].canonical, b"\0\x01\x02xx\x07example\0");
    }

    #[test]
    fn received_high_bit_ttls_are_zero_without_changing_opt_fields() {
        for ttl in [0, 1, 0x7fff_ffff, 0x8000_0000, 0x8000_0001, u32::MAX] {
            let mut bytes = header(0x8000, 1, 0, 0);
            rr(
                &mut bytes,
                &[0xc0, 12],
                15,
                1,
                ttl,
                &[0, 1, 2, b'x', b'x', 0xc0, 16],
            );
            let parsed = Message::parse(&bytes, &query()).unwrap();
            let expected = if ttl & 0x8000_0000 != 0 { 0 } else { ttl };
            assert_eq!(parsed.answer[0].ttl, expected);
        }

        let fields = 0x8000_8001;
        let mut bytes = Vec::new();
        opt(&mut bytes, fields);
        let record = read_record(&mut Reader::new(&bytes, 0, bytes.len())).unwrap();
        assert_eq!(record.ttl, fields);

        let mut bytes = header(0x8000, 0, 0, 1);
        opt(&mut bytes, fields);
        assert!(matches!(
            Message::parse(&bytes, &query()),
            Err(ParseError::Rcode(2048))
        ));
    }

    #[test]
    fn malformed_compression_never_panics() {
        for bytes in [
            vec![0xc0, 0],
            vec![0xc0, 2, 0],
            vec![1, b'x', 0xc0, 0],
            vec![0x40, 0],
            vec![0x80, 0],
            vec![0xc0],
        ] {
            let mut offset = 0;
            assert_eq!(
                parse_name(&bytes, &mut offset, bytes.len()),
                Err(ParseError::MalformedReply)
            );
            assert_eq!(offset, 0);
        }
        let mut bytes = vec![0];
        for _ in 0..33 {
            let target = if bytes.len() == 1 { 0 } else { bytes.len() - 2 };
            bytes.extend_from_slice(&(0xc000 | u16::try_from(target).unwrap()).to_be_bytes());
        }
        let mut offset = 63;
        assert_eq!(
            parse_name(&bytes, &mut offset, bytes.len()).unwrap(),
            Name::root()
        );
        assert_eq!(offset, 65);
        offset = 65;
        assert_eq!(
            parse_name(&bytes, &mut offset, bytes.len()),
            Err(ParseError::MalformedReply)
        );
    }

    #[test]
    fn name_original_encoding_stays_inside_rdata() {
        let mut offset = 0;
        assert_eq!(
            parse_name(b"\x01x\0", &mut offset, 2),
            Err(ParseError::MalformedReply)
        );
        let mut offset = 3;
        assert_eq!(
            parse_name(b"\x01x\0\xc0\0", &mut offset, 4),
            Err(ParseError::MalformedReply)
        );
        let mut offset = 3;
        assert_eq!(
            parse_name(b"\x01x\0\xc0\0", &mut offset, 5).unwrap(),
            Name::from_text("x.").unwrap()
        );
        assert_eq!(offset, 5);
    }

    #[test]
    fn malformed_record_framing() {
        let mut bytes = mx_reply();
        bytes[7] = 2; // Count exceeds the available records.
        malformed(&bytes);
        let mut bytes = mx_reply();
        bytes.push(0); // Trailing data.
        malformed(&bytes);
        let mut bytes = header(0x8000, 1, 0, 0);
        rr(&mut bytes, &[0xc0, 12], 15, 1, 3600, &[0, 1, 0]);
        let length_offset = bytes.len() - 5;
        bytes[length_offset..length_offset + 2].copy_from_slice(&u16::MAX.to_be_bytes());
        malformed(&bytes);
        let mut bytes = header(0x8000, 0, 0, 2);
        opt(&mut bytes, 0);
        opt(&mut bytes, 0);
        malformed(&bytes);
        let mut bytes = header(0x8000, 1, 0, 0);
        opt(&mut bytes, 0);
        malformed(&bytes);
        let mut bytes = header(0x8000, 0, 1, 0);
        opt(&mut bytes, 0);
        malformed(&bytes);
    }

    #[test]
    fn truncated_and_rcodes() {
        assert!(matches!(
            Message::parse(&header(0x8200, 0, 0, 0), &query()),
            Err(ParseError::Truncated)
        ));
        assert!(matches!(
            Message::parse(&header(0x8002, 0, 0, 0), &query()),
            Err(ParseError::Rcode(2))
        ));
        let mut bytes = header(0x8000, 0, 0, 1);
        opt(&mut bytes, 1 << 24);
        assert!(matches!(
            Message::parse(&bytes, &query()),
            Err(ParseError::Rcode(16))
        ));
        let parsed = Message::parse(&header(0x8003, 0, 0, 0), &query()).unwrap();
        assert_eq!(parsed.rcode, 3);
        // Framing failures precede an rcode outcome.
        malformed(&header(0x8002, 1, 0, 0));
    }

    #[test]
    fn header_and_question_checks() {
        for flags in [0, 0x8800, 0x9000] {
            malformed(&header(flags, 0, 0, 0));
        }
        let mut bytes = header(0x8000, 0, 0, 0);
        bytes[5] = 0;
        malformed(&bytes);
        let mut bytes = header(0x8000, 0, 0, 0);
        bytes[13] = b'y';
        malformed(&bytes);
        let mut bytes = header(0x8000, 0, 0, 0);
        *bytes.last_mut().unwrap() = 2;
        malformed(&bytes);
        let mut bytes = header(0x8000, 0, 0, 0);
        let type_offset = bytes.len() - 3;
        bytes[type_offset] = 1;
        malformed(&bytes);
        let mixed = Query {
            name: Name::from_text("X.W.EXAMPLE.").unwrap(),
            rtype: RecordType::MX,
        };
        assert!(Message::parse(&mx_reply(), &mixed).is_ok());
        // AA and AD are immaterial.
        assert!(Message::parse(&header(0x8420, 0, 0, 0), &query()).is_ok());
        malformed(&vec![0; MAX_MESSAGE_BYTES + 1]);
    }

    #[test]
    fn groups_deduplicates_and_keeps_lowest_ttl() {
        let mut bytes = header(0x8000, 3, 1, 0);
        let compressed = [0, 1, 2, b'x', b'x', 0xc0, 16];
        rr(&mut bytes, &[0xc0, 12], 15, 1, 3600, &compressed);
        rr(
            &mut bytes,
            b"\x01X\x01W\x07EXAMPLE\0",
            15,
            1,
            300,
            b"\0\x01\x02XX\x07EXAMPLE\0",
        );
        rr(&mut bytes, &[0xc0, 12], 15, 1, 600, &compressed);
        rr(&mut bytes, &[0xc0, 12], 15, 1, 3600, &compressed);
        let parsed = Message::parse(&bytes, &query()).unwrap();
        assert_eq!(parsed.answer.len(), 1);
        assert_eq!(parsed.answer[0].records.len(), 1);
        assert_eq!(parsed.answer[0].ttl, 300);
        assert_eq!(parsed.authority.len(), 1);
        assert_eq!(parsed.authority[0].ttl, 3600);
    }

    #[test]
    fn additional_and_foreign_class_are_parsed_then_dropped() {
        let mut bytes = header(0x8000, 1, 0, 1);
        let data = [0, 1, 2, b'x', b'x', 0xc0, 16];
        rr(&mut bytes, &[0xc0, 12], 15, 2, 3600, &data);
        rr(&mut bytes, &[0xc0, 12], 15, 1, 3600, &data);
        let parsed = Message::parse(&bytes, &query()).unwrap();
        assert!(parsed.answer.is_empty());
        assert!(parsed.authority.is_empty());
        // An invalid typed layout is malformed even in a discarded record.
        let mut bytes = header(0x8000, 0, 0, 1);
        rr(&mut bytes, &[0xc0, 12], 15, 1, 3600, &[0]);
        malformed(&bytes);
        let mut bytes = header(0x8000, 1, 0, 0);
        rr(&mut bytes, &[0xc0, 12], 15, 2, 3600, &[0]);
        malformed(&bytes);
    }

    #[test]
    fn every_prefix_of_three_valid_replies_is_malformed() {
        let first = mx_reply();
        // RFC 4035, rfc4035.txt line 2307: example NS ns1.example.
        let mut second = header(0x8000, 0, 1, 1);
        rr(
            &mut second,
            &[0xc0, 16],
            2,
            1,
            3600,
            &[3, b'n', b's', b'1', 0xc0, 16],
        );
        opt(&mut second, 0);
        let third = header(0x8003, 0, 0, 0);
        for bytes in [first, second, third] {
            assert!(Message::parse(&bytes, &query()).is_ok());
            for end in 0..bytes.len() {
                malformed(&bytes[..end]);
            }
        }
    }

    fn hex(text: &str) -> Vec<u8> {
        text.as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(core::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    // RFC 6605, rfc6605.txt lines 216-219. The printed base64 signature is
    // decoded to hex here; its printed dates are converted to serial seconds.
    fn signature() -> Vec<u8> {
        let mut bytes = vec![0, 1, 13, 3];
        for value in [3600_u32, 1_284_026_679, 1_281_607_479] {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        bytes.extend_from_slice(&55648_u16.to_be_bytes());
        bytes.extend_from_slice(Name::from_text("example.net.").unwrap().as_wire());
        bytes.extend(hex(concat!(
            "ab1eb02d8aa687e97da0229337aa8873e6f0eb26be289f28333d183f5d3b7a95c",
            "0c869adfb748daee3c5286eed6682c12e5533186baced9c26c167a9ebae950b"
        )));
        bytes
    }

    #[test]
    fn signatures_attach_by_owner_type_and_section_in_receive_order() {
        // RFC 6605, rfc6605.txt line 215: owner and A RDATA.
        let owner = Name::from_text("www.example.net.").unwrap();
        let mut bytes = header(0x8000, 5, 1, 0);
        let first = signature();
        let mut second = first.clone();
        second[17] ^= 1; // Deliberately alter the key tag to distinguish order.
        rr(&mut bytes, owner.as_wire(), 46, 1, 3600, &first);
        rr(&mut bytes, owner.as_wire(), 46, 1, 17, &first); // Duplicate, lower TTL.
        rr(&mut bytes, owner.as_wire(), 46, 1, 3600, &second);
        rr(&mut bytes, query().name.as_wire(), 46, 1, 3600, &first); // Orphan.
        rr(&mut bytes, owner.as_wire(), 1, 1, 3600, &[192, 0, 2, 1]);
        rr(&mut bytes, owner.as_wire(), 1, 1, 3600, &[192, 0, 2, 1]);
        let parsed = Message::parse(&bytes, &query()).unwrap();
        assert_eq!(parsed.answer.len(), 1);
        let sigs = &parsed.answer[0].rrsigs;
        assert_eq!(sigs.len(), 2);
        assert_eq!(sigs[0].key_tag, 55648);
        assert_eq!(sigs[1].key_tag, 55649);
        assert_eq!(sigs[0].received_ttl, 17);
        assert_eq!(sigs[1].received_ttl, 3600);
        assert_eq!(parsed.answer[0].ttl, 3600);
        assert!(parsed.authority[0].rrsigs.is_empty());
    }

    #[test]
    fn signature_cap_keeps_first_received_unique_signatures() {
        let owner = Name::from_text("www.example.net.").unwrap();
        let mut bytes = header(0x8000, 34, 0, 0);
        rr(&mut bytes, owner.as_wire(), 1, 1, 3600, &[192, 0, 2, 1]);
        for index in 0..33_u8 {
            let mut sig = signature();
            sig[17] = index; // Structural mutation of the cited RFC signature.
            rr(&mut bytes, owner.as_wire(), 46, 1, 3600, &sig);
        }
        let parsed = Message::parse(&bytes, &query()).unwrap();
        let sigs = &parsed.answer[0].rrsigs;
        assert_eq!(sigs.len(), MAX_RRSIGS_PER_RRSET);
        for (index, signature) in sigs.iter().enumerate() {
            assert_eq!(signature.key_tag & 255, u16::try_from(index).unwrap());
        }
    }

    #[test]
    fn rrset_and_dnskey_record_limits_are_enforced_after_dedup() {
        for count in [MAX_RRSET_RECORDS, MAX_RRSET_RECORDS + 1] {
            let mut bytes = header(0x8000, u16::try_from(count).unwrap(), 0, 0);
            for index in 0..count {
                // Distinct preference mutations of the cited RFC 4035 MX.
                let mut data = u16::try_from(index).unwrap().to_be_bytes().to_vec();
                data.extend_from_slice(b"\x02xx\x07example\0");
                rr(&mut bytes, &[0xc0, 12], 15, 1, 3600, &data);
            }
            if count == MAX_RRSET_RECORDS {
                assert_eq!(
                    Message::parse(&bytes, &query()).unwrap().answer[0]
                        .records
                        .len(),
                    count
                );
            } else {
                malformed(&bytes);
            }
        }
        // RFC 6605, rfc6605.txt lines 207-209: DNSKEY public key, decoded
        // from the printed base64. Flags are varied to test unique counts.
        let key = hex(concat!(
            "1a88c88615d437fbb8bf9e1942a1929f28562706ae6c2bd399e7b1bfb6d1e9e7",
            "5b92b4aa42917ae1c61b701ef035c3fe7be3009cbafe5a2f71316c902dcf0d00"
        ));
        for count in [MAX_DNSKEYS, MAX_DNSKEYS + 1] {
            let mut bytes = header(0x8000, u16::try_from(count).unwrap(), 0, 0);
            for index in 0..count {
                let mut data = u16::try_from(index).unwrap().to_be_bytes().to_vec();
                data.extend_from_slice(&[3, 13]);
                data.extend_from_slice(&key);
                rr(&mut bytes, &[0xc0, 12], 48, 1, 3600, &data);
            }
            if count == MAX_DNSKEYS {
                assert_eq!(
                    Message::parse(&bytes, &query()).unwrap().answer[0]
                        .records
                        .len(),
                    count
                );
            } else {
                malformed(&bytes);
            }
        }
        let count = MAX_RRSET_RECORDS + 1;
        let mut bytes = header(0x8000, u16::try_from(count).unwrap(), 0, 0);
        for _ in 0..count {
            rr(
                &mut bytes,
                &[0xc0, 12],
                15,
                1,
                3600,
                &[0, 1, 2, b'x', b'x', 0xc0, 16],
            );
        }
        assert_eq!(
            Message::parse(&bytes, &query()).unwrap().answer[0]
                .records
                .len(),
            1
        );
    }
}
