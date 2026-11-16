// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};

use crate::limits::{
    MAX_ALIAS_LINKS, MAX_DNSKEYS, MAX_EXTENDED_ERRORS, MAX_MESSAGE_BYTES, MAX_POINTER_HOPS,
    MAX_RRSET_RECORDS, MAX_RRSIGS_PER_RRSET,
};
use crate::rdata::{self, Rdata, RdataKind, Rrsig};
use crate::{Answer, Name, Outcome, Query, RecordType, Reply, ReplyError};

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
    /// The header's four RCODE bits. The OPT record's extended bits are
    /// not read, as unbound and dnsmasq decide on the header alone.
    pub(crate) rcode: u16,
    /// The answer section's record sets, in canonical order of owner, then
    /// type.
    pub(crate) answer: Vec<Rrset>,
    pub(crate) authority: Vec<Rrset>,
    /// Where the authority section's class IN SOA sets are in `authority`,
    /// in the order each set's first record was received. A set's TTL is
    /// the lowest of its records', duplicates included.
    pub(crate) soas: Vec<usize>,
    /// RRSIG records received in the answer and authority sections.
    pub(crate) signatures: usize,
    /// Extended DNS Error info codes from the OPT record, in received order.
    pub(crate) extended_errors: Vec<u16>,
}

impl Query {
    /// Encode this query as a DNS message with the given id.
    ///
    /// The message sets RD, CD and the EDNS DO bit, and advertises a
    /// 1232-byte EDNS buffer. Signed replies often exceed 1232 bytes,
    /// mostly for RSA and ML-DSA keys. A reply with TC set ends the
    /// validation as [`Unanswered`](crate::Verdict::Unanswered) with
    /// [`Truncated`](crate::UnansweredCause::Truncated). So when a UDP
    /// reply has TC set, retry the query over TCP and supply that reply
    /// instead.
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
        let mut opt_seen = false;
        let mut extended_errors = Vec::new();
        let mut signatures = 0_usize;
        for (section, count) in [(0, answers), (1, authorities), (2, additionals)] {
            for _ in 0..count {
                let record = read_record(&mut reader)?;
                if record.rtype == RecordType::OPT.0 {
                    if section != 2 || opt_seen {
                        return Err(ParseError::MalformedReply);
                    }
                    opt_seen = true;
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
        // Bytes after the last record are passed over, as unbound passes
        // them (msgparse.c, parse_packet: "spurious data at end of packet").
        let soas = authority.arrivals(RecordType::SOA);
        Ok(Self {
            rcode: flags & 0x000f,
            answer: answer.finish(),
            authority: authority.finish(),
            soas,
            signatures,
            extended_errors,
        })
    }

    /// The answer section's set of `rtype` owned by `owner`.
    pub(crate) fn answer_set(&self, owner: &Name, rtype: RecordType) -> Option<&Rrset> {
        self.answer
            .binary_search_by(|set| set.owner.cmp(owner).then(set.rtype.cmp(&rtype)))
            .ok()
            .and_then(|at| self.answer.get(at))
    }

    /// How long a negative answer for `name` holds, by the first SOA in
    /// received order owned by `name` or a zone above it: the lower of its
    /// TTL and its minimum (RFC 2308 section 5). None when there is no such
    /// SOA, or the first is short of its minimum: a later SOA is not
    /// taken in its place. This is dnsmasq's rule (rfc1035.c, find_soa).
    /// A set's records keep their received order, so the first record of
    /// the first set owned there is the first SOA received there.
    pub(crate) fn negative_ttl(&self, name: &Name) -> Option<u32> {
        let set = self
            .soas
            .iter()
            .filter_map(|at| self.authority.get(*at))
            .find(|set| name.is_at_or_below(&set.owner))?;
        let RdataKind::Soa(soa) = &set.records.first()?.kind else {
            return None;
        };
        soa.minimum().map(|minimum| set.ttl.min(minimum))
    }
}

impl Reply {
    /// Reads `message` as the server's reply to `query`, as it was sent:
    /// nothing is validated. The message id is not checked; matching a
    /// reply to its query is the caller's job.
    ///
    /// The answer starts at the query's name and follows the CNAME records
    /// the reply holds, up to [`MAX_ALIAS_LINKS`](crate::limits::MAX_ALIAS_LINKS)
    /// links. A reply whose aliases loop, or go on past that, is
    /// [`Malformed`](ReplyError::Malformed).
    ///
    /// The answer's outcome is [`Outcome::Records`] when the reply holds
    /// records of the query's type at the last name followed. Otherwise it
    /// is [`Outcome::NoSuchName`] for NXDOMAIN, and [`Outcome::NoData`] for
    /// NOERROR. Its TTL is the lowest of the aliases followed and:
    ///
    /// - for records, the records';
    /// - for an alias still to ask about (see [`Reply::alias`]), nothing
    ///   more;
    /// - otherwise [`Reply::negative_ttl`], or 0 without one.
    ///
    /// The whole message is read before its response code: a reply that
    /// does not echo the question, or whose body is broken, is
    /// [`Malformed`](ReplyError::Malformed) even when its header says
    /// REFUSED or SERVFAIL. Only a well-formed reply is
    /// [`Rcode`](ReplyError::Rcode).
    ///
    /// A query for RRSIG or OPT records is
    /// [`UnsupportedType`](ReplyError::UnsupportedType): signatures are
    /// kept with the records they cover and the OPT record is read as the
    /// reply's options, so neither can be an answer.
    pub fn read(query: &Query, message: &[u8]) -> Result<Reply, ReplyError> {
        if matches!(query.rtype, RecordType::RRSIG | RecordType::OPT) {
            return Err(ReplyError::UnsupportedType(query.rtype));
        }
        let message = Message::parse_any(message, query).map_err(|error| match error {
            ParseError::Truncated => ReplyError::Truncated,
            _ => ReplyError::Malformed,
        })?;
        if !matches!(message.rcode, 0 | 3) {
            return Err(ReplyError::Rcode(message.rcode));
        }
        let mut current = query.name.clone();
        let mut chain = vec![current.clone()];
        let mut ttl = u32::MAX;
        let records = loop {
            if let Some(set) = message.answer_set(&current, query.rtype) {
                break Some(set);
            }
            let Some(alias) = message.answer_set(&current, RecordType::CNAME) else {
                break None;
            };
            // Of several, the first received.
            let Some(RdataKind::Name(target)) = alias.records.first().map(|record| &record.kind)
            else {
                return Err(ReplyError::Malformed);
            };
            if chain.len() > MAX_ALIAS_LINKS || chain.contains(target) {
                return Err(ReplyError::Malformed);
            }
            ttl = ttl.min(alias.ttl);
            current = target.clone();
            chain.push(current.clone());
        };
        let negative_ttl = message
            .negative_ttl(&current)
            .map(|negative| negative.min(ttl));
        let (outcome, ttl) = match records {
            Some(set) => (
                Outcome::Records {
                    name: current,
                    rdata: set
                        .records
                        .iter()
                        .map(|record| record.uncompressed.clone())
                        .collect(),
                },
                ttl.min(set.ttl),
            ),
            None if message.rcode == 3 => (Outcome::NoSuchName, negative_ttl.unwrap_or(0)),
            None if chain.len() > 1 => (Outcome::NoData, ttl),
            None => (Outcome::NoData, negative_ttl.unwrap_or(0)),
        };
        Ok(Reply {
            answer: Answer {
                outcome,
                ttl,
                alias_chain: chain,
                rtype: query.rtype,
            },
            rcode: message.rcode,
            negative_ttl,
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
    // How many sets the section held when this one's first record came.
    arrival: usize,
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
        let arrival = self.sets.len();
        // The owner is cloned once a set, not once a record.
        let builder = self
            .sets
            .entry((record.owner, rtype))
            .or_insert_with_key(|(owner, _)| SetBuilder {
                set: Rrset {
                    owner: owner.clone(),
                    rtype,
                    ttl: record.ttl,
                    records: Vec::new(),
                    rrsigs: Vec::new(),
                },
                arrival,
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

    /// Where the sets of `rtype` will be in [`Section::finish`]'s list, in
    /// the order each set's first record was received.
    fn arrivals(&self, rtype: RecordType) -> Vec<usize> {
        let mut sets: Vec<(usize, usize)> = self
            .sets
            .values()
            .enumerate()
            .filter(|(_, builder)| builder.set.rtype == rtype)
            .map(|(at, builder)| (builder.arrival, at))
            .collect();
        sets.sort_unstable();
        sets.into_iter().map(|(_, at)| at).collect()
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

        // The OPT's extended RCODE bits do not change the header's NOERROR.
        let mut bytes = header(0x8000, 0, 0, 1);
        opt(&mut bytes, fields);
        assert_eq!(Message::parse(&bytes, &query()).unwrap().rcode, 0);
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
        // Pointer n, at offset 2n - 1, points at pointer n - 1, and the first
        // at the root name: reading from pointer n follows n pointers.
        let mut bytes = vec![0];
        for _ in 0..=MAX_POINTER_HOPS {
            let target = if bytes.len() == 1 { 0 } else { bytes.len() - 2 };
            bytes.extend_from_slice(&(0xc000 | u16::try_from(target).unwrap()).to_be_bytes());
        }
        for hops in [1, 40, MAX_POINTER_HOPS] {
            let mut offset = 2 * hops - 1;
            assert_eq!(
                parse_name(&bytes, &mut offset, bytes.len()).unwrap(),
                Name::root()
            );
            assert_eq!(offset, 2 * hops + 1);
        }
        let mut offset = 2 * (MAX_POINTER_HOPS + 1) - 1;
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
        // Trailing data is passed over, and the records read the same.
        let mut bytes = mx_reply();
        bytes.extend_from_slice(&[0, 0, 0xff]);
        let padded = Message::parse(&bytes, &query()).unwrap();
        let plain = Message::parse(&mx_reply(), &query()).unwrap();
        assert_eq!(padded.answer.len(), 1);
        assert_eq!(padded.answer[0].records, plain.answer[0].records);
        assert_eq!(padded.answer[0].ttl, plain.answer[0].ttl);
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
        // Only the header's four bits decide: an OPT's extended RCODE
        // leaves NOERROR and NXDOMAIN as they are.
        for (flags, rcode) in [(0x8000, 0), (0x8003, 3)] {
            let mut bytes = header(flags, 0, 0, 1);
            opt(&mut bytes, 1 << 24);
            assert_eq!(Message::parse(&bytes, &query()).unwrap().rcode, rcode);
        }
        let mut bytes = header(0x8002, 0, 0, 1);
        opt(&mut bytes, 1 << 24);
        assert!(matches!(
            Message::parse(&bytes, &query()),
            Err(ParseError::Rcode(2))
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

    // Replies read as sent, for an A query of x.w.example (RFC 4035's name).
    fn a_query() -> Query {
        Query::new(query().name, RecordType::A)
    }

    fn a_header(rcode: u16, answer: u16, authority: u16, additional: u16) -> Vec<u8> {
        let mut bytes = header(0x8000 | rcode, answer, authority, additional);
        let at = bytes.len() - 3;
        bytes[at] = 1;
        bytes
    }

    fn soa_data(minimum: u32, numbers: usize, trailing: usize) -> Vec<u8> {
        // RFC 5155, rfc5155.txt lines 1935..1936: the SOA's names and
        // numbers, with the minimum varied.
        let mut data = Name::from_text("ns1.example.").unwrap().as_wire().to_vec();
        data.extend(Name::from_text("bugs.x.w.example.").unwrap().as_wire());
        let fields: Vec<u8> = [1_u32, 3600, 300, 3_600_000, minimum]
            .iter()
            .flat_map(|field| field.to_be_bytes())
            .collect();
        data.extend_from_slice(&fields[..numbers]);
        data.extend(core::iter::repeat_n(0, trailing));
        data
    }

    fn read(bytes: &[u8]) -> Result<Reply, ReplyError> {
        Reply::read(&a_query(), bytes)
    }

    #[test]
    fn a_reply_read_as_sent_gives_addresses_and_their_lowest_ttl() {
        let mut bytes = a_header(0, 2, 0, 0);
        rr(&mut bytes, &[0xc0, 12], 1, 1, 300, &[192, 0, 2, 1]);
        rr(&mut bytes, &[0xc0, 12], 1, 1, 60, &[192, 0, 2, 2]);
        let reply = read(&bytes).unwrap();
        assert_eq!(reply.rcode(), 0);
        assert_eq!(reply.answer().addresses().len(), 2);
        assert_eq!(reply.answer().ttl(), 60);
        assert_eq!(reply.answer().alias_chain(), [query().name]);
        assert_eq!(reply.alias(), None);
    }

    #[test]
    fn a_reply_read_as_sent_follows_its_aliases_and_names_the_one_left_to_ask() {
        let target = Name::from_text("xx.example.").unwrap();
        let other = Name::from_text("yy.example.").unwrap();
        // x.w.example CNAME xx.example, which has the address.
        let mut bytes = a_header(0, 2, 0, 0);
        rr(&mut bytes, &[0xc0, 12], 5, 1, 30, target.as_wire());
        rr(&mut bytes, target.as_wire(), 1, 1, 300, &[192, 0, 2, 7]);
        let reply = read(&bytes).unwrap();
        assert_eq!(reply.answer().addresses().len(), 1);
        assert_eq!(reply.answer().ttl(), 30);
        assert_eq!(reply.answer().alias_chain(), [query().name, target.clone()]);
        assert_eq!(reply.alias(), None);
        // The alias's target is not in the reply: it is the one to ask.
        let mut bytes = a_header(0, 1, 0, 0);
        rr(&mut bytes, &[0xc0, 12], 5, 1, 30, target.as_wire());
        let reply = read(&bytes).unwrap();
        assert_eq!(reply.alias(), Some(&target));
        assert_eq!(reply.answer().ttl(), 30);
        // Under NXDOMAIN there is nothing more to ask.
        let mut bytes = a_header(3, 1, 0, 0);
        rr(&mut bytes, &[0xc0, 12], 5, 1, 30, target.as_wire());
        let reply = read(&bytes).unwrap();
        assert_eq!(reply.alias(), None);
        assert_eq!(*reply.answer().outcome(), Outcome::NoSuchName);
        // A loop is unreadable.
        let mut bytes = a_header(0, 2, 0, 0);
        rr(&mut bytes, &[0xc0, 12], 5, 1, 30, other.as_wire());
        rr(&mut bytes, other.as_wire(), 5, 1, 30, &[0xc0, 12]);
        assert_eq!(read(&bytes), Err(ReplyError::Malformed));
    }

    #[test]
    fn a_reply_read_as_sent_holds_its_negative_answer_by_the_first_soa_received() {
        let parent = Name::from_text("example.").unwrap();
        let child = Name::from_text("w.example.").unwrap();
        let elsewhere = Name::from_text("other.").unwrap();
        // Classes IN and CH.
        let (inet, chaos) = (1, 3);
        let negative = |soas: &[(&Name, u16, u32, Vec<u8>)]| {
            let mut bytes = a_header(3, 0, u16::try_from(soas.len()).unwrap(), 0);
            for (owner, class, ttl, data) in soas {
                rr(&mut bytes, owner.as_wire(), 6, *class, *ttl, data);
            }
            read(&bytes).unwrap()
        };
        for (soas, held) in [
            // Received order decides, not the zones' order.
            (
                vec![
                    (&child, inet, 900, soa_data(60, 20, 0)),
                    (&parent, inet, 900, soa_data(120, 20, 0)),
                ],
                Some(60),
            ),
            (
                vec![
                    (&parent, inet, 900, soa_data(120, 20, 0)),
                    (&child, inet, 900, soa_data(60, 20, 0)),
                ],
                Some(120),
            ),
            // Another zone's is passed over.
            (
                vec![
                    (&elsewhere, inet, 900, soa_data(30, 20, 0)),
                    (&parent, inet, 900, soa_data(120, 20, 0)),
                ],
                Some(120),
            ),
            // So is one of another class.
            (
                vec![
                    (&child, chaos, 900, soa_data(30, 20, 0)),
                    (&parent, inet, 900, soa_data(120, 20, 0)),
                ],
                Some(120),
            ),
            // The record's own TTL bounds the minimum.
            (vec![(&parent, inet, 20, soa_data(120, 20, 0))], Some(20)),
            // A duplicate keeps the lower TTL, in the first one's place.
            (
                vec![
                    (&parent, inet, 900, soa_data(120, 20, 0)),
                    (&child, inet, 900, soa_data(60, 20, 0)),
                    (&parent, inet, 10, soa_data(120, 20, 0)),
                ],
                Some(10),
            ),
            // Bytes after the numbers are passed over.
            (vec![(&parent, inet, 900, soa_data(120, 20, 3))], Some(120)),
            // A short SOA holds nothing, and a later one is not taken.
            (
                vec![
                    (&child, inet, 900, soa_data(60, 19, 0)),
                    (&parent, inet, 900, soa_data(120, 20, 0)),
                ],
                None,
            ),
            (vec![], None),
        ] {
            let reply = negative(&soas);
            assert_eq!(reply.negative_ttl(), held, "{soas:?}");
            assert_eq!(reply.answer().ttl(), held.unwrap_or(0), "{soas:?}");
            assert_eq!(*reply.answer().outcome(), Outcome::NoSuchName);
        }
    }

    #[test]
    fn a_reply_read_as_sent_reads_a_dot_inside_a_label_as_a_byte() {
        // The one label "x.w" is not the name x.w of two labels.
        let dotted = Name::from_wire(b"\x03x.w\x07example\0").unwrap();
        let two = Name::from_wire(b"\x01x\x01w\x07example\0").unwrap();
        assert_ne!(dotted, two);
        let answer = |asked: &Name, sent: &Name| {
            let mut bytes = vec![0x12, 0x34, 0x80, 0, 0, 1, 0, 1, 0, 0, 0, 0];
            bytes.extend_from_slice(sent.as_wire());
            bytes.extend_from_slice(&[0, 1, 0, 1]);
            rr(&mut bytes, &[0xc0, 12], 1, 1, 60, &[192, 0, 2, 1]);
            Reply::read(&Query::new(asked.clone(), RecordType::A), &bytes)
        };
        assert_eq!(
            answer(&dotted, &dotted).unwrap().answer().addresses().len(),
            1
        );
        assert_eq!(answer(&dotted, &two), Err(ReplyError::Malformed));
        assert_eq!(answer(&two, &dotted), Err(ReplyError::Malformed));
    }

    #[test]
    fn a_reply_read_as_sent_fails_on_broken_authority_framing_and_signature_queries() {
        // One SOA announced in the authority section, cut inside its data.
        let mut bytes = a_header(3, 0, 1, 0);
        let mut soa = Vec::new();
        rr(&mut soa, &[0xc0, 12], 6, 1, 900, &soa_data(60, 20, 0));
        bytes.extend_from_slice(&soa[..soa.len() - 4]);
        assert_eq!(read(&bytes), Err(ReplyError::Malformed));
        // A refusal that echoes no question is unreadable, not a refusal.
        let mut refused = a_header(5, 0, 0, 0);
        refused[5] = 0;
        refused.truncate(12);
        assert_eq!(read(&refused), Err(ReplyError::Malformed));
        for rtype in [RecordType::RRSIG, RecordType::OPT] {
            let query = Query::new(query().name, rtype);
            assert_eq!(
                Reply::read(&query, &a_header(0, 0, 0, 0)),
                Err(ReplyError::UnsupportedType(rtype))
            );
        }
    }

    #[test]
    fn a_reply_read_as_sent_decides_on_the_header_rcode_and_reads_past_padding() {
        let mut bytes = a_header(0, 0, 0, 1);
        opt(&mut bytes, 1 << 24);
        let reply = read(&bytes).unwrap();
        assert_eq!(reply.rcode(), 0);
        assert_eq!(*reply.answer().outcome(), Outcome::NoData);
        let mut bytes = a_header(0, 1, 0, 0);
        rr(&mut bytes, &[0xc0, 12], 1, 1, 60, &[192, 0, 2, 1]);
        bytes.extend_from_slice(&[0; 7]);
        assert_eq!(read(&bytes).unwrap().answer().addresses().len(), 1);
        assert_eq!(read(&a_header(2, 0, 0, 0)), Err(ReplyError::Rcode(2)));
        assert_eq!(read(&a_header(5, 0, 0, 0)), Err(ReplyError::Rcode(5)));
        let mut truncated = a_header(0, 0, 0, 0);
        truncated[2] |= 0x02;
        assert_eq!(read(&truncated), Err(ReplyError::Truncated));
        let mut cut = a_header(0, 1, 0, 0);
        cut.extend_from_slice(&[0xc0, 12, 0, 1]);
        assert_eq!(read(&cut), Err(ReplyError::Malformed));
    }
}
