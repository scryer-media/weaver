// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The reading of a DNS reply: its header, the question it answers, its
// records and names, and what it says about the name asked.
use super::INVALID;
use crate::TunnelError;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

fn invalid() -> TunnelError {
    TunnelError::Engine(INVALID.into())
}

// The error code a reply to query `id` carries in place of an answer, when
// it is one: the server could not, or would not, resolve the name.
pub(super) fn refusal(bytes: &[u8], id: u16) -> Option<u16> {
    let flags = u16_at(bytes, 2).ok()?;
    let rcode = flags & 0x000f;
    (u16_at(bytes, 0).ok()? == id && flags & 0x8000 != 0 && !matches!(rcode, 0 | 3))
        .then_some(rcode)
}

pub(super) fn u16_at(bytes: &[u8], at: usize) -> Result<u16, TunnelError> {
    Ok(u16::from_be_bytes(
        bytes
            .get(at..at + 2)
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
    ))
}

fn u32_at(bytes: &[u8], at: usize) -> Result<u32, TunnelError> {
    Ok(u32::from_be_bytes(
        bytes
            .get(at..at + 4)
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
    ))
}

pub(super) fn read_name(bytes: &[u8], cursor: &mut usize) -> Result<String, TunnelError> {
    let mut at = *cursor;
    let mut jumped = false;
    let mut labels = Vec::new();
    let mut length = 0;
    for _ in 0..128 {
        let head = *bytes.get(at).ok_or_else(invalid)?;
        if head & 0xc0 == 0xc0 {
            let target = (u16_at(bytes, at)? & 0x3fff) as usize;
            if target >= at {
                return Err(invalid());
            }
            if !jumped {
                *cursor = at + 2;
                jumped = true;
            }
            at = target;
            continue;
        }
        if head & 0xc0 != 0 {
            return Err(invalid());
        }
        at += 1;
        if head == 0 {
            if !jumped {
                *cursor = at;
            }
            return Ok(labels.join("."));
        }
        let label = bytes.get(at..at + head as usize).ok_or_else(invalid)?;
        if !label.is_ascii() {
            return Err(invalid());
        }
        length += label.len() + 1;
        if length > 254 {
            return Err(invalid());
        }
        labels.push(
            std::str::from_utf8(label)
                .map_err(|_| invalid())?
                .to_ascii_lowercase(),
        );
        at += head as usize;
    }
    Err(invalid())
}

// One resource record: its owner, type, class and TTL, and where its data
// lies in the message.
struct Record {
    owner: String,
    kind: u16,
    class: u16,
    ttl: u32,
    data: usize,
    size: usize,
}

fn record(bytes: &[u8], at: &mut usize) -> Result<Record, TunnelError> {
    let owner = read_name(bytes, at)?;
    let kind = u16_at(bytes, *at)?;
    let class = u16_at(bytes, *at + 2)?;
    // A TTL with the top bit set is read as zero (RFC 2181 §8).
    let ttl = match u32_at(bytes, *at + 4)? {
        ttl if ttl > i32::MAX as u32 => 0,
        ttl => ttl,
    };
    let size = u16_at(bytes, *at + 8)? as usize;
    let data = *at + 10;
    bytes.get(data..data + size).ok_or_else(invalid)?;
    *at = data + size;
    Ok(Record {
        owner,
        kind,
        class,
        ttl,
        data,
        size,
    })
}

// What one response said about the name asked.
pub(super) struct Reply {
    pub(super) addresses: Vec<IpAddr>,
    // The name the answer ended at, when it is an alias still to ask for.
    pub(super) alias: Option<String>,
    // The shortest TTL of the records the answer used.
    pub(super) ttl: u32,
    // For no address, how long that holds: the zone's negative TTL, from
    // the first SOA in the authority section owned by the name or a zone
    // above it, when there is one.
    pub(super) negative: Option<u32>,
    // The server said the name does not exist.
    pub(super) nonexistent: bool,
}

pub(super) fn parse_response(
    bytes: &[u8],
    id: u16,
    host: &str,
    qtype: u16,
) -> Result<Reply, TunnelError> {
    let flags = u16_at(bytes, 2)?;
    // A whole reply to a standard query that answers, or says the name does
    // not exist.
    let rcode = flags & 0x000f;
    if u16_at(bytes, 0)? != id
        || flags & 0xfa00 != 0x8000
        || !matches!(rcode, 0 | 3)
        || u16_at(bytes, 4)? != 1
    {
        return Err(invalid());
    }
    let count = u16_at(bytes, 6)? as usize;
    let authority = u16_at(bytes, 8)? as usize;
    if count > 128 || authority > 128 {
        return Err(invalid());
    }
    let mut at = 12;
    if read_name(bytes, &mut at)? != host
        || u16_at(bytes, at)? != qtype
        || u16_at(bytes, at + 2)? != 1
    {
        return Err(invalid());
    }
    at += 4;
    let mut records = Vec::new();
    let mut aliases = Vec::new();
    for _ in 0..count {
        let record = record(bytes, &mut at)?;
        if record.class != 1 {
            continue;
        }
        let data = &bytes[record.data..record.data + record.size];
        match (record.kind, record.size) {
            (1, 4) => records.push((
                record.owner,
                IpAddr::V4(Ipv4Addr::new(data[0], data[1], data[2], data[3])),
                record.ttl,
            )),
            (28, 16) => records.push((
                record.owner,
                IpAddr::V6(Ipv6Addr::from(
                    <[u8; 16]>::try_from(data).map_err(|_| invalid())?,
                )),
                record.ttl,
            )),
            (5, _) => {
                let mut cursor = record.data;
                let alias = read_name(bytes, &mut cursor)?;
                if cursor != record.data + record.size {
                    return Err(invalid());
                }
                aliases.push((record.owner, alias, record.ttl));
            }
            _ => {}
        }
    }
    let mut canonical = host.to_owned();
    let mut ttl = u32::MAX;
    for _ in 0..8 {
        match aliases.iter().find(|(owner, ..)| owner == &canonical) {
            Some((_, alias, held)) => {
                canonical = alias.clone();
                ttl = ttl.min(*held);
            }
            None => break,
        }
    }
    if aliases.iter().any(|(owner, ..)| owner == &canonical) {
        return Err(invalid());
    }
    let mut addresses = Vec::new();
    for (owner, ip, held) in records {
        if owner == canonical && ip.is_ipv4() == (qtype == 1) {
            addresses.push(ip);
            ttl = ttl.min(held);
        }
    }
    // A name that does not exist leaves no alias to ask for.
    let follow = canonical != host && rcode == 0;
    let negative = if addresses.is_empty() && !follow {
        negative_ttl(bytes, at, authority, &canonical)
    } else {
        None
    };
    let alias = follow.then_some(canonical);
    Ok(Reply {
        addresses,
        alias,
        ttl,
        negative,
        nonexistent: rcode == 3,
    })
}

// The negative TTL from the first SOA among the `count` authority records
// at `at` that is owned by `name` or a zone above it: the lower of the
// record's own TTL and the SOA's minimum (RFC 2308 §5), as dnsmasq takes
// it. A malformed authority section gives none rather than failing the
// answer.
fn negative_ttl(bytes: &[u8], mut at: usize, count: usize, name: &str) -> Option<u32> {
    for _ in 0..count {
        let record = record(bytes, &mut at).ok()?;
        if record.kind != 6 || record.class != 1 || !at_or_below(name, &record.owner) {
            continue;
        }
        let mut cursor = record.data;
        read_name(bytes, &mut cursor).ok()?;
        read_name(bytes, &mut cursor).ok()?;
        // Serial, refresh, retry and expire come before the minimum. Bytes
        // after them are passed over, as dnsmasq passes them (rfc1035.c,
        // find_soa: CHECK_LEN for 20).
        if cursor + 20 > record.data + record.size {
            return None;
        }
        let minimum = u32_at(bytes, cursor + 16).ok()?;
        return Some(record.ttl.min(minimum));
    }
    None
}

// Whether `name` is `zone` or a name inside it. Both are lower case with no
// final dot, the root empty.
fn at_or_below(name: &str, zone: &str) -> bool {
    zone.is_empty()
        || name == zone
        || name
            .strip_suffix(zone)
            .is_some_and(|head| head.ends_with('.'))
}
