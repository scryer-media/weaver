// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Answers taken as the server sent them: each family's walk along the
// aliases its answers name.
use super::{Asking, Judge, host_name};
use crate::dns::{Exchanged, FAMILIES, Found, exchange, message::refusal};
use weaver_dnssec::{Name, Query, RecordType, Reply};

// The most queries one walk sends, and the most aliases one reply's answer
// may follow.
const MOST: usize = 8;

// One family's walk along the aliases its answers name, under `AsSent`.
struct Walk {
    qtype: u16,
    // The name asked next, in lower case.
    name: Name,
    // The shortest TTL of the aliases followed so far.
    ttl: u32,
    // Queries sent so far.
    sent: usize,
    // The id the last query went out with.
    id: u16,
    // Whether its last query is given and its reply not yet taken.
    out: bool,
    // What the walk came to, once it has.
    done: Option<Result<Found, Exchanged>>,
}

impl Walk {
    // The query for the name the walk is at, or None when it is done. `id`
    // gives the id it goes out with, as the server's wire sends it.
    fn packet(&mut self, id: impl FnOnce() -> u16) -> Option<Vec<u8>> {
        if self.done.is_some() {
            return None;
        }
        // The root is no name to ask for.
        if self.sent == MOST || self.name.label_count() == 0 {
            self.done = Some(Err(Exchanged::Invalid));
            return None;
        }
        self.sent += 1;
        self.id = id();
        Some(packet(self.id, &self.name, self.qtype))
    }

    // Takes the reply to the walk's last query: an answer, or an alias to
    // ask for next. A reply is taken only under the id its query went out
    // with.
    fn take(&mut self, reply: exchange::Reply) {
        let bytes = match reply {
            Ok(bytes) => bytes,
            Err(failed) => {
                self.done = Some(Err(failed));
                return;
            }
        };
        let query = Query::new(self.name.clone(), RecordType::new(self.qtype));
        let reply = match read(&query, self.id, &bytes) {
            Ok(reply) => reply,
            Err(failed) => {
                self.done = Some(Err(failed));
                return;
            }
        };
        let answer = reply.answer();
        let addresses = answer.addresses();
        if !addresses.is_empty() {
            self.done = Some(Ok(Found {
                addresses,
                ttl: Some(self.ttl.min(answer.ttl())),
                nonexistent: false,
                insecure: None,
            }));
            return;
        }
        match reply.alias() {
            Some(alias) => {
                self.ttl = self.ttl.min(answer.ttl());
                self.name = alias.to_lowercase();
            }
            None => {
                self.done = Some(Ok(Found {
                    addresses: Vec::new(),
                    ttl: reply.negative_ttl().map(|negative| negative.min(self.ttl)),
                    nonexistent: reply.rcode() == 3,
                    insecure: None,
                }));
            }
        }
    }
}

// A query for `name`'s records of `qtype` under `id`, as a lookup that takes
// answers as sent asks it: recursion desired, one question, no EDNS.
pub(in crate::dns) fn packet(id: u16, name: &Name, qtype: u16) -> Vec<u8> {
    let mut packet = Vec::from(id.to_be_bytes());
    // Recursion desired.
    packet.extend_from_slice(&[1, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
    packet.extend_from_slice(name.as_wire());
    packet.extend_from_slice(&qtype.to_be_bytes());
    packet.extend_from_slice(&[0, 1]);
    packet
}

// `bytes` read as the reply to `query`, which went out under `id`, as a
// lookup that takes answers as sent reads it. A reply is taken only under
// the id its query went out with, only when it answers the question asked,
// and only when its answer follows at most MOST aliases.
pub(in crate::dns) fn read(query: &Query, id: u16, bytes: &[u8]) -> Result<Reply, Exchanged> {
    if bytes.get(..2) != Some(&id.to_be_bytes()[..]) {
        return Err(Exchanged::Invalid);
    }
    // A refusal is read from the header alone, so one without a question
    // to answer still says what it refused.
    let reply = Reply::read(query, bytes)
        .map_err(|_| refusal(bytes, id).map_or(Exchanged::Invalid, Exchanged::Rcode))?;
    if reply.answer().alias_chain().len() > MOST + 1 {
        return Err(Exchanged::Invalid);
    }
    Ok(reply)
}

// The wire name of `host`, a name `host_name` passes: its dot-separated
// labels, each taken as it is.
fn wire_name(host: &str) -> Option<Name> {
    if !host_name(host) {
        return None;
    }
    let mut wire = Vec::with_capacity(host.len() + 2);
    for label in host.split('.') {
        wire.push(u8::try_from(label.len()).ok()?);
        wire.extend_from_slice(label.as_bytes());
    }
    wire.push(0);
    Name::from_wire(&wire).ok()
}

// Both families' walks at one server. Each walk takes its reply as it comes
// and sends its next query at once, so a family whose query stays silent
// holds up only itself, and a walk that ended before the server's time limit
// ran out keeps its answer. Each walk's queries are its own lane, under its
// own key: the walk's place, A then AAAA.
pub(super) struct AsSent([Walk; 2]);

impl AsSent {
    // Walks from `name`, lower case. A name no query may ask for is held as
    // the root, so each walk ends invalid before it asks anything.
    pub(super) fn new(name: &str) -> Self {
        let wire = wire_name(name);
        Self(FAMILIES.map(|qtype| Walk {
            qtype,
            name: wire.clone().unwrap_or_else(Name::root),
            ttl: u32::MAX,
            sent: 0,
            id: 0,
            out: false,
            done: None,
        }))
    }

    // Each walk's outcome, None for one still under way.
    pub(super) fn found(self) -> [Option<Result<Found, Exchanged>>; 2] {
        self.0.map(|walk| walk.done)
    }
}

impl Judge for AsSent {
    // The next query of each walk with none out, in order: at the start
    // both walks' first, then the next of the walk whose reply came. A walk
    // that is done asks nothing.
    async fn want(&mut self, asking: &mut Asking<'_, '_, '_>) {
        for (at, walk) in self.0.iter_mut().enumerate() {
            if walk.out {
                continue;
            }
            if let Some(packet) = walk.packet(|| asking.id()) {
                asking.send(at, at, packet);
                walk.out = true;
            }
        }
    }

    fn supply(&mut self, key: usize, reply: exchange::Reply) {
        let walk = &mut self.0[key];
        walk.out = false;
        walk.take(reply);
    }

    fn done(&self) -> bool {
        self.0.iter().all(|walk| walk.done.is_some())
    }

    fn fail(&mut self, failed: Exchanged) {
        for walk in &mut self.0 {
            if walk.done.is_none() {
                walk.done = Some(Err(failed.copy()));
            }
        }
    }
}
