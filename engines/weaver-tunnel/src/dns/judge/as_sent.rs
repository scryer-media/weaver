// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Answers taken as the server sent them: each family's walk along the
// aliases its answers name.
use super::{Asking, Judge, host_name};
use crate::dns::{
    Exchanged, FAMILIES, Found, exchange,
    message::{parse_response, refusal},
};

// One family's walk along the aliases its answers name, under `AsSent`.
struct Walk {
    qtype: u16,
    // The name asked next.
    name: String,
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
        if self.sent == 8 || !host_name(&self.name) {
            self.done = Some(Err(Exchanged::Invalid));
            return None;
        }
        self.sent += 1;
        self.id = id();
        let mut packet = Vec::from(self.id.to_be_bytes());
        // Recursion desired.
        packet.extend_from_slice(&[1, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
        for label in self.name.split('.') {
            packet.push(label.len() as u8);
            packet.extend_from_slice(label.as_bytes());
        }
        packet.push(0);
        packet.extend_from_slice(&self.qtype.to_be_bytes());
        packet.extend_from_slice(&[0, 1]);
        Some(packet)
    }

    // Takes the reply to the walk's last query: an answer, or an alias to
    // ask for next.
    fn take(&mut self, reply: exchange::Reply) {
        let bytes = match reply {
            Ok(bytes) => bytes,
            Err(failed) => {
                self.done = Some(Err(failed));
                return;
            }
        };
        let reply = match parse_response(&bytes, self.id, &self.name, self.qtype) {
            Ok(reply) => reply,
            Err(_) => {
                let failed = refusal(&bytes, self.id).map_or(Exchanged::Invalid, Exchanged::Rcode);
                self.done = Some(Err(failed));
                return;
            }
        };
        self.ttl = self.ttl.min(reply.ttl);
        if !reply.addresses.is_empty() {
            self.done = Some(Ok(Found {
                addresses: reply.addresses,
                ttl: Some(self.ttl),
                nonexistent: false,
                insecure: None,
            }));
            return;
        }
        match reply.alias {
            Some(alias) => self.name = alias,
            None => {
                self.done = Some(Ok(Found {
                    addresses: Vec::new(),
                    ttl: reply.negative.map(|negative| negative.min(self.ttl)),
                    nonexistent: reply.nonexistent,
                    insecure: None,
                }));
            }
        }
    }
}

// Both families' walks at one server. Each walk takes its reply as it comes
// and sends its next query at once, so a family whose query stays silent
// holds up only itself, and a walk that ended before the server's time limit
// ran out keeps its answer. Each walk's queries are its own lane, under its
// own key: the walk's place, A then AAAA.
pub(super) struct AsSent([Walk; 2]);

impl AsSent {
    pub(super) fn new(name: &str) -> Self {
        Self(FAMILIES.map(|qtype| Walk {
            qtype,
            name: name.to_owned(),
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
