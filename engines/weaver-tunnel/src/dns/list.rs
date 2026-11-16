// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The listed servers, asked in order for one name: who answers when Weaver
// asks a route's DNS servers itself. The list's order, the rule that keeps a
// name off plain DNS once it was meant to go out encrypted, each server's
// time limit, the merging of the two address families and the hold on a
// negative answer all live here.
use super::{
    Carrier, DnsServer, DnsSessions, FAMILIES, Found, Stage, Trust, Wire,
    exchange::Session,
    failure::{Exchanged, Failed, Unanswered, Worst},
    judge::{asked, validating},
    resolver::{Answer, Resolver},
};
use crate::{
    metrics,
    pipe::{DialError, Dialer},
    resolution::{
        Account, Lookup, ResolutionCache, Ttl,
        record::{self, Asker, TrustOutcome},
    },
};
use std::net::IpAddr;
use std::sync::Mutex;

// The servers a naming lists, asked in order through `stage`: the leg's
// egress, or the hop a dial goes through. Answers are held in `answers` for
// as long as their TTL allows, and the TLS sessions and DNSSEC validator are
// the naming's own, in `sessions`. Built only by a naming, for one lookup.
pub(crate) struct ListResolver<'a> {
    servers: &'a [DnsServer],
    trust: Trust,
    stage: &'a dyn Dialer,
    answers: &'a ResolutionCache<DialError>,
    sessions: &'a DnsSessions,
    // The servers are asked through a proxy, which the lookup names.
    proxied: bool,
}

impl<'a> ListResolver<'a> {
    // The servers asked straight out of the leg's egress.
    pub(crate) fn at_egress(
        servers: &'a [DnsServer],
        trust: Trust,
        egress: &'a dyn Dialer,
        answers: &'a ResolutionCache<DialError>,
        sessions: &'a DnsSessions,
    ) -> Self {
        Self {
            servers,
            trust,
            stage: egress,
            answers,
            sessions,
            proxied: false,
        }
    }

    // The servers asked through the hop a dial goes through.
    pub(crate) fn through_hop(
        servers: &'a [DnsServer],
        trust: Trust,
        hop: &'a dyn Dialer,
        answers: &'a ResolutionCache<DialError>,
        sessions: &'a DnsSessions,
    ) -> Self {
        Self {
            servers,
            trust,
            stage: hop,
            answers,
            sessions,
            proxied: true,
        }
    }
}

impl Resolver for ListResolver<'_> {
    fn asker(&self, proxy: u32) -> Asker {
        if self.proxied {
            Asker::proxy_servers(proxy, self.servers, self.trust)
        } else {
            Asker::servers(self.servers, self.trust)
        }
    }

    // A held answer when there is one; else the servers are asked, and
    // their answer held. Servers that all say the name has no address fail
    // as one that answered nothing would.
    async fn lookup(&self, host: &str) -> Answer {
        let addresses = self
            .answers
            .resolve_accounted(metrics::Resolver::Routed, host, || {
                resolve_routed(self.stage, self.sessions, self.servers, self.trust, host)
            })
            .await?;
        if addresses.is_empty() {
            return Err(super::unresolved());
        }
        Ok(Some(addresses))
    }

    fn forget(&self, host: &str) {
        self.answers.forget(host);
    }
}

// Resolves `host` by asking `servers` in order through `dialer`. When no
// server could be reached at all, the error is the stage's own, so a dead
// path counts against the path and not against the name. Every server
// saying the name has no address is a negative answer: no addresses, with
// how long that holds. `trust` is how the answers are to be judged, and
// `sessions` the TLS sessions the servers are resumed with.
pub async fn resolve_through(
    dialer: &dyn Dialer,
    sessions: &DnsSessions,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
) -> Result<Lookup, DialError> {
    resolve_routed(dialer, sessions, servers, trust, host)
        .await
        .0
}

// Resolves `host` by asking `servers` in order through `dialer`, with the
// lookup's account. When no server could be reached at all, the error is
// the stage's own, so a dead path counts against the path and not against
// the name.
pub(super) async fn resolve_routed(
    dialer: &dyn Dialer,
    sessions: &DnsSessions,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
) -> (Result<Lookup, DialError>, Account) {
    if let Ok(ip) = host.parse() {
        let lookup = Lookup {
            addresses: vec![ip],
            ttl: Ttl::Unknown,
            nonexistent: false,
        };
        return (Ok(lookup), Account::default());
    }
    sessions.ready().await;
    let stage = Stage {
        dialer,
        failure: Mutex::new(None),
    };
    let (resolved, account) = resolve_judged(&stage, sessions, servers, trust, host).await;
    metrics::record_resolution(
        metrics::Resolver::Routed,
        resolved
            .as_ref()
            .is_ok_and(|lookup| !lookup.addresses.is_empty()),
    );
    let resolved = resolved.map_err(|unanswered| {
        let failure = stage.failure.into_inner().expect("dns stage failure");
        match failure {
            Some(failure) if !unanswered.reached => failure,
            _ => DialError::Destination(std::io::Error::other(unanswered.error())),
        }
    });
    (resolved, account)
}

// Asks `servers` in order for `host`, and says how the lookup was judged:
// `AsSent` for an answer it returns, positive or negative. Under
// `Validate`, `Validated` or `Unsigned` for an answer it returns, and
// `Bogus` for a lookup that failed for want of one that passed the check.
// Nothing otherwise. Each server is asked with the naming's own `sessions`:
// its TLS sessions and DNSSEC validator, and on the connection it keeps for
// the server, when it keeps one dialled through `provider`'s stage, leaving
// its own there.
pub(super) async fn resolve_judged(
    provider: &dyn Carrier,
    sessions: &DnsSessions,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
) -> (Result<Lookup, Unanswered>, Account) {
    // An encrypted wire keeps one connection to the server for both
    // families, every alias followed and every DNSSEC query, and leaves it
    // to the naming for its next lookup.
    let session = |server| Session::keeping(provider, Keeping(sessions), server);
    walk(session, sessions.dnssec(), servers, trust, host).await
}

// A lookup as `resolve_judged` makes it, with `wires` and `dnssec`, that
// keeps no connection and takes none, for a test of what is asked.
#[cfg(test)]
pub(super) async fn resolve_unkept(
    provider: &dyn Carrier,
    wires: &super::exchange::Wires,
    dnssec: &validating::Dnssec,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
) -> (Result<Lookup, Unanswered>, Account) {
    let session = |server| Session::new(provider, wires, server);
    walk(session, dnssec, servers, trust, host).await
}

// Asks `servers` in order for `host`, each in the session `session` opens
// for it.
async fn walk<'a>(
    session: impl Fn(&'a DnsServer) -> Session<'a>,
    dnssec: &validating::Dnssec,
    servers: &'a [DnsServer],
    trust: Trust,
    host: &str,
) -> (Result<Lookup, Unanswered>, Account) {
    if servers.is_empty() {
        let unanswered = Unanswered::no_servers();
        unanswered.worst.note(host);
        let account = unanswered.account();
        return (Err(unanswered), account);
    }
    let mut walk = Failover::new();
    for server in servers {
        if walk.never_in_plain(server) {
            continue;
        }
        let mut session = session(server);
        // Set as soon as the server answers a dial, so a server cut off by
        // its time limit still says it was reached.
        let mut reached = false;
        let answered = asked(&mut session, dnssec, host, trust, &mut reached).await;
        walk.reached |= reached;
        let mut said = ServerSaid::default();
        for (qtype, answered) in FAMILIES.into_iter().zip(answered) {
            said.take(server, qtype, answered, &mut walk.why);
        }
        if let Some(addresses) = said.addresses() {
            return said.answer(host, server, trust, addresses);
        }
        walk.hold(&said);
    }
    walk.end(host, servers, trust)
}

// A naming's own lookup: the one asker that takes a connection the naming
// keeps and leaves its own to it. Only this module makes one, so a server
// test, which asks each server once, never keeps or reuses a connection.
pub(super) struct Keeping<'a>(&'a DnsSessions);

impl<'a> Keeping<'a> {
    pub(super) fn sessions(&self) -> &'a DnsSessions {
        self.0
    }
}

// What the servers asked so far for one lookup have come to, as the list
// is walked in order.
struct Failover {
    // The worst failure so far, which a lookup no server answered fails
    // with.
    why: Worst,
    // Some server answered a dial.
    reached: bool,
    // How long the name's having no address holds, while every server
    // asked has said so for both families.
    negative: Option<u32>,
    // Under `Validate`, whether a negative answer rests on an unsigned reply.
    negative_unsigned: bool,
    // A server on an encrypted wire has been asked.
    encrypted_before: bool,
}

impl Failover {
    fn new() -> Self {
        Self {
            why: Worst::default(),
            reached: false,
            negative: Some(u32::MAX),
            negative_unsigned: false,
            encrypted_before: false,
        }
    }

    // No downgrade (SECURITY.md): within one lookup, a plain server listed
    // after an encrypted one is never asked, whatever came of the encrypted
    // ones, so a name meant to go out encrypted never goes out in the clear.
    // A plain server listed before them is asked in its turn. Says whether
    // `server` is passed over, noting why; else notes whether it is
    // encrypted, for the servers after it.
    fn never_in_plain(&mut self, server: &DnsServer) -> bool {
        if self.encrypted_before && server.wire == Wire::Plain {
            self.why.consider(Failed::NotInPlain, server);
            return true;
        }
        self.encrypted_before |= server.wire != Wire::Plain;
        false
    }

    // A server gave no address: its negative answer holds only while every
    // server asked has said so for both families, for the shortest time any
    // of them gave.
    fn hold(&mut self, said: &ServerSaid) {
        let held = if said.without_soa { Some(0) } else { said.ttl };
        self.negative = self
            .negative
            .zip(held)
            .filter(|_| said.said_none == 2)
            .map(|(negative, held)| negative.min(held));
        self.negative_unsigned |= said.unsigned;
    }

    // The lookup after every server listed was asked or passed over, and
    // none gave an address: a negative answer every server gave, or the
    // worst failure.
    fn end(
        self,
        host: &str,
        servers: &[DnsServer],
        trust: Trust,
    ) -> (Result<Lookup, Unanswered>, Account) {
        let nonexistent = self.why.nonexistent();
        self.why.note(host);
        match self.negative {
            // Every server said so; the first is named as the one that answered.
            Some(ttl) => {
                let account = settled(host, &servers[0], trust, self.negative_unsigned);
                let lookup = Lookup {
                    addresses: Vec::new(),
                    ttl: Ttl::Seconds(ttl),
                    nonexistent,
                };
                (Ok(lookup), account)
            }
            None => {
                // A lookup with no answer that passed the DNSSEC check fails. It
                // is never a negative answer, so nothing holds it.
                let unanswered = Unanswered {
                    worst: self.why,
                    reached: self.reached,
                };
                if let Some(judged) = unanswered.judged() {
                    record::answered(host, None, judged);
                    metrics::record_dns_trust(judged);
                }
                let account = unanswered.account();
                (Err(unanswered), account)
            }
        }
    }
}

// What one server said of both families, merged.
#[derive(Default)]
struct ServerSaid {
    addresses: Vec<IpAddr>,
    // The shortest TTL either family's answer gave.
    ttl: Option<u32>,
    // How many families said no address.
    said_none: usize,
    // A family said no address and gave no SOA, so a negative answer
    // from this server holds for no time.
    without_soa: bool,
    // Whether a reply used for this server's answer was unsigned.
    unsigned: bool,
    // The families whose answer from this server failed the DNSSEC
    // check, or came without its records.
    bogus: Vec<u16>,
}

impl ServerSaid {
    // Takes what the server said of family `qtype`: an answer, a failure,
    // or None for a family the server's time limit cut off. Each failure is
    // weighed into `why`.
    fn take(
        &mut self,
        server: &DnsServer,
        qtype: u16,
        answered: Option<Result<Found, Exchanged>>,
        why: &mut Worst,
    ) {
        match answered {
            // A family the server's time limit cut off.
            None => why.consider(Failed::TimedOut, server),
            Some(Ok(found)) => {
                if found.addresses.is_empty() {
                    why.consider(Failed::negative(found.nonexistent), server);
                }
                if let Some(reason) = found.insecure {
                    tracing::debug!(
                        %server,
                        qtype,
                        reason = validating::insecure_words(reason),
                        "routed DNS answer not signed"
                    );
                    self.unsigned = true;
                }
                match found.ttl {
                    Some(held) => self.ttl = Some(self.ttl.map_or(held, |ttl| ttl.min(held))),
                    None => self.without_soa = true,
                }
                self.said_none += usize::from(found.addresses.is_empty());
                self.addresses.extend(found.addresses);
            }
            Some(Err(failed)) => {
                if matches!(failed, Exchanged::Bogus(..) | Exchanged::Stripped(_)) {
                    self.bogus.push(qtype);
                }
                why.consider(failed.into(), server);
            }
        }
    }

    // Every address either family gave, in order and once each, when there
    // is one.
    fn addresses(&mut self) -> Option<Vec<IpAddr>> {
        if self.addresses.is_empty() {
            return None;
        }
        let mut addresses = std::mem::take(&mut self.addresses);
        addresses.sort();
        addresses.dedup();
        Some(addresses)
    }

    // The lookup's answer from this server's `addresses`.
    fn answer(
        self,
        host: &str,
        server: &DnsServer,
        trust: Trust,
        addresses: Vec<IpAddr>,
    ) -> (Result<Lookup, Unanswered>, Account) {
        // The other family answered, so the lookup does. The family
        // that failed the check is counted bogus beside the lookup's
        // own outcome, which is counted once below.
        for qtype in self.bogus {
            tracing::debug!(
                %server,
                qtype,
                "routed DNS family failed DNSSEC validation; the other family answered"
            );
            metrics::record_dns_trust(TrustOutcome::Bogus);
        }
        let account = settled(host, server, trust, self.unsigned);
        let lookup = Lookup {
            addresses,
            ttl: self.ttl.map_or(Ttl::Unknown, Ttl::Seconds),
            nonexistent: false,
        };
        (Ok(lookup), account)
    }
}

// How an answer the lookup used is judged.
fn judged(trust: Trust) -> TrustOutcome {
    match trust {
        Trust::AsSent => TrustOutcome::AsSent,
        Trust::Validate => TrustOutcome::Validated,
    }
}

// A lookup that came to an answer, positive or negative: the server it came
// from, and how it was judged. Under `Validate`, an answer that rests on any
// unsigned reply is unsigned.
fn settled(host: &str, server: &DnsServer, trust: Trust, unsigned: bool) -> Account {
    let judged = match judged(trust) {
        TrustOutcome::Validated if unsigned => TrustOutcome::Unsigned,
        judged => judged,
    };
    let from = record::Answerer::of(server);
    record::answered(host, Some(from), judged);
    metrics::record_dns_wire(from.wire);
    metrics::record_dns_trust(judged);
    Account {
        judged: Some(judged),
        failure: None,
        from: Some(from),
    }
}
