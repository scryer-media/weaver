// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// DNSSEC checked here (RFC 4033 to 4035) on what the listed servers answer.
// The validator does no I/O. Each query it needs goes to the one server
// being asked, on that server's session, as an address query does, and
// nowhere else. Nothing falls back: a check that cannot finish fails the
// query.
use crate::dns::{
    DnsServer, Exchanged, Found,
    exchange::{Reply, Session},
    judge::{self, Asking, Judge},
    roots::{self, TrustRoots},
};
use std::collections::BTreeMap;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use weaver_dnssec::{
    Answer, BogusReason, InsecureReason, Name, Outcome, Query, Question, RecordType, Source, Step,
    UnansweredCause, Validation, Validator, Verdict,
};

// One naming's validator: the root's keys, the keys and delegations it has
// proven, and the names each server has lately been seen to fail. Built
// from the naming's trust roots on its first lookup that validates, and
// kept for the naming's life.
pub(in crate::dns) struct Dnssec {
    // None for the built-in roots, built at their first use.
    roots: Option<Arc<TrustRoots>>,
    validator: OnceLock<Validator>,
}

impl Dnssec {
    pub(in crate::dns) fn new(roots: Option<Arc<TrustRoots>>) -> Self {
        Self {
            roots,
            validator: OnceLock::new(),
        }
    }

    fn validator(&self) -> &Validator {
        self.validator
            .get_or_init(|| roots::or_built_in(&self.roots).validator())
    }

    // The time signatures are checked against, by the roots' clock.
    fn now(&self) -> u64 {
        roots::or_built_in(&self.roots).now()
    }
}

// Asks the session's server for `name`'s records of `qtype`, and for every
// key, delegation and denial that proves them, and checks the signatures.
// The caller says whether the session counts the queries it sends.
pub(in crate::dns) async fn validated(
    session: &mut Session<'_>,
    dnssec: &Dnssec,
    name: &str,
    qtype: u16,
    reached: &mut bool,
) -> Result<Found, Exchanged> {
    let mut found = [None];
    validated_all(session, dnssec, name, &[qtype], &mut found, reached).await;
    let [found] = found;
    found.unwrap_or(Err(Exchanged::Invalid))
}

// Checks `name`'s records of each of `qtypes` at the session's server, all
// at once, and puts each verdict in its slot of `found` as it is reached.
// A check steps as soon as the reply it waits on comes, and the queries it
// then needs go out at once, while the other check's are still out: one
// silent query holds up only the check waiting on it. A query out for one
// check that the other needs too is not sent again, and its reply is given
// to both. The query a check cannot go on without goes before those it
// asks ahead, where queries take turns. A family's chain shares the keys
// and delegations the other proved, through the validator's cache. A reply
// is taken only under the id its query went out with. A server that
// cannot be reached, or fails TLS, fails every check still running: the
// other queries would only fail the same way. Once every check has its
// verdict the talk ends, and a query no running check still waits on is not
// sent.
pub(super) async fn validated_all(
    session: &mut Session<'_>,
    dnssec: &Dnssec,
    name: &str,
    qtypes: &[u16],
    found: &mut [Option<Result<Found, Exchanged>>],
    reached: &mut bool,
) {
    let mut checks = Validating::new(session.server(), dnssec, name, qtypes, found);
    judge::run(session, reached, &mut checks).await;
}

// The DNSSEC checks of one name at one server, one per type asked, each
// verdict put in its slot of `found` as it is reached. A query's key is its
// place in `sent`; its lane is the check that first gave it.
struct Validating<'f, 'v> {
    checks: Vec<Option<Validation<'v>>>,
    found: &'f mut [Option<Result<Found, Exchanged>>],
    // Why each query that got no reply failed, for each check, for an
    // unanswered verdict to say.
    failures: Vec<BTreeMap<Query, Exchanged>>,
    // Each query sent, by its key: the query, its id, and the checks that
    // wait on its reply, until the reply comes.
    sent: Vec<(Query, u16, Option<Vec<usize>>)>,
    // The key of each query still out, by the query.
    out: BTreeMap<Query, usize>,
    // The key of the query each check's last step waits on.
    blocked: Vec<Option<usize>>,
    // The checks to step: every one at the start, then each whose awaited
    // reply came.
    due: Vec<usize>,
}

impl<'f, 'v> Validating<'f, 'v> {
    fn new(
        server: &DnsServer,
        dnssec: &'v Dnssec,
        name: &str,
        qtypes: &[u16],
        found: &'f mut [Option<Result<Found, Exchanged>>],
    ) -> Self {
        let source = Source::new(source(server));
        let now = dnssec.now();
        let checks: Vec<_> = qtypes
            .iter()
            .zip(found.iter_mut())
            .map(|(qtype, found)| {
                let question = Name::from_text(&format!("{name}."))
                    .ok()
                    .and_then(|name| Question::new(name, RecordType::new(*qtype)).ok());
                match question {
                    Some(question) => Some(dnssec.validator().begin_from(source, question, now)),
                    None => {
                        *found = Some(Err(Exchanged::Invalid));
                        None
                    }
                }
            })
            .collect();
        let due = (0..checks.len()).rev().collect();
        Self {
            checks,
            found,
            failures: qtypes.iter().map(|_| BTreeMap::new()).collect(),
            sent: Vec::new(),
            out: BTreeMap::new(),
            blocked: qtypes.iter().map(|_| None).collect(),
            due,
        }
    }
}

impl Judge for Validating<'_, '_> {
    // Steps each check due, in order, and gives the queries its step needs:
    // the one it waits on, then those it asks ahead.
    async fn want(&mut self, asking: &mut Asking<'_, '_, '_>) {
        let Self {
            checks,
            found,
            failures,
            sent,
            out,
            blocked,
            due,
        } = self;
        while let Some(at) = due.pop() {
            let Some(validation) = &mut checks[at] else {
                continue;
            };
            let (query, hints) = match off_the_workers(|| validation.step()).await {
                Step::Need { awaited, ahead, .. } => (awaited, ahead),
                // The check is over. A step this build does not know ends it
                // too, as a failure.
                step => {
                    found[at] = Some(match step {
                        Step::Done { verdict, .. } => judged(verdict, &mut failures[at]),
                        _ => Err(Exchanged::Invalid),
                    });
                    checks[at] = None;
                    // A query only this check waited on, and not yet sent,
                    // is not sent at all.
                    out.retain(|_, key| {
                        let Some(wanting) = &mut sent[*key].2 else {
                            return false;
                        };
                        wanting.retain(|wants| *wants != at);
                        !(wanting.is_empty() && asking.withdraw(*key))
                    });
                    continue;
                }
            };
            for (hint, query) in std::iter::once(query).chain(hints).enumerate() {
                let key = match out.get(&query) {
                    Some(&key) => {
                        asking.wanted_by(key, at, hint == 0);
                        key
                    }
                    None => {
                        // The id the server's wire sends, as the address
                        // queries do.
                        let id = asking.id();
                        if hint == 0 {
                            asking.send(sent.len(), at, query.encode(id));
                        } else {
                            asking.ahead(sent.len(), at, query.encode(id));
                        }
                        out.insert(query.clone(), sent.len());
                        sent.push((query, id, Some(Vec::new())));
                        sent.len() - 1
                    }
                };
                let wanting = sent[key].2.get_or_insert_with(Vec::new);
                if !wanting.contains(&at) {
                    wanting.push(at);
                }
                if hint == 0 {
                    blocked[at] = Some(key);
                }
            }
        }
    }

    // A reply is taken only under the id its query went out with. Each check
    // that waited on it is stepped next.
    fn supply(&mut self, key: usize, reply: Reply) {
        let (query, id, wanting) = &mut self.sent[key];
        let wanting = wanting.take().unwrap_or_default();
        self.out.remove(query);
        let failed = match reply {
            Ok(bytes) if bytes.get(..2) == Some(&id.to_be_bytes()[..]) => {
                for at in &wanting {
                    if let Some(validation) = &mut self.checks[*at] {
                        // The query is this check's own and outstanding.
                        let _ = validation.supply(query, &bytes);
                    }
                }
                None
            }
            Ok(_) => Some(Exchanged::Invalid),
            Err(failed) => Some(failed),
        };
        for at in wanting {
            if let Some(validation) = &mut self.checks[at] {
                if let Some(failed) = &failed {
                    self.failures[at].insert(query.clone(), failed.copy());
                    let _ = validation.supply_failure(query);
                }
                if self.blocked[at] == Some(key) {
                    self.due.push(at);
                }
            }
        }
    }

    fn done(&self) -> bool {
        self.checks.iter().all(Option::is_none)
    }

    fn fail(&mut self, failed: Exchanged) {
        for (at, check) in self.checks.iter_mut().enumerate() {
            if check.take().is_some() {
                self.found[at] = Some(Err(failed.copy()));
            }
        }
    }
}

// What a check's verdict comes to for the lookup.
fn judged(verdict: Verdict, failures: &mut BTreeMap<Query, Exchanged>) -> Result<Found, Exchanged> {
    match verdict {
        Verdict::Secure { answer, .. } => Ok(found(&answer, None)),
        Verdict::Insecure { answer, reason, .. } => Ok(found(&answer, Some(reason))),
        Verdict::Bogus { failure, .. } if failure.reason() == BogusReason::NoDnssecRecords => {
            Err(Exchanged::Stripped(failure.extended_errors().to_vec()))
        }
        Verdict::Bogus { failure, .. } => Err(Exchanged::Bogus(
            failure.reason(),
            failure.extended_errors().to_vec(),
        )),
        Verdict::Unanswered { failure, .. } => Err(match failure.cause() {
            UnansweredCause::FetchFailed => failures
                .remove(failure.query())
                .unwrap_or(Exchanged::Closed),
            UnansweredCause::Truncated => Exchanged::Invalid,
            UnansweredCause::Rcode(rcode) => Exchanged::Rcode(rcode),
            _ => Exchanged::Invalid,
        }),
        // A verdict this build does not know is never taken as an answer.
        _ => Err(Exchanged::Invalid),
    }
}

// The lookup's answer from a verdict's records, and why the name was not
// signed when it was not.
fn found(answer: &Answer, insecure: Option<InsecureReason>) -> Found {
    Found {
        addresses: answer.addresses(),
        ttl: Some(answer.ttl()),
        nonexistent: *answer.outcome() == Outcome::NoSuchName,
        insecure,
    }
}

// The server a validation's answers come from, by its address and wire. A
// name proven bogus is held against that server alone, so the next listed
// server is still asked. The same server keeps the same value for the
// process's life, whatever order a list gives it.
fn source(server: &DnsServer) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    server.hash(&mut hasher);
    hasher.finish()
}

// Checking signatures is the CPU work of a step: microseconds a signature,
// and up to 200 signatures a check. On a worker of a runtime with several, the step runs
// in place of the worker's other tasks, which move to another worker, and
// no more steps run so at once than the runtime has workers. Anywhere else
// it runs inline: on a runtime with one thread, and on a thread that is not
// a worker, such as one blocking on the runtime, where a LocalSet forbids
// blocking in place. A lookup waiting for its turn waits within its own
// time limit, and leaves the queue when it is dropped. The turn is held
// only while the step runs, never across a query.
async fn off_the_workers<T>(work: impl FnOnce() -> T) -> T {
    if !on_a_worker() {
        return work();
    }
    let steps = steps();
    let _turn = match steps.try_acquire() {
        Ok(turn) => Ok(turn),
        Err(_) => steps.acquire().await,
    };
    // The task may have moved while it waited, but only to another worker.
    if on_a_worker() {
        #[cfg(test)]
        IN_PLACE.fetch_add(1, Ordering::Relaxed);
        tokio::task::block_in_place(work)
    } else {
        work()
    }
}

#[cfg(test)]
static IN_PLACE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

// Steps run in place so far, in this process.
#[cfg(test)]
pub(in crate::dns) fn steps_in_place() -> usize {
    IN_PLACE.load(Ordering::Relaxed)
}

// Whether this thread is a worker of a runtime with several: the one place
// tokio lets a task block in place.
fn on_a_worker() -> bool {
    use tokio::runtime::{Handle, RuntimeFlavor};
    Handle::try_current().is_ok_and(|handle| handle.runtime_flavor() == RuntimeFlavor::MultiThread)
        && tokio::runtime::worker_index().is_some()
}

// The turns steps take in place: one for each of the runtime's workers,
// the sizing the torrent engine's blocking permits use. Shared by every
// naming, for the process's life.
fn steps() -> &'static tokio::sync::Semaphore {
    static STEPS: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    STEPS.get_or_init(|| {
        let workers = tokio::runtime::Handle::try_current()
            .map_or(1, |handle| handle.metrics().num_workers());
        tokio::sync::Semaphore::new(workers.max(1))
    })
}

// Why the signatures failed, in a few fixed words.
pub(in crate::dns) fn bogus_words(reason: BogusReason) -> &'static str {
    match reason {
        BogusReason::MalformedReply => "a reply could not be read",
        BogusReason::SignaturesMissing => "the records carry no usable signature",
        BogusReason::SignatureInvalid => "the signatures do not match the records",
        BogusReason::SignatureExpired => "the signatures have expired",
        BogusReason::SignatureNotYetValid => "the signatures are not valid yet",
        BogusReason::NoMatchingKey => "no proven key made the signatures",
        BogusReason::KeysNotProven => "the zone's keys could not be proven",
        BogusReason::TrustAnchorMismatch => "the root keys do not match the trust anchor",
        BogusReason::NoDnssecRecords => "the server returned no DNSSEC records",
        BogusReason::SignerNotAZone => "a signer is not a proven zone",
        BogusReason::SignerMismatch => "a signer does not fit the records",
        BogusReason::DelegationUnproven => "a delegation could not be proven",
        BogusReason::DenialMissing => "a negative answer has no proof",
        BogusReason::DenialInvalid => "a negative answer's proof does not hold",
        BogusReason::WildcardUnproven => "a wildcard answer has no proof",
        BogusReason::AliasInvalid => "an alias chain is broken",
        BogusReason::WorkLimit => "the check needed more work than Weaver allows",
        BogusReason::Nsec3Iterations => {
            "the zone's NSEC3 records use more iterations than Weaver will compute"
        }
        _ => "the proof failed for a reason this build does not name",
    }
}

// A failure the host's clock may explain.
pub(in crate::dns) fn clock_words(reason: BogusReason) -> &'static str {
    match reason {
        BogusReason::SignatureExpired | BogusReason::SignatureNotYetValid => {
            "; the host clock may be wrong"
        }
        _ => "",
    }
}

// Why a name was not signed, in a few fixed words.
pub(in crate::dns) fn insecure_words(reason: InsecureReason) -> &'static str {
    match reason {
        InsecureReason::NoTrustAnchor => "no trust anchor covers the name",
        InsecureReason::UnsignedDelegation => "the zone is not signed",
        InsecureReason::UnsupportedAlgorithm => {
            "the zone is signed only with algorithms Weaver does not check"
        }
        InsecureReason::PrivateName => "the name is in a private zone",
        _ => "the name is not signed for another reason",
    }
}

// The Extended DNS Error codes (RFC 8914) the replies carried, as numbers.
pub(in crate::dns) fn extended_words(codes: &[u16]) -> String {
    if codes.is_empty() {
        return String::new();
    }
    let codes: Vec<String> = codes.iter().map(u16::to_string).collect();
    format!("; Extended DNS Error codes {}", codes.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dns::Wire;

    // Servers of two namings at one address, on different wire settings,
    // are different sources, so a name held bogus against one is not held
    // against the other. Each keeps its value from call to call.
    #[test]
    fn a_server_at_one_address_on_other_wire_settings_is_another_source() {
        let address = "192.0.2.53:853".parse().unwrap();
        let at = |wire| DnsServer { address, wire };
        let servers = [
            at(Wire::Plain),
            at(Wire::Tls { name: None }),
            at(Wire::Tls {
                name: Some("dns.example".into()),
            }),
            at(Wire::Tls {
                name: Some("other.example".into()),
            }),
            at(Wire::Https {
                path: "/dns-query".into(),
                name: None,
            }),
            at(Wire::Https {
                path: "/other".into(),
                name: None,
            }),
        ];
        let sources: Vec<u64> = servers.iter().map(source).collect();
        for (at, one) in sources.iter().enumerate() {
            for other in &sources[at + 1..] {
                assert_ne!(one, other, "{servers:?}");
            }
        }
        for (server, held) in servers.iter().zip(&sources) {
            assert_eq!(source(server), *held, "{server:?}");
            assert_eq!(source(&server.clone()), *held, "{server:?}");
        }
    }
}
