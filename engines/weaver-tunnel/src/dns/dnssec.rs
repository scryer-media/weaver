// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// DNSSEC checked here (RFC 4033 to 4035) on what the listed servers answer.
// The validator does no I/O. Each query it needs goes to the one server
// being asked, on that server's session, as an address query does, and
// nowhere else. Nothing falls back: a check that cannot finish fails the
// query.
use super::{DnsServer, Failed, Found, NEXT_ID, Wire, wire::Session};
use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use weaver_dnssec::{
    BogusReason, InsecureReason, Name, Outcome, Question, RecordType, Step, UnansweredCause,
    Validator, Verdict,
};

// One naming's validator: the root's keys, the keys and delegations it has
// proven, and the names each server has lately been seen to fail. Built on
// the naming's first lookup that validates and kept for the naming's life.
#[derive(Default)]
pub(super) struct Dnssec {
    validator: OnceLock<Validator>,
    #[cfg(test)]
    now: OnceLock<u64>,
}

impl Dnssec {
    fn validator(&self) -> &Validator {
        self.validator.get_or_init(Validator::with_root_anchors)
    }

    // Seconds since 1970 by the host clock, which signatures are checked
    // against within the validator's allowance for skew.
    fn now(&self) -> u64 {
        #[cfg(test)]
        if let Some(now) = self.now.get() {
            return *now;
        }
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs())
    }

    // Trusts only `anchors`, at the fixed time `now`, for a test's own
    // signed zones. Only once, before any lookup.
    #[cfg(test)]
    pub(super) fn anchored(&self, anchors: Vec<weaver_dnssec::TrustAnchor>, now: u64) {
        let validator = Validator::new(anchors, weaver_dnssec::Policy::default())
            .expect("the fixture's trust anchors");
        assert!(
            self.validator.set(validator).is_ok() && self.now.set(now).is_ok(),
            "validator set twice"
        );
    }
}

// Asks the session's server for `name`'s records of `qtype`, and for every
// key, delegation and denial that proves them, and checks the signatures.
// A reply whose id is not the query's is a failed query, never one taken.
pub(super) async fn validated(
    session: &mut Session<'_>,
    dnssec: &Dnssec,
    name: &str,
    qtype: u16,
    reached: &mut bool,
) -> Result<Found, Failed> {
    let question = Name::from_text(&format!("{name}."))
        .ok()
        .and_then(|name| Question::new(name, RecordType(qtype)).ok())
        .ok_or(Failed::Invalid)?;
    let source = source(session.server());
    session.counting();
    let mut validation = dnssec
        .validator()
        .begin_from(source, question, dnssec.now());
    // Why each query that got no reply failed, for an unanswered verdict to
    // say.
    let mut failures = BTreeMap::new();
    let verdict = loop {
        let query = match off_the_workers(|| validation.step()).await {
            Step::Done(verdict) => break verdict,
            Step::Need(query) => query,
        };
        let mut queries = vec![query];
        queries.extend(validation.also_needed());
        // DNS over HTTPS sends id 0, as the address queries do.
        let ids: Vec<u16> = queries
            .iter()
            .map(|_| match session.server().wire {
                Wire::Https { .. } => 0,
                Wire::Plain | Wire::Tls { .. } => NEXT_ID.fetch_add(1, Ordering::Relaxed),
            })
            .collect();
        let packets: Vec<Vec<u8>> = queries
            .iter()
            .zip(&ids)
            .map(|(query, id)| query.encode(*id))
            .collect();
        // A server that cannot be reached, or fails TLS, fails the check:
        // the other queries would only fail the same way.
        let replies = session.ask_all(&packets, reached).await?;
        for ((query, id), reply) in queries.iter().zip(ids).zip(replies) {
            let failed = match reply {
                Ok(bytes) if bytes.get(..2) == Some(&id.to_be_bytes()[..]) => {
                    // The query is this validation's own and outstanding.
                    let _ = validation.supply(query, &bytes);
                    continue;
                }
                Ok(_) => Failed::Invalid,
                Err(failed) => failed,
            };
            failures.insert(query.clone(), failed);
            let _ = validation.failed(query);
        }
    };
    match verdict {
        Verdict::Secure(answer) | Verdict::Insecure(answer) => Ok(Found {
            addresses: answer.addresses(),
            ttl: Some(answer.ttl),
            nonexistent: answer.outcome == Outcome::NoSuchName,
            insecure: answer.insecure,
        }),
        Verdict::Bogus(bogus) if bogus.reason == BogusReason::NoDnssecRecords => {
            Err(Failed::Stripped(bogus.extended_errors))
        }
        Verdict::Bogus(bogus) => Err(Failed::Bogus(bogus.reason, bogus.extended_errors)),
        Verdict::Unanswered(unanswered) => Err(match unanswered.cause {
            UnansweredCause::FetchFailed => {
                failures.remove(&unanswered.query).unwrap_or(Failed::Closed)
            }
            UnansweredCause::Truncated => Failed::Invalid,
            UnansweredCause::Rcode(rcode) => Failed::Rcode(rcode),
        }),
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
pub(super) fn steps_in_place() -> usize {
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
pub(super) fn bogus_words(reason: BogusReason) -> &'static str {
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
    }
}

// A failure the host's clock may explain.
pub(super) fn clock_words(reason: BogusReason) -> &'static str {
    match reason {
        BogusReason::SignatureExpired | BogusReason::SignatureNotYetValid => {
            "; the host clock may be wrong"
        }
        _ => "",
    }
}

// Why a name was not signed, in a few fixed words.
pub(super) fn insecure_words(reason: InsecureReason) -> &'static str {
    match reason {
        InsecureReason::NoTrustAnchor => "no trust anchor covers the name",
        InsecureReason::UnsignedDelegation => "the zone is not signed",
        InsecureReason::UnsupportedAlgorithm => {
            "the zone is signed only with algorithms Weaver does not check"
        }
        InsecureReason::PrivateName => "the name is in a private zone",
    }
}

// The Extended DNS Error codes (RFC 8914) the replies carried, as numbers.
pub(super) fn extended_words(codes: &[u16]) -> String {
    if codes.is_empty() {
        return String::new();
    }
    let codes: Vec<String> = codes.iter().map(u16::to_string).collect();
    format!("; Extended DNS Error codes {}", codes.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            }),
            at(Wire::Https {
                path: "/other".into(),
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
