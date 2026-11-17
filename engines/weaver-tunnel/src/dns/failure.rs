// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Why a lookup through listed servers found nothing, in one vocabulary from
// the wire to the account the rest of Weaver reads: what went wrong, at
// which server, which failure is told when every server fails, and the
// words and outcome each comes to.
use super::{
    DnsServer, INVALID, NOT_VALIDATED, SERVER_TIMEOUT, UNRESOLVED, judge::validating,
    probe::SOCKET_WAIT,
};
use crate::{
    TunnelError,
    resolution::{
        Account,
        record::{self, LookupError, LookupFailure, TrustOutcome},
    },
};

// What a lookup says when it is given no server to ask.
const NO_SERVERS: &str = "no DNS servers to ask";

// What went wrong with one query in a conversation with one server: all a
// wire, or the DNSSEC check on what it carried, can say.
#[derive(Debug)]
pub(super) enum Exchanged {
    // The server could not be reached.
    Unreached(TunnelError),
    // The connection closed before any byte of the reply came.
    Closed,
    // In plain DNS, the stream failed before any byte of the reply came,
    // for this fixed reason: reset by the server, or another I/O error.
    Io(&'static str),
    // The connection closed part way through the reply.
    Cut,
    // The server reset the HTTP/2 stream carrying the query.
    Reset,
    // The server refused the HTTP/2 stream unhandled (RFC 9113 §8.7): the
    // connection still runs, and the query may be sent again on it.
    Refused,
    // The server answered with this error code instead of an answer.
    Rcode(u16),
    Invalid,
    // The TLS handshake or certificate check failed, for this fixed reason.
    Tls(&'static str),
    // The HTTPS server answered with this status instead of 200.
    Http(u16),
    // Under `Validate`, the signatures failed the check for this reason, and
    // the replies carried these Extended DNS Error codes.
    Bogus(weaver_dnssec::BogusReason, Vec<u16>),
    // Under `Validate`, a signed zone's answer came with no DNSSEC records.
    Stripped(Vec<u16>),
}

impl Exchanged {
    // The same failure, for another query it ended too. A stage's error is
    // carried by its words, which are all a lookup says of it.
    pub(super) fn copy(&self) -> Self {
        match self {
            Self::Unreached(error) => Self::Unreached(match error {
                TunnelError::Engine(said) => TunnelError::Engine(said.clone()),
                error => TunnelError::Engine(error.to_string()),
            }),
            Self::Closed => Self::Closed,
            Self::Io(reason) => Self::Io(reason),
            Self::Cut => Self::Cut,
            Self::Reset => Self::Reset,
            Self::Refused => Self::Refused,
            Self::Rcode(rcode) => Self::Rcode(*rcode),
            Self::Invalid => Self::Invalid,
            Self::Tls(reason) => Self::Tls(reason),
            Self::Http(status) => Self::Http(*status),
            Self::Bogus(reason, codes) => Self::Bogus(*reason, codes.clone()),
            Self::Stripped(codes) => Self::Stripped(codes.clone()),
        }
    }
}

// What went wrong with one query, one server or one lookup: a wire's
// failure, or an outcome only the lookup can come to.
#[derive(Debug)]
pub(super) enum Failed {
    Wire(Exchanged),
    // The server's time limit ran out before the query came to anything.
    TimedOut,
    // The server said the name does not exist.
    NoSuchName,
    // The server said the name has no address of the family asked.
    NoAddress,
    // A plain server listed after an encrypted one, so never asked.
    NotInPlain,
    // A probe waited SOCKET_WAIT in all for a socket on the route.
    NoSocket,
    // The lookup was given no server to ask.
    NoServers,
}

impl Failed {
    // What an answer with no address says: the name does not exist, or it
    // has no address of the family asked.
    pub(super) fn negative(nonexistent: bool) -> Self {
        if nonexistent {
            Self::NoSuchName
        } else {
            Self::NoAddress
        }
    }

    // What the rest of Weaver is told the failure was.
    pub(super) fn class(&self) -> LookupFailure {
        match self {
            Self::NoSuchName => LookupFailure::NoSuchName,
            Self::NoAddress => LookupFailure::NoAddress,
            Self::Wire(
                Exchanged::Unreached(_)
                | Exchanged::Closed
                | Exchanged::Io(_)
                | Exchanged::Cut
                | Exchanged::Reset
                | Exchanged::Refused,
            ) => LookupFailure::NoReply,
            Self::Wire(Exchanged::Rcode(_) | Exchanged::Http(_)) => LookupFailure::ServerFailure,
            Self::Wire(Exchanged::Tls(_)) => LookupFailure::TlsFailed,
            Self::Wire(Exchanged::Bogus(..)) => LookupFailure::Bogus,
            Self::Wire(Exchanged::Stripped(_)) => LookupFailure::DnssecUnavailable,
            Self::TimedOut => LookupFailure::TimedOut,
            Self::Wire(Exchanged::Invalid)
            | Self::NotInPlain
            | Self::NoSocket
            | Self::NoServers => LookupFailure::Other,
        }
    }

    pub(super) fn rank(&self) -> u8 {
        rank(self.class())
    }

    // Whether the failure's words name the server it came of. A stage's
    // own words, and a lookup given no server, name none.
    fn names_server(&self) -> bool {
        !matches!(self, Self::Wire(Exchanged::Unreached(_)) | Self::NoServers)
    }

    // The failure at `server` in the operator's words.
    pub(super) fn detail_at(&self, server: Option<&DnsServer>) -> String {
        let at = At(server);
        match self {
            Self::NoSuchName => format!("{at}NXDOMAIN"),
            Self::NoAddress => format!("{at}NODATA"),
            Self::Wire(exchanged) => match exchanged {
                // The stage's own words, as it failed to dial the server.
                Exchanged::Unreached(error) => match error {
                    TunnelError::Engine(said) => said.clone(),
                    error => error.to_string(),
                },
                Exchanged::Closed => format!("{at}the connection closed without a reply"),
                Exchanged::Io(reason) => format!("{at}{reason}"),
                Exchanged::Cut => format!("{at}the connection closed part way through a reply"),
                Exchanged::Reset | Exchanged::Refused => {
                    format!("{at}the server reset the query")
                }
                Exchanged::Rcode(rcode) => match rcode {
                    2 => format!("{at}SERVFAIL"),
                    5 => format!("{at}REFUSED"),
                    rcode => format!("{at}RCODE {rcode}"),
                },
                Exchanged::Invalid => format!("{at}{INVALID}"),
                Exchanged::Tls(reason) => format!("{at}TLS failed: {reason}"),
                Exchanged::Http(status) => format!("{at}HTTP {status}"),
                Exchanged::Bogus(reason, codes) => format!(
                    "{at}DNSSEC validation failed: {}{}{}",
                    validating::bogus_words(*reason),
                    validating::clock_words(*reason),
                    validating::extended_words(codes),
                ),
                Exchanged::Stripped(codes) => format!(
                    "{at}the server returned no DNSSEC records{}",
                    validating::extended_words(codes),
                ),
            },
            Self::TimedOut => format!("{at}no answer within {}s", SERVER_TIMEOUT.as_secs()),
            Self::NotInPlain => format!("{at}not asked in plain after an encrypted server"),
            Self::NoSocket => format!(
                "{at}waited {} s in all for a DNS socket on the route",
                SOCKET_WAIT.as_secs()
            ),
            Self::NoServers => NO_SERVERS.to_owned(),
        }
    }

    // The failure at `server` as the rest of Weaver reads it.
    pub(super) fn error_at(&self, server: Option<&DnsServer>) -> LookupError {
        LookupError {
            failure: self.class(),
            detail: self.detail_at(server),
        }
    }
}

impl From<Exchanged> for Failed {
    fn from(exchanged: Exchanged) -> Self {
        Self::Wire(exchanged)
    }
}

// A failure, and the server it came of: None for a lookup given no server
// to ask, and for a failure whose words do not name its server.
#[derive(Debug)]
pub(super) struct Failure {
    pub(super) failed: Failed,
    pub(super) server: Option<DnsServer>,
}

// "<server>: " before a failure's words, or nothing without a server.
struct At<'s>(Option<&'s DnsServer>);

impl std::fmt::Display for At<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(server) => write!(f, "{server}: "),
            None => Ok(()),
        }
    }
}

// Which failure is told when every server fails: the higher, the more
// telling. Of two that rank the same, the first is told.
fn rank(failure: LookupFailure) -> u8 {
    match failure {
        LookupFailure::Bogus => 8,
        LookupFailure::DnssecUnavailable => 7,
        LookupFailure::NoSuchName => 6,
        LookupFailure::NoAddress => 5,
        LookupFailure::TlsFailed => 4,
        LookupFailure::ServerFailure => 3,
        LookupFailure::TimedOut => 2,
        LookupFailure::Other => 1,
        LookupFailure::NoReply => 0,
    }
}

impl Failure {
    // The server is copied only when the failure's words name it.
    pub(super) fn new(failed: Failed, server: &DnsServer) -> Self {
        let server = failed.names_server().then(|| server.clone());
        Self { failed, server }
    }

    // A lookup given no server to ask.
    pub(super) fn no_servers() -> Self {
        Self {
            failed: Failed::NoServers,
            server: None,
        }
    }

    pub(super) fn class(&self) -> LookupFailure {
        self.failed.class()
    }

    pub(super) fn rank(&self) -> u8 {
        self.failed.rank()
    }

    // The failure in the operator's words.
    pub(super) fn detail(&self) -> String {
        self.failed.detail_at(self.server.as_ref())
    }

    // The failure as the rest of Weaver reads it.
    pub(super) fn error(&self) -> LookupError {
        self.failed.error_at(self.server.as_ref())
    }
}

// The most telling failure of a lookup so far.
#[derive(Debug, Default)]
pub(super) struct Worst(Option<Failure>);

impl Worst {
    // Keeps `failed` at `server` when it outranks the failure held. Only
    // the failure kept takes a copy of its server.
    pub(super) fn consider(&mut self, failed: Failed, server: &DnsServer) {
        if self
            .0
            .as_ref()
            .is_none_or(|held| failed.rank() > held.rank())
        {
            self.0 = Some(Failure::new(failed, server));
        }
    }

    fn class(&self) -> Option<LookupFailure> {
        self.0.as_ref().map(Failure::class)
    }

    // A server said the name does not exist, and nothing told more.
    pub(super) fn nonexistent(&self) -> bool {
        self.class() == Some(LookupFailure::NoSuchName)
    }

    // No answer passed the DNSSEC check, for want of a good proof or of the
    // records to check.
    pub(super) fn not_validated(&self) -> bool {
        matches!(
            self.class(),
            Some(LookupFailure::Bogus | LookupFailure::DnssecUnavailable)
        )
    }

    pub(super) fn error(&self) -> Option<LookupError> {
        self.0.as_ref().map(Failure::error)
    }

    // Notes why `host` found nothing in the test recording this lookup runs
    // in, if any.
    pub(super) fn note(&self, host: &str) {
        if let Some(failure) = &self.0 {
            record::failed(host, failure.class(), || failure.detail());
        }
    }
}

// Why no server gave an answer. `reached` says whether any server could be
// dialled at all.
#[derive(Debug)]
pub(super) struct Unanswered {
    pub(super) worst: Worst,
    pub(super) reached: bool,
}

impl Unanswered {
    // A lookup given no server to ask.
    pub(super) fn no_servers() -> Self {
        Self {
            worst: Worst(Some(Failure::no_servers())),
            reached: false,
        }
    }

    // How the lookup was judged: bogus when it failed for want of an answer
    // that passed the DNSSEC check, nothing otherwise.
    pub(super) fn judged(&self) -> Option<TrustOutcome> {
        self.worst.not_validated().then_some(TrustOutcome::Bogus)
    }

    // The lookup's account: how it was judged, why it failed, from no
    // server.
    pub(super) fn account(&self) -> Account {
        Account {
            judged: self.judged(),
            failure: self.worst.error(),
            from: None,
        }
    }

    // What the lookup's error says.
    pub(super) fn error(&self) -> TunnelError {
        match &self.worst.0 {
            Some(Failure {
                failed: Failed::NoServers,
                ..
            }) => TunnelError::Configuration(NO_SERVERS.into()),
            _ if self.worst.not_validated() => TunnelError::Engine(NOT_VALIDATED.into()),
            _ => TunnelError::Engine(UNRESOLVED.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_dnssec::BogusReason;

    fn server() -> DnsServer {
        DnsServer::plain("192.0.2.53".parse().unwrap())
    }

    // Every kind of failure, one of each.
    fn every() -> Vec<Failed> {
        vec![
            Exchanged::Unreached(TunnelError::Engine(
                "egress connection failed: no route".into(),
            ))
            .into(),
            Exchanged::Closed.into(),
            Exchanged::Io("connection reset").into(),
            Exchanged::Cut.into(),
            Exchanged::Reset.into(),
            Exchanged::Refused.into(),
            Exchanged::Rcode(2).into(),
            Exchanged::Invalid.into(),
            Exchanged::Tls("unknown issuer").into(),
            Exchanged::Http(503).into(),
            Exchanged::Bogus(BogusReason::SignatureInvalid, Vec::new()).into(),
            Exchanged::Stripped(Vec::new()).into(),
            Failed::TimedOut,
            Failed::NoSuchName,
            Failed::NoAddress,
            Failed::NotInPlain,
            Failed::NoSocket,
            Failed::NoServers,
        ]
    }

    #[test]
    fn each_class_ranks_as_before() {
        let ranked = [
            (LookupFailure::Bogus, 8),
            (LookupFailure::DnssecUnavailable, 7),
            (LookupFailure::NoSuchName, 6),
            (LookupFailure::NoAddress, 5),
            (LookupFailure::TlsFailed, 4),
            (LookupFailure::ServerFailure, 3),
            (LookupFailure::TimedOut, 2),
            (LookupFailure::Other, 1),
            (LookupFailure::NoReply, 0),
        ];
        for (failure, held) in ranked {
            assert_eq!(rank(failure), held, "{failure:?}");
        }
    }

    #[test]
    fn each_failure_comes_to_its_class() {
        let server = server();
        let classes = [
            LookupFailure::NoReply,
            LookupFailure::NoReply,
            LookupFailure::NoReply,
            LookupFailure::NoReply,
            LookupFailure::NoReply,
            LookupFailure::NoReply,
            LookupFailure::ServerFailure,
            LookupFailure::Other,
            LookupFailure::TlsFailed,
            LookupFailure::ServerFailure,
            LookupFailure::Bogus,
            LookupFailure::DnssecUnavailable,
            LookupFailure::TimedOut,
            LookupFailure::NoSuchName,
            LookupFailure::NoAddress,
            LookupFailure::Other,
            LookupFailure::Other,
            LookupFailure::Other,
        ];
        let every = every();
        assert_eq!(every.len(), classes.len());
        for (failed, class) in every.into_iter().zip(classes) {
            let failure = Failure::new(failed, &server);
            assert_eq!(failure.class(), class, "{failure:?}");
            assert_eq!(failure.rank(), rank(class), "{failure:?}");
        }
    }

    #[test]
    fn each_failure_says_what_it_said_before() {
        let server = server();
        let words = [
            "egress connection failed: no route",
            "192.0.2.53: the connection closed without a reply",
            "192.0.2.53: connection reset",
            "192.0.2.53: the connection closed part way through a reply",
            "192.0.2.53: the server reset the query",
            "192.0.2.53: the server reset the query",
            "192.0.2.53: SERVFAIL",
            "192.0.2.53: routed DNS returned an invalid response",
            "192.0.2.53: TLS failed: unknown issuer",
            "192.0.2.53: HTTP 503",
            "192.0.2.53: DNSSEC validation failed: the signatures do not match the records",
            "192.0.2.53: the server returned no DNSSEC records",
            "192.0.2.53: no answer within 5s",
            "192.0.2.53: NXDOMAIN",
            "192.0.2.53: NODATA",
            "192.0.2.53: not asked in plain after an encrypted server",
            "192.0.2.53: waited 195 s in all for a DNS socket on the route",
            "no DNS servers to ask",
        ];
        let every = every();
        assert_eq!(every.len(), words.len());
        for (failed, said) in every.into_iter().zip(words) {
            let failure = Failure::new(failed, &server);
            assert_eq!(failure.detail(), said, "{failure:?}");
            assert_eq!(
                failure.error(),
                LookupError {
                    failure: failure.class(),
                    detail: said.to_owned(),
                }
            );
        }
        assert_eq!(Failure::no_servers().detail(), "no DNS servers to ask");
        assert_eq!(Failure::no_servers().class(), LookupFailure::Other);
    }

    #[test]
    fn words_carry_the_codes_the_clock_and_the_port() {
        let server = DnsServer {
            address: "192.0.2.53:5353".parse().unwrap(),
            wire: super::super::Wire::Plain,
        };
        let cases = [
            (Exchanged::Rcode(5), "192.0.2.53:5353: REFUSED"),
            (Exchanged::Rcode(9), "192.0.2.53:5353: RCODE 9"),
            (
                Exchanged::Bogus(BogusReason::SignatureExpired, vec![7, 9]),
                "192.0.2.53:5353: DNSSEC validation failed: the signatures have expired; \
                 the host clock may be wrong; Extended DNS Error codes 7, 9",
            ),
            (
                Exchanged::Stripped(vec![10]),
                "192.0.2.53:5353: the server returned no DNSSEC records; \
                 Extended DNS Error codes 10",
            ),
            (
                Exchanged::Unreached(TunnelError::Configuration("no route".into())),
                &TunnelError::Configuration("no route".into()).to_string(),
            ),
        ];
        for (failed, said) in cases {
            let failed = Failed::from(failed);
            assert_eq!(failed.detail_at(Some(&server)), said);
            assert_eq!(Failure::new(failed, &server).detail(), said);
        }
    }

    // A server on an encrypted wire is named as `Display for DnsServer`
    // names it: its address, with the port only when it is not the wire's
    // default, and never its TLS name or HTTPS path.
    #[test]
    fn words_name_encrypted_servers_by_address_and_port() {
        use super::super::Wire;
        let at = |address: &str, wire| DnsServer {
            address: address.parse().unwrap(),
            wire,
        };
        let cases = [
            (at("192.0.2.53:853", Wire::Tls { name: None }), "192.0.2.53"),
            (
                at(
                    "192.0.2.53:853",
                    Wire::Tls {
                        name: Some("dns.example".into()),
                    },
                ),
                "192.0.2.53",
            ),
            (
                at("192.0.2.53:8853", Wire::Tls { name: None }),
                "192.0.2.53:8853",
            ),
            (
                at(
                    "192.0.2.53:8853",
                    Wire::Tls {
                        name: Some("dns.example".into()),
                    },
                ),
                "192.0.2.53:8853",
            ),
            (
                at("[2001:db8::53]:853", Wire::Tls { name: None }),
                "2001:db8::53",
            ),
            (
                at("[2001:db8::53]:8853", Wire::Tls { name: None }),
                "[2001:db8::53]:8853",
            ),
            (
                at(
                    "192.0.2.53:443",
                    Wire::Https {
                        path: "/dns-query".into(),
                    },
                ),
                "192.0.2.53",
            ),
            (
                at(
                    "192.0.2.53:8443",
                    Wire::Https {
                        path: "/other".into(),
                    },
                ),
                "192.0.2.53:8443",
            ),
            // A TLS port on the plain wire is not its default.
            (at("192.0.2.53:853", Wire::Plain), "192.0.2.53:853"),
        ];
        for (server, named) in cases {
            assert_eq!(server.to_string(), named, "{server:?}");
            let failure = Failure::new(Exchanged::Tls("unknown issuer").into(), &server);
            assert_eq!(
                failure.detail(),
                format!("{named}: TLS failed: unknown issuer"),
                "{server:?}"
            );
            assert_eq!(
                Failure::new(Exchanged::Http(502).into(), &server).detail(),
                format!("{named}: HTTP 502"),
                "{server:?}"
            );
            assert_eq!(
                Failed::TimedOut.detail_at(Some(&server)),
                format!("{named}: no answer within 5s"),
                "{server:?}"
            );
        }
    }

    // Only a failure whose words name its server keeps a copy of it.
    #[test]
    fn the_server_is_kept_only_where_the_words_name_it() {
        let server = server();
        let unreached = Failure::new(
            Exchanged::Unreached(TunnelError::Engine("no route".into())).into(),
            &server,
        );
        assert_eq!(unreached.server, None);
        assert_eq!(unreached.detail(), "no route");
        let closed = Failure::new(Exchanged::Closed.into(), &server);
        assert_eq!(closed.server, Some(server));
        assert_eq!(Failure::no_servers().server, None);
    }

    // The most telling failure wins; of two that rank the same, the first.
    #[test]
    fn the_worst_is_the_highest_ranked_and_the_first_of_a_tie() {
        let server = server();
        let other = DnsServer::plain("192.0.2.54".parse().unwrap());
        let mut worst = Worst::default();
        assert!(worst.error().is_none());
        worst.consider(Exchanged::Closed.into(), &server);
        worst.consider(Failed::NotInPlain, &server);
        worst.consider(Exchanged::Invalid.into(), &other);
        assert_eq!(
            worst.error(),
            Some(LookupError {
                failure: LookupFailure::Other,
                detail: "192.0.2.53: not asked in plain after an encrypted server".into(),
            })
        );
        worst.consider(Failed::TimedOut, &server);
        worst.consider(Exchanged::Rcode(2).into(), &other);
        worst.consider(Exchanged::Http(500).into(), &server);
        worst.consider(Exchanged::Tls("unknown issuer").into(), &other);
        worst.consider(
            Exchanged::Unreached(TunnelError::Engine("x".into())).into(),
            &server,
        );
        assert_eq!(
            worst.error().map(|error| error.detail),
            Some("192.0.2.54: TLS failed: unknown issuer".into())
        );
        worst.consider(Failed::NoAddress, &server);
        assert!(!worst.nonexistent());
        worst.consider(Failed::NoSuchName, &other);
        assert!(worst.nonexistent());
        assert!(!worst.not_validated());
        worst.consider(Exchanged::Stripped(Vec::new()).into(), &server);
        assert!(!worst.nonexistent());
        assert!(worst.not_validated());
        worst.consider(
            Exchanged::Bogus(BogusReason::SignatureInvalid, Vec::new()).into(),
            &other,
        );
        worst.consider(
            Exchanged::Bogus(BogusReason::SignatureExpired, Vec::new()).into(),
            &server,
        );
        assert!(worst.not_validated());
        assert_eq!(
            worst.error(),
            Some(LookupError {
                failure: LookupFailure::Bogus,
                detail: "192.0.2.54: DNSSEC validation failed: \
                         the signatures do not match the records"
                    .into(),
            })
        );
    }

    // A lookup that reached `server` and had each of `failures` of it.
    fn unanswered(server: &DnsServer, failures: impl IntoIterator<Item = Failed>) -> Unanswered {
        let mut worst = Worst::default();
        for failed in failures {
            worst.consider(failed, server);
        }
        Unanswered {
            worst,
            reached: true,
        }
    }

    fn engine(error: &TunnelError) -> Option<&str> {
        match error {
            TunnelError::Engine(said) => Some(said),
            _ => None,
        }
    }

    // A lookup that found nothing for want of an answer that passed the
    // DNSSEC check is judged bogus and says so; any other is judged
    // nothing and says it could not resolve the destination.
    #[test]
    fn an_unanswered_lookup_comes_to_its_account_and_words() {
        let server = server();
        let bogus = unanswered(
            &server,
            [
                Exchanged::Rcode(2).into(),
                Exchanged::Bogus(BogusReason::SignatureInvalid, vec![6]).into(),
            ],
        );
        let account = bogus.account();
        assert_eq!(account.judged, Some(TrustOutcome::Bogus));
        assert_eq!(
            account.failure,
            Some(LookupError {
                failure: LookupFailure::Bogus,
                detail: "192.0.2.53: DNSSEC validation failed: \
                         the signatures do not match the records; Extended DNS Error codes 6"
                    .into(),
            })
        );
        assert_eq!(account.from, None);
        assert_eq!(engine(&bogus.error()), Some(NOT_VALIDATED));

        let stripped = unanswered(&server, [Exchanged::Stripped(Vec::new()).into()]);
        let account = stripped.account();
        assert_eq!(account.judged, Some(TrustOutcome::Bogus));
        assert_eq!(
            account.failure.map(|error| error.failure),
            Some(LookupFailure::DnssecUnavailable)
        );
        assert_eq!(engine(&stripped.error()), Some(NOT_VALIDATED));

        let failed = unanswered(&server, [Exchanged::Rcode(2).into(), Failed::TimedOut]);
        let account = failed.account();
        assert_eq!(account.judged, None);
        assert_eq!(
            account.failure,
            Some(LookupError {
                failure: LookupFailure::ServerFailure,
                detail: "192.0.2.53: SERVFAIL".into(),
            })
        );
        assert_eq!(account.from, None);
        assert_eq!(engine(&failed.error()), Some(UNRESOLVED));

        let empty = unanswered(&server, []);
        let account = empty.account();
        assert_eq!(account.judged, None);
        assert_eq!(account.failure, None);
        assert_eq!(engine(&empty.error()), Some(UNRESOLVED));
    }

    #[test]
    fn a_lookup_given_no_server_fails_on_its_configuration() {
        let unanswered = Unanswered::no_servers();
        assert!(!unanswered.reached);
        let account = unanswered.account();
        assert_eq!(account.judged, None);
        assert_eq!(
            account.failure,
            Some(LookupError {
                failure: LookupFailure::Other,
                detail: "no DNS servers to ask".into(),
            })
        );
        assert_eq!(account.from, None);
        assert!(matches!(
            unanswered.error(),
            TunnelError::Configuration(said) if said == "no DNS servers to ask"
        ));
    }
}
