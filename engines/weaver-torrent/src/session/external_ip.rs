// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Our address as the internet sees it, one for each address family: the
//! winner of a vote among those that see us (libtorrent `ip_voter`). Peers
//! vote with the `yourip` of their extended handshake, trackers with the
//! `external ip` of an accepted announce, and the gateway with the address
//! a port mapper got from it.

use std::{
    cmp::Ordering,
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::Arc,
    time::Duration,
};

use parking_lot::Mutex;
use tokio::time::Instant;
use tracing::debug;

use crate::torrent_state::live::peers::holepunch::is_local;
use crate::upnp::PortMapped;

/// Who told us our address (libtorrent `session_interface::ip_source_t`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Voter {
    /// A peer, at its address, by its `yourip`.
    Peer(IpAddr),
    /// A tracker, at the address its answer came from, by the answer's
    /// `external ip`.
    Tracker(IpAddr),
    /// The gateway, by a port mapper's report. Its address is not known,
    /// so every mapper votes as one voter (libtorrent votes the router as
    /// the unspecified address).
    Router,
}

impl Voter {
    // Who votes, by address: the router as the unspecified address of the
    // family it votes for.
    fn key(self, ip: IpAddr) -> IpAddr {
        match self {
            Voter::Peer(at) | Voter::Tracker(at) => at.to_canonical(),
            Voter::Router if ip.is_ipv4() => Ipv4Addr::UNSPECIFIED.into(),
            Voter::Router => Ipv6Addr::UNSPECIFIED.into(),
        }
    }

    // libtorrent's `source_peer`, `source_tracker` and `source_router`
    // bits: when the votes tie, an address the gateway named outranks one a
    // tracker named, which outranks one only peers named.
    fn kind(self) -> u8 {
        match self {
            Voter::Peer(_) => 1 << 1,
            Voter::Tracker(_) => 1 << 2,
            Voter::Router => 1 << 3,
        }
    }
}

// A round of voting ends at this many votes, or with a vote this long after
// it began; its winner is our address (libtorrent `ip_voter::maybe_rotate`).
const ROUND_VOTES: u32 = 50;
const ROUND_TIME: Duration = Duration::from_secs(5 * 60);
// Before a round has ever ended, the first address voted for stands until
// this many votes are in (`ip_voter::cast_vote`).
const FIRST_ROUND_VOTES: u32 = 25;
// The most addresses voted for at once (`ip_voter::cast_vote`).
const MAX_CANDIDATES: usize = 40;

struct Candidate {
    addr: IpAddr,
    voters: HashSet<IpAddr>,
    kinds: u8,
}

impl Candidate {
    fn votes(&self) -> usize {
        self.voters.len()
    }

    // Most votes first, then the higher kinds of voter (libtorrent
    // `ip_voter::external_ip_t::operator<`).
    fn rank(&self, other: &Self) -> Ordering {
        other
            .votes()
            .cmp(&self.votes())
            .then(other.kinds.cmp(&self.kinds))
    }
}

/// One address family's vote (libtorrent `ip_voter`).
struct IpVoter {
    candidates: Vec<Candidate>,
    address: Option<IpAddr>,
    // The votes of this round.
    votes: u32,
    // Whether a round has ended: from then on the address changes only
    // when a round does.
    settled: bool,
    round_began: Instant,
}

impl IpVoter {
    fn new(now: Instant) -> Self {
        Self {
            candidates: Vec::new(),
            address: None,
            votes: 0,
            settled: false,
            round_began: now,
        }
    }

    /// `voter` says we are at `ip`. Returns whether our address changed
    /// (libtorrent `ip_voter::cast_vote`): an address that is local, or a
    /// voter of the other family, counts for nothing, and a voter votes for
    /// an address once.
    fn cast_vote(&mut self, ip: IpAddr, voter: Voter, now: Instant) -> bool {
        let ip = ip.to_canonical();
        if ip.is_unspecified() || ip.is_loopback() || is_local(ip) {
            return false;
        }
        let key = voter.key(ip);
        if key.is_ipv4() != ip.is_ipv4() {
            return false;
        }
        let i = match self.candidates.iter().position(|c| c.addr == ip) {
            Some(i) => i,
            None => {
                if self.candidates.len() >= MAX_CANDIDATES {
                    // The fewest votes, the newest of those, gives way.
                    self.candidates.sort_by(Candidate::rank);
                    self.candidates.pop();
                }
                self.candidates.push(Candidate {
                    addr: ip,
                    voters: HashSet::new(),
                    kinds: 0,
                });
                self.candidates.len() - 1
            }
        };
        let candidate = &mut self.candidates[i];
        candidate.kinds |= voter.kind();
        if !candidate.voters.insert(key) {
            return self.end_round(now);
        }
        self.votes += 1;
        if self.settled {
            return self.end_round(now);
        }
        // Until a round ends, the first address voted for stands, unless
        // the leader takes it over once enough votes are in.
        let leader = self
            .candidates
            .iter()
            .min_by(|a, b| a.rank(b))
            .map(|c| c.addr);
        if leader == self.address {
            return self.end_round(now);
        }
        if self.address.is_some() {
            return self.votes >= FIRST_ROUND_VOTES && self.end_round(now);
        }
        self.address = leader;
        true
    }

    /// Ends the round when it is over, if an address won it: alone with
    /// two votes, or with half again the votes of the next (libtorrent
    /// `ip_voter::maybe_rotate`). The winner is our address and voting
    /// starts afresh. Returns whether our address changed.
    fn end_round(&mut self, now: Instant) -> bool {
        let over = self.votes >= ROUND_VOTES
            || (self.votes > 0 && now.duration_since(self.round_began) >= ROUND_TIME);
        if self.settled && !over {
            return false;
        }
        self.candidates.sort_by(Candidate::rank);
        let winner = match &self.candidates[..] {
            [only] if only.votes() >= 2 => only.addr,
            [first, second, ..] if first.votes() * 2 / 3 > second.votes() => first.addr,
            _ => return false,
        };
        let changed = self.address != Some(winner);
        self.address = Some(winner);
        self.candidates.clear();
        self.votes = 0;
        self.settled = true;
        self.round_began = now;
        changed
    }
}

/// The session's external addresses, one for each address family, and
/// every reader's one source of them (libtorrent
/// `listen_socket_t::external_address`, one listen socket to a family).
pub(crate) struct ExternalIps {
    v4: Mutex<IpVoter>,
    v6: Mutex<IpVoter>,
}

impl ExternalIps {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            v4: Mutex::new(IpVoter::new(now)),
            v6: Mutex::new(IpVoter::new(now)),
        }
    }

    /// `voter` says we are at `ip`: a vote in the family of `ip`
    /// (libtorrent `session_impl::set_external_address`). Returns whether
    /// that family's address changed.
    pub(crate) fn cast_vote(&self, ip: IpAddr, voter: Voter, now: Instant) -> bool {
        let ip = ip.to_canonical();
        let family = if ip.is_ipv4() { &self.v4 } else { &self.v6 };
        let changed = family.lock().cast_vote(ip, voter, now);
        if changed {
            debug!(%ip, ?voter, "external address changed");
        }
        changed
    }

    /// A port mapper's report: the address the gateway gave a mapping is a
    /// vote of the router (libtorrent `session_impl::on_port_mapping`).
    pub(crate) fn on_port_mapping(&self, event: &PortMapped, now: Instant) {
        if event.error.is_none()
            && let Some(ip) = event.external_ip
        {
            self.cast_vote(ip, Voter::Router, now);
        }
    }

    /// The vote of a tracker that accepted an announce: the address it says
    /// we are at, by the tracker at the address its answer came from
    /// (libtorrent `torrent::tracker_response`).
    pub(crate) fn tracker_vote(self: &Arc<Self>) -> crate::tracker::ExternalIpVote {
        let ips = Arc::clone(self);
        Arc::new(move |ip, tracker| {
            ips.cast_vote(ip, Voter::Tracker(tracker), Instant::now());
        })
    }

    /// Our addresses, IPv4 first, of the families we know one for.
    pub(crate) fn get(&self) -> Vec<IpAddr> {
        [self.v4.lock().address, self.v6.lock().address]
            .into_iter()
            .flatten()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn peer(s: &str) -> Voter {
        Voter::Peer(ip(s))
    }

    #[tokio::test(start_paused = true)]
    async fn of_three_peers_naming_two_addresses_the_majority_wins() {
        let now = Instant::now();
        let ips = ExternalIps::new(now);
        assert!(ips.cast_vote(ip("203.0.113.5"), peer("198.51.100.1"), now));
        assert!(!ips.cast_vote(ip("203.0.113.9"), peer("198.51.100.2"), now));
        assert!(!ips.cast_vote(ip("203.0.113.5"), peer("198.51.100.3"), now));
        assert_eq!(ips.get(), [ip("203.0.113.5")]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_address_named_gives_way_to_one_that_out_votes_it() {
        let now = Instant::now();
        let ips = ExternalIps::new(now);
        assert!(ips.cast_vote(ip("203.0.113.5"), peer("198.51.100.1"), now));
        for n in 2..FIRST_ROUND_VOTES {
            let voter = peer(&format!("198.51.100.{n}"));
            assert!(!ips.cast_vote(ip("203.0.113.9"), voter, now));
            assert_eq!(ips.get(), [ip("203.0.113.5")]);
        }
        let last = peer(&format!("198.51.100.{FIRST_ROUND_VOTES}"));
        assert!(ips.cast_vote(ip("203.0.113.9"), last, now));
        assert_eq!(ips.get(), [ip("203.0.113.9")]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_settled_address_changes_only_when_a_round_ends() {
        let now = Instant::now();
        let ips = ExternalIps::new(now);
        ips.cast_vote(ip("203.0.113.5"), peer("198.51.100.1"), now);
        ips.cast_vote(ip("203.0.113.5"), peer("198.51.100.2"), now);
        // A new round: three votes for another address are no change yet.
        for n in 3..6 {
            let voter = peer(&format!("198.51.100.{n}"));
            assert!(!ips.cast_vote(ip("203.0.113.9"), voter, now));
        }
        assert_eq!(ips.get(), [ip("203.0.113.5")]);
        // Five minutes on, the next vote ends the round, and the new
        // address has won it.
        tokio::time::advance(ROUND_TIME).await;
        let later = Instant::now();
        assert!(ips.cast_vote(ip("203.0.113.9"), peer("198.51.100.6"), later));
        assert_eq!(ips.get(), [ip("203.0.113.9")]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_local_address_or_a_voter_of_the_other_family_counts_for_nothing() {
        let now = Instant::now();
        let ips = ExternalIps::new(now);
        for local in [
            "127.0.0.1",
            "10.0.0.2",
            "192.168.1.9",
            "::1",
            "fe80::1",
            "0.0.0.0",
        ] {
            assert!(!ips.cast_vote(ip(local), peer("198.51.100.1"), now));
        }
        assert!(!ips.cast_vote(ip("203.0.113.5"), peer("2001:db8::1"), now));
        assert!(!ips.cast_vote(ip("2001:db8::5"), peer("198.51.100.1"), now));
        assert_eq!(ips.get(), [] as [IpAddr; 0]);
        // A v4-mapped voter is an IPv4 one.
        assert!(ips.cast_vote(ip("203.0.113.5"), peer("::ffff:198.51.100.1"), now));
        assert!(ips.cast_vote(ip("2001:db8::5"), peer("2001:db8::1"), now));
        assert_eq!(ips.get(), [ip("203.0.113.5"), ip("2001:db8::5")]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_voter_counts_once_for_an_address() {
        let now = Instant::now();
        let ips = ExternalIps::new(now);
        ips.cast_vote(ip("203.0.113.5"), peer("198.51.100.1"), now);
        // Its second vote would have settled the address on two votes.
        ips.cast_vote(ip("203.0.113.5"), peer("198.51.100.1"), now);
        assert!(!ips.v4.lock().settled);
        assert_eq!(ips.v4.lock().candidates[0].votes(), 1);
        ips.cast_vote(ip("203.0.113.5"), peer("198.51.100.2"), now);
        assert!(ips.v4.lock().settled);
    }

    #[tokio::test(start_paused = true)]
    async fn a_tracker_votes_as_one_voter_at_its_address_and_ties_go_its_way() {
        let now = Instant::now();
        let ips = Arc::new(ExternalIps::new(now));
        let vote = ips.tracker_vote();
        vote(ip("203.0.113.5"), ip("198.51.100.1"));
        assert_eq!(ips.get(), [ip("203.0.113.5")]);
        // Its second answer is the same voter again.
        vote(ip("203.0.113.5"), ip("198.51.100.1"));
        assert_eq!(ips.v4.lock().candidates[0].votes(), 1);
        // A tracker of the other family counts for nothing here.
        vote(ip("203.0.113.5"), ip("2001:db8::1"));
        assert_eq!(ips.v4.lock().candidates[0].votes(), 1);
        // A peer naming another address ties it: the tracker's outranks it.
        ips.cast_vote(ip("203.0.113.9"), peer("198.51.100.2"), now);
        let mut candidates = std::mem::take(&mut ips.v4.lock().candidates);
        candidates.sort_by(Candidate::rank);
        assert_eq!(candidates[0].addr, ip("203.0.113.5"));
    }

    #[tokio::test(start_paused = true)]
    async fn the_port_mappers_vote_as_one_router() {
        use crate::upnp::{PortmapProtocol::Tcp, PortmapTransport};
        let now = Instant::now();
        let ips = ExternalIps::new(now);
        let mapped = |transport, external_ip, error: Option<&str>| PortMapped {
            transport,
            protocol: Tcp,
            local_port: 6881,
            external_port: Some(6881),
            external_ip: Some(ip(external_ip)),
            error: error.map(str::to_owned),
        };
        // A failed mapping says nothing of our address.
        ips.on_port_mapping(
            &mapped(PortmapTransport::Upnp, "203.0.113.9", Some("refused")),
            now,
        );
        assert_eq!(ips.get(), [] as [IpAddr; 0]);
        ips.on_port_mapping(&mapped(PortmapTransport::NatPmp, "203.0.113.5", None), now);
        ips.on_port_mapping(&mapped(PortmapTransport::Upnp, "203.0.113.5", None), now);
        assert_eq!(ips.get(), [ip("203.0.113.5")]);
        assert_eq!(ips.v4.lock().candidates[0].votes(), 1);
        // A peer that agrees makes two voters.
        ips.cast_vote(ip("203.0.113.5"), peer("198.51.100.1"), now);
        assert!(ips.v4.lock().settled);
    }
}
