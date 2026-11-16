// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use weaver_server_core::auth::service::verify_browser_csrf_token;
use weaver_server_core::auth::{generate_setup_code, hash_api_key, normalize_setup_code};
use weaver_server_core::security::{canonical_ip, ip_is_loopback};

// Failures one client may make inside the window.
const MAX_FAILURES: usize = 5;
const FAILURE_WINDOW: Duration = Duration::from_secs(60);
// Clients whose failures are remembered at once. A client not among them is
// refused while the table is full, before its code is looked at, so guessing
// is capped at this many clients times `MAX_FAILURES` a window however many
// addresses a guesser rotates through. The cost: 1,024 clients failing inside
// one window keep setup closed to any new client until their failures expire,
// except loopback, which has its own slot outside the table.
const MAX_TRACKED_CLIENTS: usize = 1024;
// An IPv6 client is its /64: one host commonly holds a whole /64, so each of
// its addresses is not a separate client.
const IPV6_CLIENT_PREFIX: u32 = 64;

// Asks for a setup code even on loopback, to try the flow a container gets.
const ENV_REQUIRE_SETUP_CODE: &str = "WEAVER_REQUIRE_SETUP_CODE";

// Whether first-time setup has to be proven with the code from the console.
//
// A Weaver listening on loopback alone can only be opened from this machine,
// so reaching the wizard is proof enough. Anything wider needs the code: a
// container's `0.0.0.0`, a LAN address, or a reverse proxy relaying browsers
// from elsewhere to a loopback listener.
pub(super) fn setup_code_required(bind_address: IpAddr, behind_trusted_proxy: bool) -> bool {
    let forced = std::env::var(ENV_REQUIRE_SETUP_CODE)
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    forced || behind_trusted_proxy || !weaver_server_core::security::ip_is_loopback(bind_address)
}

#[derive(Clone)]
pub(super) struct SetupChallenge(Arc<Mutex<State>>);

struct State {
    // `None` for a Weaver only this machine can reach, whose setup needs no
    // code: being able to open it at all already proves the operator is here.
    verifier: Option<String>,
    // Recent failures per client, so one client cannot hold setup closed for
    // everyone else.
    failures: HashMap<IpAddr, VecDeque<Instant>>,
    // Loopback's failures, outside the table: a full table never refuses
    // this machine, so the owner is not locked out by a flood. Its own limit
    // still applies, since a reverse proxy on this host that is not trusted
    // makes every browser it relays look like loopback.
    loopback: VecDeque<Instant>,
    // While the table is full, no entry can expire before this, so a new
    // client is refused without walking the table. It is the earliest expiry
    // seen by the last walk; failures only move an entry's expiry later, so
    // it never runs ahead of the real one. `None` until the first walk.
    full_until: Option<Instant>,
    #[cfg(test)]
    walks: usize,
    consumed: bool,
}

impl State {
    fn new(verifier: Option<String>) -> Self {
        Self {
            verifier,
            failures: HashMap::new(),
            loopback: VecDeque::new(),
            full_until: None,
            #[cfg(test)]
            walks: 0,
            consumed: false,
        }
    }

    // Whether `client` may try a code at `now`. A tracked client always may,
    // and its own limit applies; a new one only while the table has room.
    // When it is full, expired entries are dropped first. That walk runs
    // only for a new client at a full table, and only once `full_until` has
    // passed, so it runs at most once per expiry of a tracked failure, not
    // once per refused request.
    fn admits(&mut self, client: Client, now: Instant) -> bool {
        let Client::Remote(client) = client else {
            return true;
        };
        if self.failures.contains_key(&client) || self.failures.len() < MAX_TRACKED_CLIENTS {
            return true;
        }
        if self.full_until.is_some_and(|until| now < until) {
            return false;
        }
        #[cfg(test)]
        {
            self.walks += 1;
        }
        self.failures.retain(|_, times| {
            times
                .back()
                .is_some_and(|then| now.duration_since(*then) < FAILURE_WINDOW)
        });
        self.full_until = self
            .failures
            .values()
            .filter_map(VecDeque::back)
            .min()
            .map(|then| *then + FAILURE_WINDOW);
        self.failures.len() < MAX_TRACKED_CLIENTS
    }
    // The failures `client` made inside the window ending at `now`.
    fn recent_failures(&mut self, client: Client, now: Instant) -> usize {
        let times = match client {
            Client::Loopback => &mut self.loopback,
            Client::Remote(client) => match self.failures.get_mut(&client) {
                Some(times) => times,
                None => return 0,
            },
        };
        while times
            .front()
            .is_some_and(|then| now.duration_since(*then) >= FAILURE_WINDOW)
        {
            times.pop_front();
        }
        let count = times.len();
        if let (0, Client::Remote(client)) = (count, client) {
            self.failures.remove(&client);
        }
        count
    }

    // Only a client `admits` let in reaches here, so the table never grows
    // past its bound.
    fn record_failure(&mut self, client: Client, now: Instant) {
        let Client::Remote(client) = client else {
            self.loopback.push_back(now);
            return;
        };
        debug_assert!(
            self.failures.contains_key(&client) || self.failures.len() < MAX_TRACKED_CLIENTS
        );
        self.failures.entry(client).or_default().push_back(now);
    }
}

// Who a request counts against: loopback, in any of its forms, is one
// client with its own slot; any other address is a client of the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Client {
    Loopback,
    Remote(IpAddr),
}

fn client_of(address: IpAddr) -> Client {
    if ip_is_loopback(address) {
        Client::Loopback
    } else {
        Client::Remote(client_key(address))
    }
}

// The table key of an address: its canonical form, with an IPv6 address cut
// to its /64. IPv4, and IPv4 in the mapped form, stay whole addresses.
fn client_key(address: IpAddr) -> IpAddr {
    match canonical_ip(address) {
        IpAddr::V6(address) => {
            let mask = u128::MAX << (128 - IPV6_CLIENT_PREFIX);
            IpAddr::V6((u128::from(address) & mask).into())
        }
        address => address,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SetupCodeError {
    Missing,
    Invalid,
    RateLimited,
    Consumed,
}

impl SetupChallenge {
    // Generates a short setup code and returns it once. Only its verifier
    // remains in the cloneable challenge state.
    pub(super) fn generate() -> (Self, String) {
        let code = generate_setup_code();
        let verifier = hex_hash(hash_api_key(&normalize_setup_code(&code)));
        (Self(Arc::new(Mutex::new(State::new(Some(verifier))))), code)
    }

    // Setup without a code, for a Weaver that listens on loopback only.
    pub(super) fn open() -> Self {
        Self(Arc::new(Mutex::new(State::new(None))))
    }

    pub(super) fn code_required(&self) -> bool {
        self.0
            .lock()
            .expect("setup challenge lock poisoned")
            .verifier
            .is_some()
    }

    // Checks `code` for a request attributed to `client`. Wrong and missing
    // codes count against that client only (its /64 for IPv6).
    pub(super) fn verify(&self, code: Option<&str>, client: IpAddr) -> Result<(), SetupCodeError> {
        self.verify_at(code, client, Instant::now())
    }

    pub(super) fn is_available(&self) -> bool {
        !self
            .0
            .lock()
            .expect("setup challenge lock poisoned")
            .consumed
    }

    fn verify_at(
        &self,
        code: Option<&str>,
        client: IpAddr,
        now: Instant,
    ) -> Result<(), SetupCodeError> {
        let client = client_of(client);
        let mut state = self.0.lock().expect("setup challenge lock poisoned");
        if state.consumed {
            return Err(SetupCodeError::Consumed);
        }
        let Some(verifier) = state.verifier.clone() else {
            return Ok(());
        };
        // Before the code is looked at: a refused client learns nothing
        // about it and leaves nothing in the table.
        if !state.admits(client, now) || state.recent_failures(client, now) >= MAX_FAILURES {
            return Err(SetupCodeError::RateLimited);
        }
        // Accept the code however it was typed: any case, with or without
        // its hyphen.
        let Some(code) = code
            .map(normalize_setup_code)
            .filter(|code| !code.is_empty())
        else {
            state.record_failure(client, now);
            return Err(SetupCodeError::Missing);
        };
        if !verify_browser_csrf_token(&code, &verifier) {
            state.record_failure(client, now);
            return Err(SetupCodeError::Invalid);
        }
        Ok(())
    }

    // Call only after the setup database transaction commits. Failed writes
    // intentionally leave the challenge retryable.
    pub(super) fn consume(&self) {
        self.0
            .lock()
            .expect("setup challenge lock poisoned")
            .consumed = true;
    }
}

fn hex_hash(hash: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in hash {
        value.push(HEX[(byte >> 4) as usize] as char);
        value.push(HEX[(byte & 15) as usize] as char);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_code_remains_usable_until_commit_consumes_it() {
        let (challenge, code) = SetupChallenge::generate();
        assert!(challenge.is_available());
        assert_eq!(challenge.verify(Some(&code), CLIENT), Ok(()));
        assert_eq!(challenge.verify(Some(&code), CLIENT), Ok(()));
        challenge.consume();
        assert!(!challenge.is_available());
        assert_eq!(
            challenge.verify(Some(&code), CLIENT),
            Err(SetupCodeError::Consumed)
        );
    }

    #[test]
    fn only_a_loopback_listener_without_a_proxy_skips_the_code() {
        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(!setup_code_required(loopback, false));
        assert!(!setup_code_required("::1".parse().unwrap(), false));
        assert!(setup_code_required(loopback, true));
        assert!(setup_code_required("0.0.0.0".parse().unwrap(), false));
        assert!(setup_code_required("192.168.1.20".parse().unwrap(), false));
    }

    #[test]
    fn an_open_challenge_takes_no_code_and_is_still_single_use() {
        let challenge = SetupChallenge::open();
        assert!(!challenge.code_required());
        assert_eq!(challenge.verify(None, CLIENT), Ok(()));
        assert_eq!(challenge.verify(Some("anything"), CLIENT), Ok(()));
        challenge.consume();
        assert_eq!(
            challenge.verify(None, CLIENT),
            Err(SetupCodeError::Consumed)
        );
    }

    #[test]
    fn codes_are_short_and_accepted_in_any_case_with_or_without_the_hyphen() {
        let (challenge, code) = SetupChallenge::generate();
        assert!(weaver_server_core::auth::is_setup_code(&code), "{code}");
        assert_eq!(
            challenge.verify(Some(&format!(" {} ", code.to_ascii_lowercase())), CLIENT),
            Ok(())
        );
        assert_eq!(
            challenge.verify(Some(&code.replace('-', "")), CLIENT),
            Ok(())
        );
        assert_eq!(
            weaver_server_core::auth::normalize_setup_code(&code).len(),
            8
        );
    }

    #[test]
    fn a_six_character_code_is_refused() {
        let (challenge, code) = SetupChallenge::generate();
        let normal = weaver_server_core::auth::normalize_setup_code(&code);
        // The first six characters of the right code, in the old 3-3 shape.
        let short = format!("{}-{}", &normal[..3], &normal[3..6]);
        assert_eq!(
            challenge.verify(Some(&short), CLIENT),
            Err(SetupCodeError::Invalid)
        );
        assert_eq!(
            challenge.verify(Some(&normal[..6]), CLIENT),
            Err(SetupCodeError::Invalid)
        );
        assert_eq!(challenge.verify(Some(&code), CLIENT), Ok(()));
    }

    const CLIENT: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(192, 168, 1, 20));

    #[test]
    fn missing_and_wrong_codes_share_one_client_limit() {
        let (challenge, _) = SetupChallenge::generate();
        for _ in 0..3 {
            assert_eq!(challenge.verify(None, CLIENT), Err(SetupCodeError::Missing));
        }
        for _ in 0..2 {
            assert_eq!(
                challenge.verify(Some("wrong"), CLIENT),
                Err(SetupCodeError::Invalid)
            );
        }
        assert_eq!(
            challenge.verify(Some("wrong"), CLIENT),
            Err(SetupCodeError::RateLimited)
        );
    }

    #[test]
    fn failure_window_expires() {
        let (challenge, _) = SetupChallenge::generate();
        let now = Instant::now();
        for _ in 0..MAX_FAILURES {
            assert_eq!(
                challenge.verify_at(Some("wrong"), CLIENT, now),
                Err(SetupCodeError::Invalid)
            );
        }
        assert_eq!(
            challenge.verify_at(Some("wrong"), CLIENT, now),
            Err(SetupCodeError::RateLimited)
        );
        assert_eq!(
            challenge.verify_at(Some("wrong"), CLIENT, now + FAILURE_WINDOW),
            Err(SetupCodeError::Invalid)
        );
    }

    #[test]
    fn one_client_at_its_limit_does_not_lock_out_another() {
        let (challenge, code) = SetupChallenge::generate();
        let attacker: IpAddr = "192.168.1.66".parse().unwrap();
        let now = Instant::now();
        for _ in 0..MAX_FAILURES {
            assert_eq!(
                challenge.verify_at(Some("wrong"), attacker, now),
                Err(SetupCodeError::Invalid)
            );
        }
        assert_eq!(
            challenge.verify_at(Some(&code), attacker, now),
            Err(SetupCodeError::RateLimited)
        );
        assert_eq!(challenge.verify_at(Some(&code), CLIENT, now), Ok(()));
        // The mapped form of an address is the same client.
        let mapped: IpAddr = "::ffff:192.168.1.66".parse().unwrap();
        assert_eq!(
            challenge.verify_at(Some(&code), mapped, now),
            Err(SetupCodeError::RateLimited)
        );
    }

    fn tracked(challenge: &SetupChallenge) -> usize {
        challenge.0.lock().unwrap().failures.len()
    }

    fn walks(challenge: &SetupChallenge) -> usize {
        challenge.0.lock().unwrap().walks
    }

    // A distinct IPv4 client per index.
    fn nth_client(index: u32) -> IpAddr {
        IpAddr::V4(std::net::Ipv4Addr::from(0x0a00_0000 + index))
    }

    // Fills the table with one failure from each of `MAX_TRACKED_CLIENTS`
    // clients at `now`.
    fn fill(challenge: &SetupChallenge, now: Instant) {
        for index in 0..MAX_TRACKED_CLIENTS as u32 {
            assert_eq!(
                challenge.verify_at(Some("wrong"), nth_client(index), now),
                Err(SetupCodeError::Invalid)
            );
        }
        assert_eq!(tracked(challenge), MAX_TRACKED_CLIENTS);
    }

    #[test]
    fn an_ipv6_client_is_its_64() {
        let (challenge, code) = SetupChallenge::generate();
        let now = Instant::now();
        let first: IpAddr = "2001:db8:1:2::1".parse().unwrap();
        let same_64: IpAddr = "2001:db8:1:2:ffff:ffff:ffff:fffe".parse().unwrap();
        let other_64: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        for _ in 0..3 {
            assert_eq!(
                challenge.verify_at(Some("wrong"), first, now),
                Err(SetupCodeError::Invalid)
            );
        }
        for _ in 0..2 {
            assert_eq!(
                challenge.verify_at(Some("wrong"), same_64, now),
                Err(SetupCodeError::Invalid)
            );
        }
        // Five failures across the /64: both addresses are at the limit.
        assert_eq!(
            challenge.verify_at(Some(&code), first, now),
            Err(SetupCodeError::RateLimited)
        );
        assert_eq!(
            challenge.verify_at(Some(&code), same_64, now),
            Err(SetupCodeError::RateLimited)
        );
        assert_eq!(tracked(&challenge), 1);
        // The next /64 has a count of its own.
        assert_eq!(challenge.verify_at(Some(&code), other_64, now), Ok(()));
    }

    #[test]
    fn an_ipv4_client_stays_its_whole_address() {
        let v4: IpAddr = "192.168.1.66".parse().unwrap();
        assert_eq!(client_key(v4), v4);
        assert_eq!(client_key("::ffff:192.168.1.66".parse().unwrap()), v4);
        assert_ne!(client_key("192.168.1.67".parse().unwrap()), v4);
        assert_eq!(
            client_key("2001:db8:1:2:3:4:5:6".parse().unwrap()),
            "2001:db8:1:2::".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn a_full_table_refuses_a_new_client_before_its_code_is_compared() {
        let (challenge, code) = SetupChallenge::generate();
        let now = Instant::now();
        fill(&challenge, now);
        let newcomer = CLIENT;
        // The right code is refused: the code was never compared.
        assert_eq!(
            challenge.verify_at(Some(&code), newcomer, now),
            Err(SetupCodeError::RateLimited)
        );
        assert_eq!(
            challenge.verify_at(None, newcomer, now),
            Err(SetupCodeError::RateLimited)
        );
        // And it was not recorded.
        assert_eq!(tracked(&challenge), MAX_TRACKED_CLIENTS);
        assert!(!challenge.0.lock().unwrap().failures.contains_key(&newcomer));

        // Once the window has passed, the full entries expire and it gets in.
        assert_eq!(
            challenge.verify_at(Some(&code), newcomer, now + FAILURE_WINDOW),
            Ok(())
        );
    }

    const LOOPBACKS: [&str; 4] = ["127.0.0.1", "127.8.9.10", "::1", "::ffff:127.0.0.1"];

    // A full table never refuses this machine: loopback in each of its forms
    // gets in with the right code.
    #[test]
    fn a_full_table_admits_loopback() {
        for address in LOOPBACKS {
            let (challenge, code) = SetupChallenge::generate();
            let now = Instant::now();
            fill(&challenge, now);
            let loopback: IpAddr = address.parse().unwrap();
            assert_eq!(
                challenge.verify_at(Some(&code), loopback, now),
                Ok(()),
                "{address}"
            );
            // A new client from elsewhere is still refused.
            assert_eq!(
                challenge.verify_at(Some(&code), CLIENT, now),
                Err(SetupCodeError::RateLimited)
            );
        }
    }

    // Loopback keeps its own limit while the table is full, shared by all of
    // its forms, and its failures take no place in the table.
    #[test]
    fn loopback_stops_at_its_own_limit_while_the_table_is_full() {
        let (challenge, code) = SetupChallenge::generate();
        let now = Instant::now();
        fill(&challenge, now);
        for (attempt, address) in LOOPBACKS.iter().cycle().take(MAX_FAILURES).enumerate() {
            assert_eq!(
                challenge.verify_at(Some("wrong"), address.parse().unwrap(), now),
                Err(SetupCodeError::Invalid),
                "attempt {attempt}"
            );
        }
        for address in LOOPBACKS {
            assert_eq!(
                challenge.verify_at(Some(&code), address.parse().unwrap(), now),
                Err(SetupCodeError::RateLimited),
                "{address}"
            );
        }
        assert_eq!(tracked(&challenge), MAX_TRACKED_CLIENTS);
        // Its window passes as any client's does.
        assert_eq!(
            challenge.verify_at(
                Some(&code),
                "127.0.0.1".parse().unwrap(),
                now + FAILURE_WINDOW
            ),
            Ok(())
        );
    }

    #[test]
    fn a_tracked_client_under_its_limit_succeeds_while_the_table_is_full() {
        let (challenge, code) = SetupChallenge::generate();
        let now = Instant::now();
        fill(&challenge, now);
        let tracked_client = nth_client(7);
        for _ in 1..MAX_FAILURES - 1 {
            assert_eq!(
                challenge.verify_at(Some("wrong"), tracked_client, now),
                Err(SetupCodeError::Invalid)
            );
        }
        assert_eq!(
            challenge.verify_at(Some(&code), tracked_client, now),
            Ok(())
        );
        // Its own fifth failure still limits it.
        assert_eq!(
            challenge.verify_at(Some("wrong"), tracked_client, now),
            Err(SetupCodeError::Invalid)
        );
        assert_eq!(
            challenge.verify_at(Some(&code), tracked_client, now),
            Err(SetupCodeError::RateLimited)
        );
    }

    #[test]
    fn rotating_addresses_get_no_more_guesses_than_the_table_holds() {
        let (challenge, _) = SetupChallenge::generate();
        let now = Instant::now();
        let addresses = MAX_TRACKED_CLIENTS as u32 * 2 + 1;
        let mut compared = vec![0usize; addresses as usize];
        // Seven rounds over twice the table's worth of addresses, all inside
        // one window: every guess that reached the comparison is counted.
        for _ in 0..7 {
            for index in 0..addresses {
                match challenge.verify_at(Some("wrong"), nth_client(index), now) {
                    Err(SetupCodeError::Invalid) => compared[index as usize] += 1,
                    Err(SetupCodeError::RateLimited) => {}
                    other => panic!("{other:?}"),
                }
                assert!(tracked(&challenge) <= MAX_TRACKED_CLIENTS);
            }
        }
        assert!(compared.iter().all(|guesses| *guesses <= MAX_FAILURES));
        assert_eq!(
            compared.iter().sum::<usize>(),
            MAX_TRACKED_CLIENTS * MAX_FAILURES
        );
        // Thousands of refusals cost one walk of the table, not one each.
        assert_eq!(walks(&challenge), 1);
    }

    #[test]
    fn expired_entries_are_dropped_and_the_walk_waits_for_the_next_expiry() {
        let (challenge, _) = SetupChallenge::generate();
        let now = Instant::now();
        fill(&challenge, now);
        let later = now + FAILURE_WINDOW / 2;
        // One tracked client fails again halfway through, so its entry
        // outlives the rest.
        assert_eq!(
            challenge.verify_at(Some("wrong"), nth_client(0), later),
            Err(SetupCodeError::Invalid)
        );
        assert_eq!(
            challenge.verify_at(Some("wrong"), CLIENT, later),
            Err(SetupCodeError::RateLimited)
        );
        assert_eq!(walks(&challenge), 1);
        assert_eq!(
            challenge.verify_at(Some("wrong"), CLIENT, later),
            Err(SetupCodeError::RateLimited)
        );
        assert_eq!(walks(&challenge), 1);

        // At the first expiry everyone but that client is dropped.
        let expiry = now + FAILURE_WINDOW;
        assert_eq!(
            challenge.verify_at(Some("wrong"), CLIENT, expiry),
            Err(SetupCodeError::Invalid)
        );
        assert_eq!(walks(&challenge), 2);
        assert_eq!(tracked(&challenge), 2);
    }
}
