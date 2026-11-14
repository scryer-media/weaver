// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use weaver_server_core::auth::service::verify_browser_csrf_token;
use weaver_server_core::auth::{generate_setup_code, hash_api_key, normalize_setup_code};
use weaver_server_core::security::canonical_ip;

// Failures one client address may make inside the window.
const MAX_FAILURES: usize = 5;
const FAILURE_WINDOW: Duration = Duration::from_secs(60);
// Client addresses whose failures are remembered at once. Past this, the
// address whose last failure is oldest is forgotten first.
const MAX_TRACKED_CLIENTS: usize = 1024;

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
    // Recent failures per client address, so one client cannot hold setup
    // closed for everyone else.
    failures: HashMap<IpAddr, VecDeque<Instant>>,
    consumed: bool,
}

impl State {
    // The failures `client` made inside the window ending at `now`.
    fn recent_failures(&mut self, client: IpAddr, now: Instant) -> usize {
        let Some(times) = self.failures.get_mut(&client) else {
            return 0;
        };
        while times
            .front()
            .is_some_and(|then| now.duration_since(*then) >= FAILURE_WINDOW)
        {
            times.pop_front();
        }
        let count = times.len();
        if count == 0 {
            self.failures.remove(&client);
        }
        count
    }

    fn record_failure(&mut self, client: IpAddr, now: Instant) {
        if !self.failures.contains_key(&client) && self.failures.len() >= MAX_TRACKED_CLIENTS {
            self.failures.retain(|_, times| {
                times
                    .back()
                    .is_some_and(|then| now.duration_since(*then) < FAILURE_WINDOW)
            });
            if self.failures.len() >= MAX_TRACKED_CLIENTS
                && let Some(stalest) = self
                    .failures
                    .iter()
                    .min_by_key(|(_, times)| times.back().copied())
                    .map(|(ip, _)| *ip)
            {
                self.failures.remove(&stalest);
            }
        }
        self.failures.entry(client).or_default().push_back(now);
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
        (
            Self(Arc::new(Mutex::new(State {
                verifier: Some(verifier),
                failures: HashMap::new(),
                consumed: false,
            }))),
            code,
        )
    }

    // Setup without a code, for a Weaver that listens on loopback only.
    pub(super) fn open() -> Self {
        Self(Arc::new(Mutex::new(State {
            verifier: None,
            failures: HashMap::new(),
            consumed: false,
        })))
    }

    pub(super) fn code_required(&self) -> bool {
        self.0
            .lock()
            .expect("setup challenge lock poisoned")
            .verifier
            .is_some()
    }

    // Checks `code` for a request attributed to `client`. Wrong and missing
    // codes count against that client address only.
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
        let client = canonical_ip(client);
        let mut state = self.0.lock().expect("setup challenge lock poisoned");
        if state.consumed {
            return Err(SetupCodeError::Consumed);
        }
        let Some(verifier) = state.verifier.clone() else {
            return Ok(());
        };
        if state.recent_failures(client, now) >= MAX_FAILURES {
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

    #[test]
    fn the_failure_table_stays_bounded() {
        let (challenge, _) = SetupChallenge::generate();
        let now = Instant::now();
        let tracked = |challenge: &SetupChallenge| challenge.0.lock().unwrap().failures.len();
        for index in 0..(MAX_TRACKED_CLIENTS as u32 * 2) {
            let client = IpAddr::V4(std::net::Ipv4Addr::from(0x0a00_0000 + index));
            assert_eq!(
                challenge.verify_at(Some("wrong"), client, now),
                Err(SetupCodeError::Invalid)
            );
            assert!(tracked(&challenge) <= MAX_TRACKED_CLIENTS);
        }
        assert_eq!(tracked(&challenge), MAX_TRACKED_CLIENTS);

        // Expired entries are dropped before anyone still inside the window.
        let later = now + FAILURE_WINDOW;
        for _ in 0..MAX_FAILURES {
            let _ = challenge.verify_at(Some("wrong"), CLIENT, later);
        }
        assert_eq!(tracked(&challenge), 1);
        assert_eq!(
            challenge.verify_at(Some("wrong"), CLIENT, later),
            Err(SetupCodeError::RateLimited)
        );
    }
}
