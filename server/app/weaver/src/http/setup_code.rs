use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use weaver_server_core::auth::service::verify_browser_csrf_token;
use weaver_server_core::auth::{generate_setup_code, hash_api_key, normalize_setup_code};

const MAX_FAILURES: usize = 5;
const FAILURE_WINDOW: Duration = Duration::from_secs(60);

/// Asks for a setup code even on loopback, to try the flow a container gets.
const ENV_REQUIRE_SETUP_CODE: &str = "WEAVER_REQUIRE_SETUP_CODE";

/// Whether first-time setup has to be proven with the code from the console.
///
/// A Weaver listening on loopback alone can only be opened from this machine,
/// so reaching the wizard is proof enough. Anything wider needs the code: a
/// container's `0.0.0.0`, a LAN address, or a reverse proxy relaying browsers
/// from elsewhere to a loopback listener.
pub(super) fn setup_code_required(bind_address: IpAddr, behind_trusted_proxy: bool) -> bool {
    let forced = std::env::var(ENV_REQUIRE_SETUP_CODE)
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    forced || behind_trusted_proxy || !weaver_server_core::security::ip_is_loopback(bind_address)
}

#[derive(Clone)]
pub(super) struct SetupChallenge(Arc<Mutex<State>>);

struct State {
    /// `None` for a Weaver only this machine can reach, whose setup needs no
    /// code: being able to open it at all already proves the operator is here.
    verifier: Option<String>,
    failures: VecDeque<Instant>,
    consumed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SetupCodeError {
    Missing,
    Invalid,
    RateLimited,
    Consumed,
}

impl SetupChallenge {
    /// Generates a short setup code and returns it once. Only its verifier
    /// remains in the cloneable challenge state.
    pub(super) fn generate() -> (Self, String) {
        let code = generate_setup_code();
        let verifier = hex_hash(hash_api_key(&normalize_setup_code(&code)));
        (
            Self(Arc::new(Mutex::new(State {
                verifier: Some(verifier),
                failures: VecDeque::new(),
                consumed: false,
            }))),
            code,
        )
    }

    /// Setup without a code, for a Weaver that listens on loopback only.
    pub(super) fn open() -> Self {
        Self(Arc::new(Mutex::new(State {
            verifier: None,
            failures: VecDeque::new(),
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

    pub(super) fn verify(&self, code: Option<&str>) -> Result<(), SetupCodeError> {
        self.verify_at(code, Instant::now())
    }

    pub(super) fn is_available(&self) -> bool {
        !self
            .0
            .lock()
            .expect("setup challenge lock poisoned")
            .consumed
    }

    fn verify_at(&self, code: Option<&str>, now: Instant) -> Result<(), SetupCodeError> {
        let mut state = self.0.lock().expect("setup challenge lock poisoned");
        if state.consumed {
            return Err(SetupCodeError::Consumed);
        }
        let Some(verifier) = state.verifier.clone() else {
            return Ok(());
        };
        while state
            .failures
            .front()
            .is_some_and(|then| now.duration_since(*then) >= FAILURE_WINDOW)
        {
            state.failures.pop_front();
        }
        if state.failures.len() >= MAX_FAILURES {
            return Err(SetupCodeError::RateLimited);
        }
        // Accept the code however it was typed: any case, with or without
        // its hyphen.
        let Some(code) = code
            .map(normalize_setup_code)
            .filter(|code| !code.is_empty())
        else {
            state.failures.push_back(now);
            return Err(SetupCodeError::Missing);
        };
        if !verify_browser_csrf_token(&code, &verifier) {
            state.failures.push_back(now);
            return Err(SetupCodeError::Invalid);
        }
        Ok(())
    }

    /// Call only after the setup database transaction commits. Failed writes
    /// intentionally leave the challenge retryable.
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
        assert_eq!(challenge.verify(Some(&code)), Ok(()));
        assert_eq!(challenge.verify(Some(&code)), Ok(()));
        challenge.consume();
        assert!(!challenge.is_available());
        assert_eq!(challenge.verify(Some(&code)), Err(SetupCodeError::Consumed));
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
        assert_eq!(challenge.verify(None), Ok(()));
        assert_eq!(challenge.verify(Some("anything")), Ok(()));
        challenge.consume();
        assert_eq!(challenge.verify(None), Err(SetupCodeError::Consumed));
    }

    #[test]
    fn codes_are_short_and_accepted_in_any_case_with_or_without_the_hyphen() {
        let (challenge, code) = SetupChallenge::generate();
        assert!(weaver_server_core::auth::is_setup_code(&code), "{code}");
        assert_eq!(
            challenge.verify(Some(&format!(" {} ", code.to_ascii_lowercase()))),
            Ok(())
        );
        assert_eq!(challenge.verify(Some(&code.replace('-', ""))), Ok(()));
    }

    #[test]
    fn missing_and_wrong_codes_share_the_global_limit() {
        let (challenge, _) = SetupChallenge::generate();
        for _ in 0..3 {
            assert_eq!(challenge.verify(None), Err(SetupCodeError::Missing));
        }
        for _ in 0..2 {
            assert_eq!(
                challenge.verify(Some("wrong")),
                Err(SetupCodeError::Invalid)
            );
        }
        assert_eq!(
            challenge.verify(Some("wrong")),
            Err(SetupCodeError::RateLimited)
        );
    }

    #[test]
    fn failure_window_expires() {
        let (challenge, _) = SetupChallenge::generate();
        let now = Instant::now();
        for _ in 0..MAX_FAILURES {
            assert_eq!(
                challenge.verify_at(Some("wrong"), now),
                Err(SetupCodeError::Invalid)
            );
        }
        assert_eq!(
            challenge.verify_at(Some("wrong"), now),
            Err(SetupCodeError::RateLimited)
        );
        assert_eq!(
            challenge.verify_at(Some("wrong"), now + FAILURE_WINDOW),
            Err(SetupCodeError::Invalid)
        );
    }
}
