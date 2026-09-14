use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use weaver_server_core::auth::service::verify_browser_csrf_token;
use weaver_server_core::auth::{generate_setup_code, hash_api_key};

const MAX_FAILURES: usize = 5;
const FAILURE_WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(super) struct SetupChallenge(Arc<Mutex<State>>);

struct State {
    verifier: String,
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
        let verifier = hex_hash(hash_api_key(&code));
        (
            Self(Arc::new(Mutex::new(State {
                verifier,
                failures: VecDeque::new(),
                consumed: false,
            }))),
            code,
        )
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
        // Codes are capitals; accept them however they were typed.
        let Some(code) = code
            .map(|code| code.trim().to_ascii_uppercase())
            .filter(|code| !code.is_empty())
        else {
            state.failures.push_back(now);
            return Err(SetupCodeError::Missing);
        };
        if !verify_browser_csrf_token(&code, &state.verifier) {
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
    fn codes_are_short_and_accepted_in_any_case() {
        let (challenge, code) = SetupChallenge::generate();
        assert!(weaver_server_core::auth::is_setup_code(&code), "{code}");
        assert_eq!(
            challenge.verify(Some(&format!(" {} ", code.to_ascii_lowercase()))),
            Ok(())
        );
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
