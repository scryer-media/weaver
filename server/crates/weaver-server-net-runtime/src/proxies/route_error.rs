// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Why a route or a draft of one could not be built, by kind. Each kind
//! words itself as the plain sentence it replaced, so a caller that reports
//! a string reports the same text.

use super::NetworkRefusal;
use super::runtime::ROUTES_CHANGING;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteError {
    // A route the operator may not save, refused by its own code.
    Refused(NetworkRefusal),
    // An egress, proxy or pool the route names that is gone.
    Missing(&'static str),
    // The consumer the route is for is gone.
    ConsumerGone,
    // A proxy whose saved settings cannot make the hop the route asks for.
    Invalid(String),
    // The routes are mid-reload.
    Changing,
    // The proxy runtime has stopped.
    Stopped,
    // The runtime holds as many live routes as it allows.
    Full,
    // A fault inside Weaver.
    Fault(String),
}

impl std::fmt::Display for RouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refusal) => write!(f, "{refusal}"),
            Self::Missing(what) => f.write_str(what),
            Self::ConsumerGone => f.write_str("routing consumer no longer exists"),
            Self::Invalid(message) | Self::Fault(message) => f.write_str(message),
            Self::Changing => f.write_str(ROUTES_CHANGING),
            Self::Stopped => f.write_str("proxy runtime is stopped"),
            Self::Full => f.write_str("too many active consumer routes"),
        }
    }
}

impl std::error::Error for RouteError {}

impl From<NetworkRefusal> for RouteError {
    fn from(refusal: NetworkRefusal) -> Self {
        Self::Refused(refusal)
    }
}

// Callers that report a plain string keep `?` working, with the same text.
impl From<RouteError> for String {
    fn from(error: RouteError) -> Self {
        error.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_kind_words_itself_as_the_sentence_it_replaced() {
        let refusal = NetworkRefusal::PoolGone;
        let cases = [
            (RouteError::Refused(refusal.clone()), refusal.to_string()),
            (
                RouteError::Missing("proxy pool does not exist"),
                "proxy pool does not exist".to_string(),
            ),
            (
                RouteError::ConsumerGone,
                "routing consumer no longer exists".to_string(),
            ),
            (
                RouteError::Invalid("HTTP/3 must be the first hop on an egress".into()),
                "HTTP/3 must be the first hop on an egress".to_string(),
            ),
            (RouteError::Changing, ROUTES_CHANGING.to_string()),
            (RouteError::Stopped, "proxy runtime is stopped".to_string()),
            (
                RouteError::Full,
                "too many active consumer routes".to_string(),
            ),
            (RouteError::Fault("boom".into()), "boom".to_string()),
        ];
        for (error, sentence) in cases {
            assert_eq!(error.to_string(), sentence);
            assert_eq!(String::from(error), sentence);
        }
    }
}
