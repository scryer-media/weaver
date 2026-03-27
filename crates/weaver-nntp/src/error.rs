use std::fmt;

use thiserror::Error;

use crate::types::StatusCode;

/// Errors that can occur during NNTP operations.
#[derive(Debug, Error)]
pub enum NntpError {
    // --- Transport errors ---
    /// An I/O error occurred on the underlying transport.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A TLS error occurred during handshake or encrypted communication.
    #[error("TLS error: {0}")]
    Tls(#[from] tokio_rustls::rustls::Error),

    /// The operation timed out.
    #[error("connection timed out")]
    Timeout,

    /// The server closed the connection unexpectedly.
    #[error("connection closed by server")]
    ConnectionClosed,

    // --- Protocol errors ---
    /// The server returned an unexpected response code.
    #[error("unexpected response: {code} {message}")]
    UnexpectedResponse { code: StatusCode, message: String },

    /// The server sent a response that could not be parsed.
    #[error("malformed response: {0}")]
    MalformedResponse(String),

    // --- Authentication errors ---
    /// The server requires authentication (480).
    #[error("authentication required (480)")]
    AuthenticationRequired,

    /// Authentication failed — bad credentials (481).
    #[error("authentication failed (481)")]
    AuthenticationFailed,

    /// Authentication credentials were rejected (482).
    #[error("authentication rejected (482)")]
    AuthenticationRejected,

    // --- Article/group errors ---
    /// The requested article does not exist on this server (430).
    #[error("no such article: {message_id}")]
    NoSuchArticle { message_id: String },

    /// Article not found (430) without a specific message-id context.
    #[error("article not found on server (430)")]
    ArticleNotFound,

    /// No such newsgroup (411).
    #[error("no such newsgroup (411)")]
    NoSuchGroup,

    /// No newsgroup selected (412).
    #[error("no newsgroup selected (412)")]
    NoGroupSelected,

    /// No article with that number in the current group (423).
    #[error("no article with that number (423)")]
    NoArticleWithNumber,

    // --- Server errors ---
    /// Service temporarily unavailable (400).
    #[error("service temporarily unavailable (400)")]
    ServiceUnavailable,

    /// Command not recognized by the server (500).
    #[error("command not recognized (500)")]
    CommandNotRecognized,

    /// Too many connections to this server (502).
    /// Transient — back off and retry, do NOT disable the server.
    #[error("too many connections (502)")]
    TooManyConnections,

    /// Access denied — server refuses this client (502).
    /// Permanent — credentials or IP are rejected.
    #[error("access denied (502)")]
    AccessDenied,

    /// The server requires TLS before proceeding (483).
    #[error("TLS required (483)")]
    TlsRequired,

    // --- Pool errors (defined here for completeness; used later) ---
    /// No connections are available in the pool.
    #[error("no connections available")]
    PoolExhausted,

    /// The connection pool has been shut down.
    #[error("pool is shut down")]
    PoolShutdown,

    /// An article fetch exceeded the soft timeout, triggering failover to the next server.
    #[error("article fetch soft timeout ({0}s)")]
    SoftTimeout(u64),
}

/// Convenience type alias.
pub type Result<T> = std::result::Result<T, NntpError>;

impl NntpError {
    /// Create an `UnexpectedResponse` from a status code and message.
    pub fn unexpected(code: StatusCode, message: impl Into<String>) -> Self {
        NntpError::UnexpectedResponse {
            code,
            message: message.into(),
        }
    }

    /// Map common NNTP status codes to specific error variants.
    pub fn from_status(code: StatusCode, message: &str) -> Self {
        match code.raw() {
            400 => NntpError::ServiceUnavailable,
            411 => NntpError::NoSuchGroup,
            412 => NntpError::NoGroupSelected,
            423 => NntpError::NoArticleWithNumber,
            430 => NntpError::ArticleNotFound,
            480 => NntpError::AuthenticationRequired,
            481 => Self::classify_auth_error(message, NntpError::AuthenticationFailed),
            482 => Self::classify_auth_error(message, NntpError::AuthenticationRejected),
            483 => NntpError::TlsRequired,
            500 => NntpError::CommandNotRecognized,
            502 => Self::classify_502(message),
            _ => NntpError::unexpected(code, message),
        }
    }

    /// Check if a 481/482 response is actually a "too many connections" error.
    ///
    /// Some servers (Eweka, etc.) return 481 or 482 with messages like
    /// "too many connections for your user" instead of the standard 502.
    /// SABnzbd's `clues_too_many()` handles this pattern.
    fn classify_auth_error(message: &str, default: NntpError) -> Self {
        if Self::is_too_many_connections_message(message) {
            NntpError::TooManyConnections
        } else {
            default
        }
    }

    /// Distinguish "too many connections" (transient) from "access denied"
    /// (permanent) based on the 502 response message text.
    ///
    /// Real-world 502 messages from news providers:
    /// - "502 too many connections" (Newshosting, Eweka, etc.)
    /// - "502 Too Many Connections - Teknews"
    /// - "502 max connection limit reached"
    /// - "502 connection limit exceeded"
    /// - "502 Access Denied"
    /// - "502 You have no permission to talk"
    fn classify_502(message: &str) -> Self {
        if Self::is_too_many_connections_message(message) {
            NntpError::TooManyConnections
        } else {
            NntpError::AccessDenied
        }
    }

    /// Check if a response message indicates a connection-limit error.
    ///
    /// Used by both `classify_502` and `classify_auth_error` since servers
    /// return these messages on 481, 482, and 502 interchangeably.
    fn is_too_many_connections_message(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        lower.contains("too many")
            || lower.contains("connection limit")
            || lower.contains("max connect")
            || lower.contains("limit reached")
            || lower.contains("limit exceeded")
            || lower.contains("connections exceeded")
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_status_rfc3977_codes() {
        let sc = |raw| StatusCode::new(raw);

        assert!(matches!(
            NntpError::from_status(sc(400), ""),
            NntpError::ServiceUnavailable
        ));
        assert!(matches!(
            NntpError::from_status(sc(411), ""),
            NntpError::NoSuchGroup
        ));
        assert!(matches!(
            NntpError::from_status(sc(412), ""),
            NntpError::NoGroupSelected
        ));
        assert!(matches!(
            NntpError::from_status(sc(423), ""),
            NntpError::NoArticleWithNumber
        ));
        assert!(matches!(
            NntpError::from_status(sc(430), ""),
            NntpError::ArticleNotFound
        ));
        assert!(matches!(
            NntpError::from_status(sc(480), ""),
            NntpError::AuthenticationRequired
        ));
        assert!(matches!(
            NntpError::from_status(sc(481), "bad credentials"),
            NntpError::AuthenticationFailed
        ));
        assert!(matches!(
            NntpError::from_status(sc(482), "out of sequence"),
            NntpError::AuthenticationRejected
        ));
        assert!(matches!(
            NntpError::from_status(sc(483), ""),
            NntpError::TlsRequired
        ));
        assert!(matches!(
            NntpError::from_status(sc(500), ""),
            NntpError::CommandNotRecognized
        ));
    }

    #[test]
    fn classify_502_too_many_connections() {
        let sc = |raw| StatusCode::new(raw);

        // Common "too many connections" messages from real providers.
        assert!(matches!(
            NntpError::from_status(sc(502), "too many connections"),
            NntpError::TooManyConnections
        ));
        assert!(matches!(
            NntpError::from_status(sc(502), "Too Many Connections - Teknews"),
            NntpError::TooManyConnections
        ));
        assert!(matches!(
            NntpError::from_status(sc(502), "max connection limit reached"),
            NntpError::TooManyConnections
        ));
        assert!(matches!(
            NntpError::from_status(sc(502), "connection limit exceeded"),
            NntpError::TooManyConnections
        ));
        assert!(matches!(
            NntpError::from_status(sc(502), "connections exceeded for account"),
            NntpError::TooManyConnections
        ));
    }

    #[test]
    fn classify_502_access_denied() {
        let sc = |raw| StatusCode::new(raw);

        // Permanent access denial.
        assert!(matches!(
            NntpError::from_status(sc(502), "Access Denied"),
            NntpError::AccessDenied
        ));
        assert!(matches!(
            NntpError::from_status(sc(502), "You have no permission to talk"),
            NntpError::AccessDenied
        ));
        // Unknown 502 message defaults to AccessDenied (safe default).
        assert!(matches!(
            NntpError::from_status(sc(502), ""),
            NntpError::AccessDenied
        ));
    }

    #[test]
    fn classify_481_too_many_connections() {
        let sc = |raw| StatusCode::new(raw);

        // Some servers return 481 with "too many connections" text.
        assert!(matches!(
            NntpError::from_status(sc(481), "too many connections for your user"),
            NntpError::TooManyConnections
        ));
        assert!(matches!(
            NntpError::from_status(sc(481), "connection limit exceeded"),
            NntpError::TooManyConnections
        ));

        // Normal 481 auth failure.
        assert!(matches!(
            NntpError::from_status(sc(481), "Authentication failed"),
            NntpError::AuthenticationFailed
        ));
    }

    #[test]
    fn classify_482_too_many_connections() {
        let sc = |raw| StatusCode::new(raw);

        // Some servers return 482 for account sharing / connection limits.
        assert!(matches!(
            NntpError::from_status(sc(482), "Too many connections"),
            NntpError::TooManyConnections
        ));

        // Normal 482 auth rejection.
        assert!(matches!(
            NntpError::from_status(sc(482), "Authentication commands issued out of sequence"),
            NntpError::AuthenticationRejected
        ));
    }

    #[test]
    fn from_status_unknown_falls_through() {
        let err = NntpError::from_status(StatusCode::new(503), "test msg");
        assert!(matches!(err, NntpError::UnexpectedResponse { .. }));
    }
}
