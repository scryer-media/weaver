use bytes::Bytes;
use std::collections::HashMap;

/// A raw three-digit NNTP status code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatusCode(u16);

impl StatusCode {
    /// Create a new `StatusCode` from a raw `u16`.
    ///
    /// # Panics
    /// Panics if the value is not in the range 100..=599.
    pub fn new(code: u16) -> Self {
        debug_assert!(
            (100..=599).contains(&code),
            "invalid NNTP status code: {code}"
        );
        StatusCode(code)
    }

    /// Try to create a `StatusCode`, returning `None` if out of range.
    pub fn from_u16(code: u16) -> Option<Self> {
        if (100..=599).contains(&code) {
            Some(StatusCode(code))
        } else {
            None
        }
    }

    /// The raw numeric value.
    pub fn raw(&self) -> u16 {
        self.0
    }

    /// The response kind (first digit).
    pub fn kind(&self) -> ResponseKind {
        match self.0 / 100 {
            1 => ResponseKind::Informational,
            2 => ResponseKind::Success,
            3 => ResponseKind::Continue,
            4 => ResponseKind::TransientError,
            5 => ResponseKind::PermanentError,
            _ => unreachable!(),
        }
    }

    /// True if this is a 2xx success code.
    pub fn is_success(&self) -> bool {
        matches!(self.kind(), ResponseKind::Success)
    }

    /// True if this is a 3xx continue code.
    pub fn is_continue(&self) -> bool {
        matches!(self.kind(), ResponseKind::Continue)
    }

    /// True if this is a 4xx or 5xx error code.
    pub fn is_error(&self) -> bool {
        matches!(
            self.kind(),
            ResponseKind::TransientError | ResponseKind::PermanentError
        )
    }
}

/// Categorization of NNTP response codes by first digit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseKind {
    /// 1xx - Informational
    Informational,
    /// 2xx - Command completed OK
    Success,
    /// 3xx - Command OK so far, send more
    Continue,
    /// 4xx - Command failed (transient)
    TransientError,
    /// 5xx - Command failed (permanent)
    PermanentError,
}

/// A single-line NNTP response (status code + message text).
#[derive(Debug, Clone)]
pub struct Response {
    pub code: StatusCode,
    pub message: String,
}

impl Response {
    /// True if this response indicates success (2xx).
    pub fn is_success(&self) -> bool {
        self.code.is_success()
    }

    /// True if this response indicates an error (4xx/5xx).
    pub fn is_error(&self) -> bool {
        self.code.is_error()
    }
}

/// A multi-line NNTP response: initial status line plus the data body.
#[derive(Debug, Clone)]
pub struct MultiLineResponse {
    /// The initial response line (e.g. "222 0 <msgid> body follows").
    pub initial: Response,
    /// The data portion, already dot-unstuffed, without the terminating ".\r\n".
    pub data: Bytes,
}

/// Identifies an article by either message-id or article number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArticleId {
    /// A message-id including angle brackets, e.g. `<abc123@example.com>`.
    MessageId(String),
    /// An article number within the current group.
    Number(u64),
}

impl ArticleId {
    /// Create a `MessageId` variant with validation.
    ///
    /// Returns `None` if the message-id is empty or missing angle brackets.
    pub fn message_id(id: impl Into<String>) -> Option<Self> {
        let id = id.into();
        if id.len() >= 3 && id.starts_with('<') && id.ends_with('>') {
            Some(ArticleId::MessageId(id))
        } else {
            None
        }
    }
}

impl std::fmt::Display for ArticleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArticleId::MessageId(id) => write!(f, "{id}"),
            ArticleId::Number(n) => write!(f, "{n}"),
        }
    }
}

/// Parsed CAPABILITIES response from the server.
#[derive(Debug, Clone, Default)]
pub struct Capabilities {
    /// Map from capability keyword to its arguments (if any).
    raw: HashMap<String, Vec<String>>,
}

impl Capabilities {
    /// Parse capabilities from the multi-line data body.
    ///
    /// Each line is `KEYWORD [arg1 arg2 ...]`.
    /// The first line (version) is included as-is.
    pub fn parse(data: &[u8]) -> Self {
        let mut raw = HashMap::new();
        let text = String::from_utf8_lossy(data);
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(2, ' ');
            let keyword = parts.next().unwrap_or("").to_uppercase();
            let args: Vec<String> = parts
                .next()
                .map(|rest| rest.split_whitespace().map(String::from).collect())
                .unwrap_or_default();
            raw.insert(keyword, args);
        }
        Capabilities { raw }
    }

    /// Check whether a capability is advertised.
    pub fn has(&self, name: &str) -> bool {
        self.raw.contains_key(&name.to_uppercase())
    }

    /// Get the arguments for a capability, if present.
    pub fn get(&self, name: &str) -> Option<&[String]> {
        self.raw.get(&name.to_uppercase()).map(|v| v.as_slice())
    }

    /// Whether the server supports STARTTLS.
    pub fn supports_starttls(&self) -> bool {
        self.has("STARTTLS")
    }

    /// Whether the server supports COMPRESS (RFC 8054).
    pub fn supports_compress(&self) -> bool {
        self.has("COMPRESS")
    }

    /// Whether the server supports AUTHINFO.
    pub fn supports_authinfo(&self) -> bool {
        self.has("AUTHINFO")
    }

    /// Whether MODE READER is required.
    pub fn mode_reader_required(&self) -> bool {
        self.has("MODE-READER")
    }

    /// Whether the server supports command pipelining (RFC 4644).
    pub fn supports_pipelining(&self) -> bool {
        self.has("PIPELINING")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_code_kind() {
        assert_eq!(StatusCode::new(100).kind(), ResponseKind::Informational);
        assert_eq!(StatusCode::new(200).kind(), ResponseKind::Success);
        assert_eq!(StatusCode::new(281).kind(), ResponseKind::Success);
        assert_eq!(StatusCode::new(381).kind(), ResponseKind::Continue);
        assert_eq!(StatusCode::new(430).kind(), ResponseKind::TransientError);
        assert_eq!(StatusCode::new(500).kind(), ResponseKind::PermanentError);
    }

    #[test]
    fn status_code_predicates() {
        let ok = StatusCode::new(200);
        assert!(ok.is_success());
        assert!(!ok.is_error());
        assert!(!ok.is_continue());

        let err = StatusCode::new(481);
        assert!(err.is_error());
        assert!(!err.is_success());
    }

    #[test]
    fn status_code_from_u16_range() {
        assert!(StatusCode::from_u16(99).is_none());
        assert!(StatusCode::from_u16(100).is_some());
        assert!(StatusCode::from_u16(599).is_some());
        assert!(StatusCode::from_u16(600).is_none());
    }

    #[test]
    fn capabilities_parse_basic() {
        let data = b"VERSION 2\r\nREADER\r\nSTARTTLS\r\nAUTHINFO USER PASS\r\nCOMPRESS DEFLATE\r\n";
        let caps = Capabilities::parse(data);
        assert!(caps.has("VERSION"));
        assert!(caps.has("READER"));
        assert!(caps.supports_starttls());
        assert!(caps.supports_authinfo());
        assert!(caps.supports_compress());
        assert_eq!(
            caps.get("AUTHINFO"),
            Some(["USER".into(), "PASS".into()].as_slice())
        );
        assert_eq!(caps.get("COMPRESS"), Some(["DEFLATE".into()].as_slice()));
    }

    #[test]
    fn capabilities_case_insensitive() {
        let data = b"starttls\r\nauthinfo user\r\n";
        let caps = Capabilities::parse(data);
        assert!(caps.supports_starttls());
        assert!(caps.supports_authinfo());
    }

    #[test]
    fn capabilities_empty() {
        let caps = Capabilities::parse(b"");
        assert!(!caps.supports_starttls());
        assert!(!caps.has("ANYTHING"));
    }

    #[test]
    fn article_id_display() {
        let mid = ArticleId::MessageId("<abc@example.com>".into());
        assert_eq!(mid.to_string(), "<abc@example.com>");

        let num = ArticleId::Number(12345);
        assert_eq!(num.to_string(), "12345");
    }

    #[test]
    fn article_id_message_id_validation() {
        // Valid message-ids
        assert!(ArticleId::message_id("<abc@example.com>").is_some());
        assert!(ArticleId::message_id("<a>").is_some());

        // Invalid: empty
        assert!(ArticleId::message_id("").is_none());
        // Invalid: no brackets
        assert!(ArticleId::message_id("abc@example.com").is_none());
        // Invalid: missing closing bracket
        assert!(ArticleId::message_id("<abc@example.com").is_none());
        // Invalid: missing opening bracket
        assert!(ArticleId::message_id("abc@example.com>").is_none());
        // Invalid: just brackets (too short, no content)
        assert!(ArticleId::message_id("<>").is_none());
    }
}
