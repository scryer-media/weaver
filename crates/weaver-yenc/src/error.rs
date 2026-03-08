use thiserror::Error;

/// Errors that can occur during yEnc decoding or encoding.
#[derive(Debug, Error)]
pub enum YencError {
    #[error("missing =ybegin header")]
    MissingHeader,

    #[error("missing =yend trailer")]
    MissingTrailer,

    #[error("invalid header field `{field}`: {reason}")]
    InvalidHeader { field: String, reason: String },

    #[error("missing required header field `{0}`")]
    MissingField(String),

    #[error("CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("size mismatch: header says {expected}, decoded {actual} bytes")]
    SizeMismatch { expected: u64, actual: u64 },

    #[error("output buffer too small: need {needed}, have {available}")]
    BufferTooSmall { needed: usize, available: usize },

    #[error("malformed escape sequence at byte offset {0}")]
    MalformedEscape(usize),

    #[error("unexpected end of data")]
    UnexpectedEof,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_messages() {
        let err = YencError::MissingHeader;
        assert_eq!(err.to_string(), "missing =ybegin header");

        let err = YencError::CrcMismatch {
            expected: 0xDEADBEEF,
            actual: 0x12345678,
        };
        assert!(err.to_string().contains("0xdeadbeef"));
        assert!(err.to_string().contains("0x12345678"));

        let err = YencError::InvalidHeader {
            field: "size".to_string(),
            reason: "not a number".to_string(),
        };
        assert!(err.to_string().contains("size"));
        assert!(err.to_string().contains("not a number"));

        let err = YencError::BufferTooSmall {
            needed: 1024,
            available: 512,
        };
        assert!(err.to_string().contains("1024"));
        assert!(err.to_string().contains("512"));
    }

    #[test]
    fn error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<YencError>();
    }
}
