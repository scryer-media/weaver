use bytes::Bytes;

use crate::error::NntpError;
use crate::types::{MultiLineResponse, Response, StatusCode};

/// Parse a single-line NNTP response string into a `Response`.
///
/// Expected format: `xyz message text` where `xyz` is a 3-digit status code
/// followed by a space (or end of line) and optional message text.
pub fn parse_response(line: &str) -> Result<Response, NntpError> {
    if line.len() < 3 {
        return Err(NntpError::MalformedResponse(format!(
            "response too short: {line:?}"
        )));
    }

    let code_str = &line[..3];
    let code_val: u16 = code_str.parse().map_err(|_| {
        NntpError::MalformedResponse(format!("invalid status code: {code_str:?}"))
    })?;

    let code = StatusCode::from_u16(code_val).ok_or_else(|| {
        NntpError::MalformedResponse(format!("status code out of range: {code_val}"))
    })?;

    let message = if line.len() > 4 { &line[4..] } else { "" };

    Ok(Response {
        code,
        message: message.to_string(),
    })
}

/// Build a `MultiLineResponse` from a parsed initial `Response` and the raw
/// multi-line data body (already dot-unstuffed by the codec).
pub fn build_multiline_response(initial: Response, data: Bytes) -> MultiLineResponse {
    MultiLineResponse { initial, data }
}

/// Check a response and return an appropriate error if it indicates failure.
///
/// Returns `Ok(response)` if the status code is not an error code. Otherwise
/// maps known error codes to specific `NntpError` variants.
pub fn check_response(response: Response) -> Result<Response, NntpError> {
    if !response.code.is_error() {
        return Ok(response);
    }
    Err(NntpError::from_status(response.code, &response.message))
}

/// Returns `true` if the given status code indicates a multi-line data block follows.
///
/// Based on RFC 3977 and common extensions.
pub fn is_multiline_status(code: u16) -> bool {
    matches!(
        code,
        101 |  // CAPABILITIES
        211 |  // GROUP (list form)
        215 |  // LIST
        220 |  // ARTICLE
        221 |  // HEAD
        222 |  // BODY
        224 |  // OVER / XOVER
        225 |  // HDR / XHDR
        230 |  // NEWNEWS
        231    // NEWGROUPS
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_response() {
        let resp = parse_response("200 NNTP Service Ready").unwrap();
        assert_eq!(resp.code.raw(), 200);
        assert_eq!(resp.message, "NNTP Service Ready");
        assert!(resp.is_success());
    }

    #[test]
    fn parse_response_no_message() {
        let resp = parse_response("200").unwrap();
        assert_eq!(resp.code.raw(), 200);
        assert_eq!(resp.message, "");
    }

    #[test]
    fn parse_response_with_space_no_message() {
        let resp = parse_response("200 ").unwrap();
        assert_eq!(resp.code.raw(), 200);
        assert_eq!(resp.message, "");
    }

    #[test]
    fn parse_auth_continue() {
        let resp = parse_response("381 Enter passphrase").unwrap();
        assert_eq!(resp.code.raw(), 381);
        assert!(resp.code.is_continue());
    }

    #[test]
    fn parse_error_response() {
        let resp = parse_response("430 No Such Article").unwrap();
        assert_eq!(resp.code.raw(), 430);
        assert!(resp.is_error());
    }

    #[test]
    fn parse_too_short() {
        assert!(parse_response("20").is_err());
        assert!(parse_response("").is_err());
    }

    #[test]
    fn parse_non_numeric() {
        assert!(parse_response("abc hello").is_err());
    }

    #[test]
    fn parse_out_of_range() {
        assert!(parse_response("999 out of range").is_err());
        assert!(parse_response("000 too low").is_err());
    }

    #[test]
    fn check_success_passes_through() {
        let resp = Response {
            code: StatusCode::new(200),
            message: "OK".into(),
        };
        let result = check_response(resp);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().code.raw(), 200);
    }

    #[test]
    fn check_error_maps_known_codes() {
        let resp = Response {
            code: StatusCode::new(480),
            message: "Authentication required".into(),
        };
        let err = check_response(resp).unwrap_err();
        assert!(matches!(err, NntpError::AuthenticationRequired));
    }

    #[test]
    fn check_error_maps_article_not_found() {
        let resp = Response {
            code: StatusCode::new(430),
            message: "No Such Article".into(),
        };
        let err = check_response(resp).unwrap_err();
        assert!(matches!(err, NntpError::ArticleNotFound));
    }

    #[test]
    fn check_error_unknown_code() {
        let resp = Response {
            code: StatusCode::new(503),
            message: "Internal Fault".into(),
        };
        let err = check_response(resp).unwrap_err();
        assert!(matches!(err, NntpError::UnexpectedResponse { .. }));
    }

    #[test]
    fn multiline_status_codes() {
        assert!(is_multiline_status(101));
        assert!(is_multiline_status(220));
        assert!(is_multiline_status(221));
        assert!(is_multiline_status(222));
        assert!(!is_multiline_status(200));
        assert!(!is_multiline_status(281));
        assert!(!is_multiline_status(430));
    }

    #[test]
    fn build_multiline() {
        let initial = Response {
            code: StatusCode::new(222),
            message: "body follows".into(),
        };
        let data = Bytes::from("some body data\r\n");
        let mlr = build_multiline_response(initial, data.clone());
        assert_eq!(mlr.initial.code.raw(), 222);
        assert_eq!(mlr.data, data);
    }
}
