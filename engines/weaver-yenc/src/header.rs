use crate::error::YencError;
use crate::types::YencMetadata;

/// Parsed =yend trailer fields.
#[derive(Debug, Default)]
pub struct YendFields {
    pub size: Option<u64>,
    pub part: Option<u32>,
    pub pcrc32: Option<u32>,
    pub crc32: Option<u32>,
}

/// Result of parsing all yEnc headers from an article.
#[derive(Debug)]
pub struct ParsedHeaders {
    pub metadata: YencMetadata,
    pub data_start: usize,
    pub data_end: usize,
    pub yend: Option<YendFields>,
}

/// Find a line starting with the given prefix. Returns the byte offset of the
/// prefix within `input`, or `None`.
fn find_line_start(input: &[u8], prefix: &[u8]) -> Option<usize> {
    // Check if the input itself starts with the prefix.
    if input.starts_with(prefix) {
        return Some(0);
    }
    // Search for \n followed by the prefix (handles both \r\n and bare \n).
    let mut pos = 0;
    while pos < input.len() {
        if let Some(idx) = memchr_lf(&input[pos..]) {
            let abs = pos + idx + 1; // byte after \n
            if abs < input.len() && input[abs..].starts_with(prefix) {
                return Some(abs);
            }
            pos = abs;
        } else {
            break;
        }
    }
    None
}

/// Find first LF byte using SIMD-accelerated memchr.
fn memchr_lf(haystack: &[u8]) -> Option<usize> {
    memchr::memchr(b'\n', haystack)
}

/// Find the end of the current line (position of \r\n or \n).
/// Returns the index of the line terminator start, and the index after the full terminator.
fn line_end(input: &[u8], start: usize) -> (usize, usize) {
    if let Some(rel) = memchr::memchr(b'\n', &input[start..]) {
        let i = start + rel;
        if i > start && input[i - 1] == b'\r' {
            (i - 1, i + 1)
        } else {
            (i, i + 1)
        }
    } else {
        // No line terminator found; line extends to end of input.
        (input.len(), input.len())
    }
}

/// Convert bytes to a string, trying UTF-8 first, falling back to Latin-1.
fn bytes_to_string(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => {
            // Latin-1: each byte maps directly to its Unicode code point.
            bytes.iter().map(|&b| b as char).collect()
        }
    }
}

/// Parse key=value fields from a header line's content (after the keyword like `=ybegin `).
/// The `name` field is treated specially: it consumes everything from `name=` to end of line.
fn parse_fields(line: &str) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    let mut remaining = line;

    loop {
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            break;
        }

        // Check if this is the `name` field -- it must be last and consumes the rest.
        if let Some(value) = remaining.strip_prefix("name=") {
            // Trim trailing whitespace (e.g. trailing \r that wasn't stripped).
            let value = value.trim_end();
            fields.push(("name".to_string(), value.to_string()));
            break;
        }

        // Find the `=` separator for key=value.
        if let Some(eq_pos) = remaining.find('=') {
            let key = remaining[..eq_pos].trim();
            let after_eq = &remaining[eq_pos + 1..];

            // Value extends to the next space (or end of string).
            let value_end = after_eq.find(' ').unwrap_or(after_eq.len());
            let value = &after_eq[..value_end];

            fields.push((key.to_lowercase(), value.to_string()));
            remaining = &after_eq[value_end..];
        } else {
            // No more key=value pairs.
            break;
        }
    }

    fields
}

/// Parse a hex string (case-insensitive) into a u32 CRC value.
fn parse_crc_hex(s: &str) -> Result<u32, YencError> {
    u32::from_str_radix(s.trim(), 16).map_err(|_| YencError::InvalidHeader {
        field: "crc".to_string(),
        reason: format!("invalid hex value: {s}"),
    })
}

/// Parse all yEnc headers from an article body.
///
/// Returns parsed metadata, the byte range of encoded data, and =yend fields.
pub fn parse_headers(input: &[u8]) -> Result<ParsedHeaders, YencError> {
    // Find =ybegin line.
    let ybegin_start = find_line_start(input, b"=ybegin ").ok_or(YencError::MissingHeader)?;
    let (ybegin_line_end, after_ybegin) = line_end(input, ybegin_start);

    let ybegin_content = &input[ybegin_start + 8..ybegin_line_end];
    let ybegin_str = bytes_to_string(ybegin_content);

    let ybegin_fields = parse_fields(&ybegin_str);

    // Extract required fields from =ybegin.
    let line_length: u32 = get_field_u64(&ybegin_fields, "line")? as u32;
    let size: u64 = get_field_u64(&ybegin_fields, "size")?;
    let name = get_field_str(&ybegin_fields, "name")?;
    let part: Option<u32> = get_optional_field_u64(&ybegin_fields, "part")?.map(|v| v as u32);
    let total: Option<u32> = get_optional_field_u64(&ybegin_fields, "total")?.map(|v| v as u32);

    // If multi-part, parse =ypart.
    let (begin, end, data_start) = if part.is_some() {
        let ypart_start = find_line_start(&input[after_ybegin..], b"=ypart ")
            .map(|off| off + after_ybegin)
            .ok_or(YencError::MissingField("=ypart".to_string()))?;
        let (ypart_line_end, after_ypart) = line_end(input, ypart_start);

        let ypart_content = &input[ypart_start + 7..ypart_line_end];
        let ypart_str = bytes_to_string(ypart_content);

        let ypart_fields = parse_fields(&ypart_str);
        let begin = get_field_u64(&ypart_fields, "begin")?;
        let end = get_field_u64(&ypart_fields, "end")?;

        // Validate that the part range is sane.
        if end < begin {
            return Err(YencError::InvalidHeader {
                field: "end".to_string(),
                reason: format!("end ({end}) < begin ({begin})"),
            });
        }
        if end > size {
            return Err(YencError::InvalidHeader {
                field: "end".to_string(),
                reason: format!("end ({end}) > file size ({size})"),
            });
        }

        (Some(begin), Some(end), after_ypart)
    } else {
        (None, None, after_ybegin)
    };

    // Find =yend line.
    let yend = if let Some(yend_start) = find_line_start(&input[data_start..], b"=yend ") {
        let yend_abs = yend_start + data_start;
        let (yend_line_end, _) = line_end(input, yend_abs);

        let yend_content = &input[yend_abs + 6..yend_line_end];
        let yend_str = bytes_to_string(yend_content);

        let yend_fields = parse_fields(&yend_str);

        // Compute data_end: the byte just before the =yend line (before preceding \r\n or \n).
        let data_end = if yend_abs > 0 && input[yend_abs - 1] == b'\n' {
            if yend_abs >= 2 && input[yend_abs - 2] == b'\r' {
                yend_abs - 2
            } else {
                yend_abs - 1
            }
        } else {
            yend_abs
        };

        let yend_parsed = YendFields {
            size: get_optional_field_u64(&yend_fields, "size")?,
            part: get_optional_field_u64(&yend_fields, "part")?.map(|v| v as u32),
            pcrc32: get_optional_field_str(&yend_fields, "pcrc32")
                .map(|s| parse_crc_hex(&s))
                .transpose()?,
            crc32: get_optional_field_str(&yend_fields, "crc32")
                .map(|s| parse_crc_hex(&s))
                .transpose()?,
        };

        Some((yend_parsed, data_end))
    } else {
        None
    };

    let (yend_fields, data_end) = match yend {
        Some((fields, end)) => (Some(fields), end),
        None => (None, input.len()),
    };

    let metadata = YencMetadata {
        name,
        size,
        line_length,
        part,
        total,
        begin,
        end,
    };

    Ok(ParsedHeaders {
        metadata,
        data_start,
        data_end,
        yend: yend_fields,
    })
}

/// Extract a filename from a yEnc-style NNTP subject line.
///
/// yEnc subjects typically follow patterns like:
/// - `"filename.rar" yEnc (1/10)`
/// - `[group] "filename.rar" yEnc (1/10)`
/// - `some description - "filename.rar" yEnc (01/10)`
/// - `filename.rar yEnc (1/10)` (unquoted)
///
/// Returns `None` if no filename can be extracted.
pub fn extract_filename_from_subject(subject: &str) -> Option<String> {
    // Strategy 1: Look for a quoted filename before "yEnc"
    if let Some(yenc_pos) = subject.find("yEnc") {
        let before_yenc = subject[..yenc_pos].trim();

        // Try to find a quoted string
        if let Some(last_quote) = before_yenc.rfind('"')
            && let Some(first_quote) = before_yenc[..last_quote].rfind('"')
        {
            let filename = &before_yenc[first_quote + 1..last_quote];
            if !filename.is_empty() {
                return Some(filename.to_string());
            }
        }

        // Strategy 2: Unquoted - take the last whitespace-delimited token before "yEnc"
        if let Some(last_token) = before_yenc.split_whitespace().next_back() {
            // Only accept if it looks like a filename (contains a dot)
            if last_token.contains('.') {
                return Some(last_token.to_string());
            }
        }
    }

    None
}

fn get_field_str(fields: &[(String, String)], name: &str) -> Result<String, YencError> {
    fields
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.clone())
        .ok_or_else(|| YencError::MissingField(name.to_string()))
}

fn get_optional_field_str(fields: &[(String, String)], name: &str) -> Option<String> {
    fields
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.clone())
}

fn get_field_u64(fields: &[(String, String)], name: &str) -> Result<u64, YencError> {
    let value_str = get_field_str(fields, name)?;
    value_str
        .trim()
        .parse::<u64>()
        .map_err(|_| YencError::InvalidHeader {
            field: name.to_string(),
            reason: format!("invalid integer: {value_str}"),
        })
}

fn get_optional_field_u64(
    fields: &[(String, String)],
    name: &str,
) -> Result<Option<u64>, YencError> {
    match get_optional_field_str(fields, name) {
        Some(v) => {
            let parsed = v
                .trim()
                .parse::<u64>()
                .map_err(|_| YencError::InvalidHeader {
                    field: name.to_string(),
                    reason: format!("invalid integer: {v}"),
                })?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_part_article() {
        let input = b"=ybegin line=128 size=1234 name=testfile.bin\r\n\
                       some encoded data here\r\n\
                       =yend size=1234 crc32=ABCDEF12\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.name, "testfile.bin");
        assert_eq!(parsed.metadata.size, 1234);
        assert_eq!(parsed.metadata.line_length, 128);
        assert_eq!(parsed.metadata.part, None);
        assert_eq!(parsed.metadata.total, None);
        assert_eq!(parsed.metadata.begin, None);
        assert_eq!(parsed.metadata.end, None);

        let yend = parsed.yend.unwrap();
        assert_eq!(yend.size, Some(1234));
        assert_eq!(yend.crc32, Some(0xABCDEF12));
    }

    #[test]
    fn parse_multi_part_article() {
        let input = b"=ybegin part=1 total=10 line=128 size=500000 name=myfile.dat\r\n\
                       =ypart begin=1 end=50000\r\n\
                       encoded data\r\n\
                       =yend size=50000 part=1 pcrc32=abcdef12 crc32=12345678\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.name, "myfile.dat");
        assert_eq!(parsed.metadata.size, 500000);
        assert_eq!(parsed.metadata.part, Some(1));
        assert_eq!(parsed.metadata.total, Some(10));
        assert_eq!(parsed.metadata.begin, Some(1));
        assert_eq!(parsed.metadata.end, Some(50000));

        let yend = parsed.yend.unwrap();
        assert_eq!(yend.size, Some(50000));
        assert_eq!(yend.part, Some(1));
        assert_eq!(yend.pcrc32, Some(0xABCDEF12));
        assert_eq!(yend.crc32, Some(0x12345678));
    }

    #[test]
    fn filename_with_spaces() {
        let input = b"=ybegin line=128 size=100 name=my cool file (part=1).rar\r\n\
                       data\r\n\
                       =yend size=100\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.name, "my cool file (part=1).rar");
    }

    #[test]
    fn filename_with_equals() {
        let input = b"=ybegin line=128 size=100 name=file=name=test.bin\r\n\
                       data\r\n\
                       =yend size=100\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.name, "file=name=test.bin");
    }

    #[test]
    fn case_insensitive_crc_hex() {
        let input = b"=ybegin line=128 size=100 name=test.bin\r\n\
                       data\r\n\
                       =yend size=100 crc32=aBcDeF01\r\n";
        let parsed = parse_headers(input).unwrap();
        let yend = parsed.yend.unwrap();
        assert_eq!(yend.crc32, Some(0xABCDEF01));
    }

    #[test]
    fn bare_lf_line_endings() {
        let input = b"=ybegin line=128 size=100 name=test.bin\n\
                       data\n\
                       =yend size=100 crc32=12345678\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.name, "test.bin");
        let yend = parsed.yend.unwrap();
        assert_eq!(yend.crc32, Some(0x12345678));
    }

    #[test]
    fn missing_ybegin_header() {
        let input = b"some random data\r\nno headers here\r\n";
        let result = parse_headers(input);
        assert!(matches!(result, Err(YencError::MissingHeader)));
    }

    #[test]
    fn missing_yend_trailer() {
        let input = b"=ybegin line=128 size=100 name=test.bin\r\nsome data\r\n";
        let parsed = parse_headers(input).unwrap();
        assert!(parsed.yend.is_none());
        // data_end should be at end of input
        assert_eq!(parsed.data_end, input.len());
    }

    #[test]
    fn missing_required_field() {
        // Missing `size` field.
        let input = b"=ybegin line=128 name=test.bin\r\ndata\r\n=yend size=100\r\n";
        let result = parse_headers(input);
        assert!(matches!(result, Err(YencError::MissingField(_))));
    }

    #[test]
    fn optional_total_missing() {
        let input = b"=ybegin part=1 line=128 size=500000 name=myfile.dat\r\n\
                       =ypart begin=1 end=50000\r\n\
                       data\r\n\
                       =yend size=50000\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.part, Some(1));
        assert_eq!(parsed.metadata.total, None);
    }

    #[test]
    fn data_range_is_correct() {
        let header = b"=ybegin line=128 size=100 name=test.bin\r\n";
        let data = b"encoded data here\r\n";
        let trailer = b"=yend size=100 crc32=12345678\r\n";

        let mut input = Vec::new();
        input.extend_from_slice(header);
        input.extend_from_slice(data);
        input.extend_from_slice(trailer);

        let parsed = parse_headers(&input).unwrap();
        assert_eq!(parsed.data_start, header.len());
        // data_end should be before the \r\n preceding =yend
        let data_section = &input[parsed.data_start..parsed.data_end];
        assert_eq!(data_section, b"encoded data here");
    }

    #[test]
    fn ybegin_not_at_start_of_input() {
        // Sometimes there's junk before the =ybegin line.
        let input = b"some header junk\r\n=ybegin line=128 size=100 name=test.bin\r\n\
                       data\r\n\
                       =yend size=100\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.name, "test.bin");
    }

    #[test]
    fn parse_fields_handles_tabs_and_extra_spaces() {
        let fields = parse_fields("  line=128  size=100  name=test file.bin");
        assert!(fields.iter().any(|(k, v)| k == "line" && v == "128"));
        assert!(fields.iter().any(|(k, v)| k == "size" && v == "100"));
        assert!(
            fields
                .iter()
                .any(|(k, v)| k == "name" && v == "test file.bin")
        );
    }

    #[test]
    fn short_crc_hex() {
        // Some encoders omit leading zeros.
        let result = parse_crc_hex("1a2b");
        assert_eq!(result.unwrap(), 0x1A2B);
    }

    #[test]
    fn crc_hex_full_width() {
        let result = parse_crc_hex("DEADBEEF");
        assert_eq!(result.unwrap(), 0xDEADBEEF);
    }

    #[test]
    fn invalid_crc_hex() {
        let result = parse_crc_hex("GGGG");
        assert!(result.is_err());
    }

    #[test]
    fn missing_ypart_for_multipart() {
        let input = b"=ybegin part=1 line=128 size=500000 name=myfile.dat\r\n\
                       data here\r\n\
                       =yend size=50000\r\n";
        let result = parse_headers(input);
        assert!(matches!(result, Err(YencError::MissingField(_))));
    }

    #[test]
    fn ypart_end_less_than_begin() {
        let input = b"=ybegin part=1 line=128 size=500000 name=myfile.dat\r\n\
                       =ypart begin=1000 end=500\r\n\
                       data\r\n\
                       =yend size=500\r\n";
        let result = parse_headers(input);
        assert!(matches!(result, Err(YencError::InvalidHeader { .. })));
    }

    #[test]
    fn ypart_end_exceeds_file_size() {
        let input = b"=ybegin part=1 line=128 size=1000 name=myfile.dat\r\n\
                       =ypart begin=1 end=2000\r\n\
                       data\r\n\
                       =yend size=2000\r\n";
        let result = parse_headers(input);
        assert!(matches!(result, Err(YencError::InvalidHeader { .. })));
    }

    #[test]
    fn ypart_valid_range() {
        let input = b"=ybegin part=1 line=128 size=500000 name=myfile.dat\r\n\
                       =ypart begin=1 end=50000\r\n\
                       data\r\n\
                       =yend size=50000\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.begin, Some(1));
        assert_eq!(parsed.metadata.end, Some(50000));
    }

    // Latin-1 tests

    #[test]
    fn parse_latin1_filename() {
        // Filename with German umlaut (ü = 0xFC in Latin-1, not valid UTF-8 as a standalone byte)
        let mut input = Vec::new();
        input.extend_from_slice(b"=ybegin line=128 size=100 name=");
        input.push(0xFC); // ü in Latin-1
        input.extend_from_slice(b"ber.bin\r\ndata\r\n=yend size=100\r\n");

        let parsed = parse_headers(&input).unwrap();
        assert_eq!(parsed.metadata.name, "\u{00FC}ber.bin");
    }

    #[test]
    fn parse_utf8_still_works() {
        // Normal UTF-8 filename should still work.
        let input = b"=ybegin line=128 size=100 name=normal.bin\r\ndata\r\n=yend size=100\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.name, "normal.bin");
    }

    // Subject extraction tests

    #[test]
    fn extract_filename_quoted() {
        let subject = r#"[alt.binaries] "myfile.rar" yEnc (1/10)"#;
        assert_eq!(
            extract_filename_from_subject(subject),
            Some("myfile.rar".to_string())
        );
    }

    #[test]
    fn extract_filename_quoted_with_spaces() {
        let subject = r#"some desc - "my cool file.nfo" yEnc (01/01)"#;
        assert_eq!(
            extract_filename_from_subject(subject),
            Some("my cool file.nfo".to_string())
        );
    }

    #[test]
    fn extract_filename_unquoted() {
        let subject = "myfile.rar yEnc (1/10)";
        assert_eq!(
            extract_filename_from_subject(subject),
            Some("myfile.rar".to_string())
        );
    }

    #[test]
    fn extract_filename_no_yenc_marker() {
        let subject = "just a normal subject line";
        assert_eq!(extract_filename_from_subject(subject), None);
    }

    #[test]
    fn extract_filename_empty_quotes() {
        let subject = r#""" yEnc (1/1)"#;
        // Empty quoted string should not match
        assert_eq!(extract_filename_from_subject(subject), None);
    }

    #[test]
    fn extract_filename_no_filename_before_yenc() {
        let subject = "yEnc (1/1)";
        assert_eq!(extract_filename_from_subject(subject), None);
    }
}
