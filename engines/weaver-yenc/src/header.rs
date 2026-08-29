use crate::decode::DecodeOptions;
use crate::error::YencError;
use crate::types::{YencHeaderDefects, YencMetadata};

/// Parsed =yend trailer fields.
#[derive(Debug, Default)]
pub struct YendFields {
    pub size: Option<u64>,
    pub part: Option<u32>,
    pub pcrc32: Option<u32>,
    pub crc32: Option<u32>,
    /// Trailer damage that was tolerated (unparseable `size=`/`crc32=`).
    pub defects: YencHeaderDefects,
}

/// Match `keyword` at the start of `line`, requiring a space or tab separator
/// immediately after it.
///
/// Both reference decoders detect control lines with a literal, case-sensitive
/// prefix that *includes* the trailing space (sabctools `starts_with(line,
/// "=ybegin ")`, nzbget `strncmp(buffer, "=ybegin ", 8)`), so a bare `=ybegin`
/// with no separator is deliberately not a header there and is not one here
/// either. The separator requirement is also what keeps junk lines that merely
/// share a prefix (`=yb`, `=ybeginner notes`) from being mistaken for headers.
///
/// Weaver additionally accepts a tab separator, where both references accept
/// only a space. That is a strict superset: it decodes articles they would
/// treat as non-binary and cannot change how any article they accept is parsed.
///
/// Returns the field content after the keyword, line ending trimmed.
fn strip_keyword<'a>(line: &'a [u8], keyword: &[u8]) -> Option<&'a [u8]> {
    let rest = line.strip_prefix(keyword)?;
    match rest.first() {
        Some(b' ' | b'\t') => Some(trim_line_end(rest)),
        _ => None,
    }
}

/// True when `line` begins a yEnc control line for `keyword` (`=ybegin`,
/// `=ypart`, `=yend`).
pub fn is_control_line(line: &[u8], keyword: &[u8]) -> bool {
    strip_keyword(line, keyword).is_some()
}

pub fn parse_ybegin_line(line: &[u8]) -> Result<YencMetadata, YencError> {
    let content = strip_keyword(line, b"=ybegin").ok_or_else(|| YencError::InvalidHeader {
        field: "=ybegin".to_string(),
        reason: "missing =ybegin prefix".to_string(),
    })?;
    let mut fields = YbeginFieldRefs::default();
    visit_fields(content, |key, value| {
        if key_eq_ascii_ignore_case(key, b"name") {
            fields.name = Some(value);
        } else if key_eq_ascii_ignore_case(key, b"size") {
            fields.size = Some(value);
        } else if key_eq_ascii_ignore_case(key, b"line") {
            fields.line = Some(value);
        } else if key_eq_ascii_ignore_case(key, b"part") {
            fields.part = Some(value);
        } else if key_eq_ascii_ignore_case(key, b"total") {
            fields.total = Some(value);
        }
    });

    // Reference decoders do not require any =ybegin field: sabctools reads
    // `part`/`begin`/`end` and hands the rest to Python, and SABnzbd falls back
    // to the NZB/subject when `name=` or `size=` are missing. Missing or
    // unparseable fields therefore degrade to a neutral value and are recorded
    // in `defects` rather than failing the article.
    let mut defects = YencHeaderDefects::default();
    let (size, size_missing, size_invalid) = tolerant_u64(fields.size);
    defects.missing_size = size_missing;
    defects.invalid_size = size_invalid;
    let (line_length, line_missing, line_invalid) = tolerant_u64(fields.line);
    defects.missing_line = line_missing;
    defects.invalid_line = line_invalid;
    let name = match fields.name {
        Some(value) => bytes_to_string(value),
        None => {
            defects.missing_name = true;
            String::new()
        }
    };

    Ok(YencMetadata {
        name,
        size: size.unwrap_or(0),
        line_length: u32::try_from(line_length.unwrap_or(0)).unwrap_or(u32::MAX),
        // `part`/`total` steer control flow (multi-part needs =ypart), so an
        // unparseable value is treated as absent rather than guessed at.
        part: tolerant_u64(fields.part).0.map(saturating_u32),
        total: tolerant_u64(fields.total).0.map(saturating_u32),
        begin: None,
        end: None,
        defects,
    })
}

pub fn apply_ypart_line(line: &[u8], metadata: &mut YencMetadata) -> Result<(), YencError> {
    let content = strip_keyword(line, b"=ypart").ok_or_else(|| YencError::InvalidHeader {
        field: "=ypart".to_string(),
        reason: "missing =ypart prefix".to_string(),
    })?;
    let mut fields = YpartFieldRefs::default();
    visit_fields(content, |key, value| {
        if key_eq_ascii_ignore_case(key, b"begin") {
            fields.begin = Some(value);
        } else if key_eq_ascii_ignore_case(key, b"end") {
            fields.end = Some(value);
        }
    });
    let begin = required_u64_field(fields.begin, "begin")?;
    let end = required_u64_field(fields.end, "end")?;

    // `end < begin` would make the part length negative; there is no sane
    // recovery, so it stays a hard error (and keeps the length arithmetic from
    // ever underflowing).
    if end < begin {
        return Err(YencError::InvalidHeader {
            field: "end".to_string(),
            reason: format!("end ({end}) < begin ({begin})"),
        });
    }
    // `end > size` is a broken-poster class the ecosystem shrugs off: neither
    // sabctools nor nzbget cross-checks =ypart against =ybegin size=. The part
    // length (end - begin + 1) is still authoritative and still verified
    // against the decoded byte count, so record the inconsistency and continue.
    if end > metadata.size {
        metadata.defects.ypart_end_exceeds_size = true;
    }

    metadata.begin = Some(begin);
    metadata.end = Some(end);
    Ok(())
}

pub fn parse_yend_line(line: &[u8]) -> Result<YendFields, YencError> {
    let content = strip_keyword(line, b"=yend").ok_or_else(|| YencError::InvalidHeader {
        field: "=yend".to_string(),
        reason: "missing =yend prefix".to_string(),
    })?;
    let mut fields = YendFieldRefs::default();
    visit_fields(content, |key, value| {
        if key_eq_ascii_ignore_case(key, b"size") {
            fields.size = Some(value);
        } else if key_eq_ascii_ignore_case(key, b"part") {
            fields.part = Some(value);
        } else if key_eq_ascii_ignore_case(key, b"pcrc32") {
            fields.pcrc32 = Some(value);
        } else if key_eq_ascii_ignore_case(key, b"crc32") {
            fields.crc32 = Some(value);
        }
    });

    let mut defects = YencHeaderDefects::default();
    let (size, _, size_invalid) = tolerant_u64(fields.size);
    defects.invalid_yend_size = size_invalid;
    // A CRC that cannot be parsed carries no information, so it is dropped
    // rather than allowed to fail an otherwise complete article: this matches
    // strtoul-based reference parsers, which never reject on a bad CRC field.
    // Dropping it leaves the article *unverified*, which `CrcVerification`
    // reports distinctly from *verified*.
    let pcrc32 = fields
        .pcrc32
        .and_then(|value| parse_crc_hex_bytes(value).inspect_none(&mut defects.invalid_pcrc32));
    let crc32 = fields
        .crc32
        .and_then(|value| parse_crc_hex_bytes(value).inspect_none(&mut defects.invalid_crc32));

    Ok(YendFields {
        size,
        part: tolerant_u64(fields.part).0.map(saturating_u32),
        pcrc32,
        crc32,
        defects,
    })
}

/// Small helper so `Option`-returning field parses can flag their own defect
/// inline without an intermediate `match`.
trait InspectNone: Sized {
    fn inspect_none(self, flag: &mut bool) -> Self;
}

impl<T> InspectNone for Option<T> {
    fn inspect_none(self, flag: &mut bool) -> Self {
        if self.is_none() {
            *flag = true;
        }
        self
    }
}

/// Parse an optional integer field, reporting `(value, missing, invalid)`.
/// An unparseable value is reported as absent so callers degrade the same way
/// for "not written" and "written as garbage".
fn tolerant_u64(field: Option<&[u8]>) -> (Option<u64>, bool, bool) {
    match field {
        None => (None, true, false),
        Some(value) => match parse_u64_opt(value) {
            Some(parsed) => (Some(parsed), false, false),
            None => (None, false, true),
        },
    }
}

fn saturating_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Result of parsing all yEnc headers from an article.
#[derive(Debug)]
pub struct ParsedHeaders {
    pub metadata: YencMetadata,
    pub data_start: usize,
    pub data_end: usize,
    pub yend: Option<YendFields>,
}

/// Find a line starting with the given yEnc control keyword. Returns the byte
/// offset of the keyword within `input`, or `None`.
///
/// The keyword must be followed by ASCII whitespace or end-of-line, so junk
/// lines that merely share a prefix (`=yb`, `=ybegin_notes`) never match.
fn find_line_start(input: &[u8], keyword: &[u8]) -> Option<usize> {
    find_line_start_within(input, keyword, usize::MAX)
}

/// [`find_line_start`], giving up once a candidate line would start past
/// `max_start`.
///
/// The bound matters for `=ybegin`: the streaming and fused decoders both stop
/// scanning after [`crate::decode::MAX_HEADER_SCAN_BYTES`] of leading junk, and
/// the whole-buffer path has to agree with them about which articles are
/// header-less rather than scanning an arbitrarily long body.
fn find_line_start_within(input: &[u8], keyword: &[u8], max_start: usize) -> Option<usize> {
    // Check if the input itself starts with the keyword.
    if is_control_line(input, keyword) {
        return Some(0);
    }
    // Search for \n followed by the keyword (handles both \r\n and bare \n).
    let mut pos = 0;
    while pos < input.len() {
        if let Some(idx) = memchr_lf(&input[pos..]) {
            let abs = pos + idx + 1; // byte after \n
            if abs > max_start {
                return None;
            }
            if abs < input.len() && is_control_line(&input[abs..], keyword) {
                return Some(abs);
            }
            pos = abs;
        } else {
            break;
        }
    }
    None
}

/// The yEnc control line that ends a body, located by the same rule the SIMD
/// kernel's `search_end` uses.
#[derive(Debug, Clone, Copy)]
struct BodyControlLine {
    /// First byte that is no longer body data (the `\r` of the `\r\n`, or
    /// `data_start` when the body is empty). The kernel emits nothing for the
    /// line break or for the `=y` that follows it, so this is exactly where its
    /// decoded output stops.
    data_end: usize,
    /// Offset of the `=` that begins the control line, after any NNTP-stuffed
    /// dot the kernel would have stripped.
    keyword_start: usize,
}

/// `=y` at `at`, allowing the one NNTP-stuffed `.` the kernel strips at line
/// start in raw mode.
fn control_keyword_at(input: &[u8], at: usize, dot_unstuffing: bool) -> Option<usize> {
    let rest = input.get(at..)?;
    if rest.starts_with(b"=y") {
        return Some(at);
    }
    if dot_unstuffing && rest.first() == Some(&b'.') && rest[1..].starts_with(b"=y") {
        return Some(at + 1);
    }
    None
}

/// Find the control line that ends the body, matching the SIMD kernel's stop
/// rule byte for byte.
///
/// The kernel reaches its line-start state only through a literal `\r\n` (a
/// bare `\n` leaves it mid-line) and then stops at *any* `=y`, not specifically
/// at `=yend`. Scanning for `=yend` lines instead — which is what this used to
/// do — made the whole-buffer path disagree with the streaming and fused paths
/// on three reachable inputs: a `\r\n=y…` line that is not `=yend` (the kernel
/// stops and the article fails; the line scan decoded it as body data), a bare
/// `\n=yend ` (the line scan accepted a trailer the kernel never stops at), and
/// a dot-stuffed `\r\n.=yend ` (the kernel strips the dot, the line scan did
/// not).
fn find_body_control_line(
    input: &[u8],
    from: usize,
    dot_unstuffing: bool,
) -> Option<BodyControlLine> {
    // A body always begins immediately after a header line's terminator, and
    // the kernel starts a body in its line-start state, so `from` itself is a
    // candidate.
    if let Some(keyword_start) = control_keyword_at(input, from, dot_unstuffing) {
        return Some(BodyControlLine {
            data_end: from,
            keyword_start,
        });
    }

    let mut pos = from;
    while let Some(rel) = memchr_lf(&input[pos..]) {
        let lf = pos + rel;
        if lf > from
            && input[lf - 1] == b'\r'
            && let Some(keyword_start) = control_keyword_at(input, lf + 1, dot_unstuffing)
        {
            return Some(BodyControlLine {
                data_end: lf - 1,
                keyword_start,
            });
        }
        pos = lf + 1;
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

fn trim_line_end(bytes: &[u8]) -> &[u8] {
    bytes.trim_ascii_end()
}

#[derive(Default)]
struct YbeginFieldRefs<'a> {
    name: Option<&'a [u8]>,
    size: Option<&'a [u8]>,
    line: Option<&'a [u8]>,
    part: Option<&'a [u8]>,
    total: Option<&'a [u8]>,
}

#[derive(Default)]
struct YpartFieldRefs<'a> {
    begin: Option<&'a [u8]>,
    end: Option<&'a [u8]>,
}

#[derive(Default)]
struct YendFieldRefs<'a> {
    size: Option<&'a [u8]>,
    part: Option<&'a [u8]>,
    pcrc32: Option<&'a [u8]>,
    crc32: Option<&'a [u8]>,
}

fn visit_fields<'a>(line: &'a [u8], mut visit: impl FnMut(&[u8], &'a [u8])) {
    let mut remaining = line;

    loop {
        remaining = remaining.trim_ascii_start();
        if remaining.is_empty() {
            return;
        }

        let Some(eq_pos) = remaining.iter().position(|&b| b == b'=') else {
            return;
        };
        // The key is the last whitespace-delimited token before `=`, so a stray
        // token with no `=` of its own (`=ybegin junk line=128`) does not
        // swallow the following field's name.
        let key_start = remaining[..eq_pos]
            .iter()
            .rposition(|b| b.is_ascii_whitespace())
            .map_or(0, |idx| idx + 1);
        let key = &remaining[key_start..eq_pos];
        let value_start = eq_pos + 1;

        if key_eq_ascii_ignore_case(key, b"name") {
            let value = trim_line_end(&remaining[value_start..]);
            visit(key, value);
            return;
        }

        let value_end = remaining[value_start..]
            .iter()
            .position(|b| b.is_ascii_whitespace())
            .map(|offset| value_start + offset)
            .unwrap_or(remaining.len());
        let value = &remaining[value_start..value_end];
        visit(key, value);

        remaining = &remaining[value_end..];
    }
}

fn required_field<'a>(field: Option<&'a [u8]>, label: &str) -> Result<&'a [u8], YencError> {
    field.ok_or_else(|| YencError::MissingField(label.to_string()))
}

fn required_u64_field(field: Option<&[u8]>, label: &str) -> Result<u64, YencError> {
    parse_u64_bytes(required_field(field, label)?, label)
}

/// Parse an unsigned decimal field value. Zero-alloc and overflow-checked:
/// `None` for empty, non-digit, or wider-than-`u64` input.
///
/// Digits only, matching sabctools' `extract_int`, which requires a digit
/// immediately after the field name and therefore rejects `size=-1000` and
/// `size= 10` rather than storing a nonsense value.
fn parse_u64_opt(value: &[u8]) -> Option<u64> {
    let value = value.trim_ascii();
    if value.is_empty() {
        return None;
    }

    let mut parsed = 0u64;
    for &byte in value {
        if !byte.is_ascii_digit() {
            return None;
        }
        parsed = parsed
            .checked_mul(10)?
            .checked_add(u64::from(byte - b'0'))?;
    }

    Some(parsed)
}

/// `parse_u64_opt` with a labelled error, for the two `=ypart` fields that are
/// still genuinely required.
fn parse_u64_bytes(value: &[u8], label: &str) -> Result<u64, YencError> {
    parse_u64_opt(value).ok_or_else(|| YencError::InvalidHeader {
        field: label.to_string(),
        reason: format!("invalid integer: {}", bytes_to_string(value.trim_ascii())),
    })
}

/// Parse a `crc32=`/`pcrc32=` hex value, zero-alloc and overflow-checked.
///
/// Returns `None` for anything that is not a usable CRC. Callers treat `None`
/// as "the poster did not give us a CRC": an unparseable CRC carries no
/// verification value, and failing the whole article over it is exactly the
/// strictness the reference decoders do not have.
///
/// Matching sabctools (`from_chars` into a `uint64_t`, then truncated):
///  * fewer than 8 digits is fine — some encoders omit leading zeros;
///  * up to 16 digits is accepted and truncated to the low 32 bits, which
///    sabctools does deliberately for posters that emit over-long hashes;
///  * wider than 64 bits is unusable.
///
/// Two deliberate divergences, both toward tolerance, because sabctools turns
/// these into a *wrong* expected CRC that then fails the article:
///  * an empty value is absent here, where sabctools yields `0`;
///  * a value with non-hex bytes is absent here, where sabctools keeps the
///    leading hex run (`1234ZZZZ` -> `0x1234`).
fn parse_crc_hex_bytes(value: &[u8]) -> Option<u32> {
    let value = value.trim_ascii();
    if value.is_empty() {
        return None;
    }

    let mut parsed: u64 = 0;
    for &byte in value {
        let digit = match byte {
            b'0'..=b'9' => u64::from(byte - b'0'),
            b'a'..=b'f' => u64::from(byte - b'a') + 10,
            b'A'..=b'F' => u64::from(byte - b'A') + 10,
            _ => return None,
        };
        parsed = parsed.checked_mul(16)?.checked_add(digit)?;
    }

    Some(parsed as u32)
}

fn key_eq_ascii_ignore_case(actual: &[u8], expected: &[u8]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(&a, &b)| a.eq_ignore_ascii_case(&b))
}

/// Parse all yEnc headers from an article body.
///
/// Returns parsed metadata, the byte range of encoded data, and =yend fields.
///
/// This is a thin composition over the byte-wise line parsers above
/// ([`parse_ybegin_line`], [`apply_ypart_line`], [`parse_yend_line`]) so that
/// the whole-buffer path and the streaming/fused paths share exactly one set of
/// header semantics — same case-insensitivity, same tab handling, same
/// tolerance for missing fields.
pub fn parse_headers(input: &[u8]) -> Result<ParsedHeaders, YencError> {
    parse_headers_with_options(input, DecodeOptions::default())
}

/// [`parse_headers`] told whether the article still carries NNTP dot-stuffing.
///
/// Only the trailer scan cares: in raw mode the kernel strips one leading `.`
/// at line start before looking for `=y`, so `\r\n.=yend ` is a trailer there
/// and body data otherwise.
pub fn parse_headers_with_options(
    input: &[u8],
    options: DecodeOptions,
) -> Result<ParsedHeaders, YencError> {
    // Scan for the =ybegin line: SABnzbd and nzbget both search the article for
    // it rather than demanding it be the first line, so leading junk (headers
    // left in the body, poster banners, blank lines) does not kill the article.
    //
    // Bounded at the same 64 KiB the streaming and fused decoders use, so all
    // three entry points call the same articles header-less.
    let ybegin_start =
        find_line_start_within(input, b"=ybegin", crate::decode::MAX_HEADER_SCAN_BYTES)
            .ok_or(YencError::MissingHeader)?;
    let (ybegin_line_end, after_ybegin) = line_end(input, ybegin_start);

    let mut metadata = parse_ybegin_line(&input[ybegin_start..ybegin_line_end])?;
    metadata.defects.junk_before_ybegin = ybegin_start > 0;

    // If multi-part, parse =ypart.
    let data_start = if metadata.part.is_some() {
        let ypart_start = find_line_start(&input[after_ybegin..], b"=ypart")
            .map(|off| off + after_ybegin)
            .ok_or(YencError::MissingField("=ypart".to_string()))?;
        let (ypart_line_end, after_ypart) = line_end(input, ypart_start);

        apply_ypart_line(&input[ypart_start..ypart_line_end], &mut metadata)?;
        after_ypart
    } else {
        after_ybegin
    };

    // Find the control line that ends the body, by the kernel's stop rule.
    let (yend_fields, data_end) =
        match find_body_control_line(input, data_start, options.dot_unstuffing) {
            Some(control) => {
                let (line_end, _) = line_end(input, control.keyword_start);
                let line = &input[control.keyword_start..line_end];

                if !is_control_line(line, b"=yend") {
                    // The kernel already stopped here, so there is no reading of
                    // this article that keeps decoding. The streaming and fused
                    // decoders reject it with this exact error.
                    return Err(YencError::InvalidHeader {
                        field: "=yend".to_string(),
                        reason: "unexpected trailing line after yEnc body".to_string(),
                    });
                }

                (Some(parse_yend_line(line)?), control.data_end)
            }
            None => (None, input.len()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ybegin_line_extracts_metadata() {
        let metadata =
            parse_ybegin_line(b"=ybegin part=3 total=5 line=128 size=4096 name=test.bin\r\n")
                .unwrap();

        assert_eq!(metadata.name, "test.bin");
        assert_eq!(metadata.size, 4096);
        assert_eq!(metadata.line_length, 128);
        assert_eq!(metadata.part, Some(3));
        assert_eq!(metadata.total, Some(5));
        assert_eq!(metadata.begin, None);
        assert_eq!(metadata.end, None);
    }

    #[test]
    fn apply_ypart_line_updates_metadata() {
        let mut metadata =
            parse_ybegin_line(b"=ybegin part=1 total=2 line=128 size=4096 name=test.bin\r\n")
                .unwrap();

        apply_ypart_line(b"=ypart begin=257 end=512\r\n", &mut metadata).unwrap();

        assert_eq!(metadata.begin, Some(257));
        assert_eq!(metadata.end, Some(512));
    }

    /// `=ypart end=` past `=ybegin size=` is a known broken-poster class.
    /// Neither sabctools nor nzbget cross-checks the two, so weaver records the
    /// inconsistency instead of failing the article.
    #[test]
    fn apply_ypart_line_tolerates_end_past_declared_file_size() {
        let mut metadata =
            parse_ybegin_line(b"=ybegin part=1 total=2 line=128 size=4096 name=test.bin\r\n")
                .unwrap();
        apply_ypart_line(b"=ypart begin=1 end=4097\r\n", &mut metadata).unwrap();

        assert_eq!(metadata.begin, Some(1));
        assert_eq!(metadata.end, Some(4097));
        assert!(metadata.defects.ypart_end_exceeds_size);
    }

    #[test]
    fn apply_ypart_line_end_below_declared_file_size_is_clean() {
        let mut metadata =
            parse_ybegin_line(b"=ybegin part=1 total=2 line=128 size=4096 name=test.bin\r\n")
                .unwrap();
        apply_ypart_line(b"=ypart begin=1 end=4095\r\n", &mut metadata).unwrap();

        assert_eq!(metadata.end, Some(4095));
        assert!(!metadata.defects.ypart_end_exceeds_size);
        assert!(!metadata.defects.any());
    }

    /// `end < begin` stays a hard error: the part length would be negative, and
    /// weaver's assembler places bytes by `begin`, so guessing would write a
    /// corrupt file rather than fail an article.
    #[test]
    fn apply_ypart_line_still_rejects_end_before_begin() {
        let mut metadata =
            parse_ybegin_line(b"=ybegin part=1 total=2 line=128 size=4096 name=test.bin\r\n")
                .unwrap();
        let err = apply_ypart_line(b"=ypart begin=100 end=50\r\n", &mut metadata).unwrap_err();

        assert!(matches!(err, YencError::InvalidHeader { field, .. } if field == "end"));
    }

    #[test]
    fn parse_yend_line_extracts_trailer_fields() {
        let yend =
            parse_yend_line(b"=yend size=1234 part=2 pcrc32=ABCDEF12 crc32=01234567\r\n").unwrap();

        assert_eq!(yend.size, Some(1234));
        assert_eq!(yend.part, Some(2));
        assert_eq!(yend.pcrc32, Some(0xABCDEF12));
        assert_eq!(yend.crc32, Some(0x01234567));
    }

    #[test]
    fn parse_yend_line_rejects_non_yend_line() {
        let err = parse_yend_line(b"=ypart begin=1 end=10\r\n").unwrap_err();
        assert!(matches!(err, YencError::InvalidHeader { field, .. } if field == "=yend"));
    }

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

    /// Bare-LF articles: the `=ybegin` scan accepts them (all three entry
    /// points find headers line-wise, LF-anchored), but the *trailer* does not
    /// — the SIMD kernel reaches its line-start state only through a literal
    /// CRLF, so `\n=yend` is body data to the streaming and fused decoders and
    /// must be body data here too. See
    /// `decode::tests::bare_lf_trailer_is_body_data_in_every_entry_point`.
    #[test]
    fn bare_lf_line_endings_find_ybegin_but_not_the_trailer() {
        let input = b"=ybegin line=128 size=100 name=test.bin\n\
                       data\n\
                       =yend size=100 crc32=12345678\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.name, "test.bin");
        assert!(parsed.yend.is_none());
        assert_eq!(parsed.data_end, input.len());
    }

    /// The same article with CRLF endings does find its trailer.
    #[test]
    fn crlf_trailer_is_found() {
        let input = b"=ybegin line=128 size=100 name=test.bin\r\n\
                       data\r\n\
                       =yend size=100 crc32=12345678\r\n";
        let parsed = parse_headers(input).unwrap();
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

    /// No `=ybegin` field is required. sabctools defaults every numeric
    /// field to 0 and leaves `file_name` unset; nothing raises.
    #[test]
    fn missing_size_field_is_tolerated() {
        let input = b"=ybegin line=128 name=test.bin\r\ndata\r\n=yend size=100\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.size, 0);
        assert_eq!(parsed.metadata.name, "test.bin");
        assert!(parsed.metadata.defects.missing_size);
        assert!(!parsed.metadata.defects.invalid_size);
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
        assert!(parsed.metadata.defects.junk_before_ybegin);
    }

    // ── One byte-wise field parser behind every entry point ──────────────

    #[test]
    fn fields_tolerate_tabs_and_runs_of_spaces() {
        let metadata =
            parse_ybegin_line(b"=ybegin\tline=128 \t size=100\tname=test file.bin\r\n").unwrap();
        assert_eq!(metadata.line_length, 128);
        assert_eq!(metadata.size, 100);
        assert_eq!(metadata.name, "test file.bin");
        assert!(!metadata.defects.any());
    }

    #[test]
    fn field_names_are_case_insensitive_on_every_entry_point() {
        let metadata = parse_ybegin_line(b"=ybegin LINE=128 Size=100 NaMe=Test.BIN\r\n").unwrap();
        assert_eq!(metadata.line_length, 128);
        assert_eq!(metadata.size, 100);
        // The value's own case is preserved; only the key is folded.
        assert_eq!(metadata.name, "Test.BIN");

        let yend = parse_yend_line(b"=yend SIZE=100 PCRC32=abcdef12 CRC32=01234567\r\n").unwrap();
        assert_eq!(yend.size, Some(100));
        assert_eq!(yend.pcrc32, Some(0xABCDEF12));
        assert_eq!(yend.crc32, Some(0x01234567));

        // parse_headers must agree, since it is now the same parser.
        let input = b"=ybegin LINE=128 Size=100 NaMe=Test.BIN\r\ndata\r\n=yend SIZE=100\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.line_length, 128);
        assert_eq!(parsed.metadata.name, "Test.BIN");
        assert_eq!(parsed.yend.unwrap().size, Some(100));
    }

    #[test]
    fn stray_token_does_not_swallow_the_next_field_name() {
        let metadata = parse_ybegin_line(b"=ybegin junk line=128 size=100 name=x.bin\r\n").unwrap();
        assert_eq!(metadata.line_length, 128);
        assert_eq!(metadata.size, 100);
        assert_eq!(metadata.name, "x.bin");
    }

    // ── crc32 parsing degrades to "absent", never to a failure ──────────

    #[test]
    fn short_crc_hex() {
        // Some encoders omit leading zeros.
        assert_eq!(parse_crc_hex_bytes(b"1a2b"), Some(0x1A2B));
    }

    #[test]
    fn crc_hex_full_width() {
        assert_eq!(parse_crc_hex_bytes(b"DEADBEEF"), Some(0xDEADBEEF));
    }

    #[test]
    fn crc_hex_garbage_reads_as_absent() {
        for garbage in [
            b"GGGG".as_slice(),
            b"".as_slice(),
            b"   ".as_slice(),
            b"1234ZZZZ".as_slice(),
            b"0x1234".as_slice(),
            b"-1".as_slice(),
            // Wider than 64 bits: sabctools' from_chars reports out_of_range.
            b"DEADBEEFDEADBEEF0".as_slice(),
        ] {
            assert_eq!(
                parse_crc_hex_bytes(garbage),
                None,
                "expected {garbage:?} to read as absent"
            );
        }
    }

    #[test]
    fn crc_hex_over_long_truncates_to_low_32_bits_like_sabctools() {
        assert_eq!(parse_crc_hex_bytes(b"AAAAAAAADEADBEEF"), Some(0xDEADBEEF));
    }

    #[test]
    fn yend_records_unparseable_crc_as_absent_with_a_defect() {
        let yend = parse_yend_line(b"=yend size=10 pcrc32=nothex crc32=\r\n").unwrap();

        assert_eq!(yend.size, Some(10));
        assert_eq!(yend.pcrc32, None);
        assert_eq!(yend.crc32, None);
        assert!(yend.defects.invalid_pcrc32);
        assert!(yend.defects.invalid_crc32);
        assert!(!yend.defects.invalid_yend_size);
    }

    #[test]
    fn yend_records_unparseable_size_as_absent_with_a_defect() {
        let yend = parse_yend_line(b"=yend size=-5 crc32=DEADBEEF\r\n").unwrap();

        assert_eq!(yend.size, None);
        assert_eq!(yend.crc32, Some(0xDEADBEEF));
        assert!(yend.defects.invalid_yend_size);
    }

    // ── Every combination of missing =ybegin fields ─────────────────────

    #[test]
    fn every_combination_of_missing_ybegin_fields_parses() {
        for line_field in [None, Some("line=128")] {
            for size_field in [None, Some("size=100")] {
                for name_field in [None, Some("name=test.bin")] {
                    let mut header = String::from("=ybegin");
                    for field in [line_field, size_field, name_field].into_iter().flatten() {
                        header.push(' ');
                        header.push_str(field);
                    }
                    // A bare `=ybegin` still needs its separator to be detected
                    // at all -- both references key on the literal "=ybegin ".
                    header.push_str(" \r\n");

                    let metadata = parse_ybegin_line(header.as_bytes())
                        .unwrap_or_else(|err| panic!("{header:?} should parse, got {err}"));

                    assert_eq!(
                        metadata.line_length,
                        if line_field.is_some() { 128 } else { 0 }
                    );
                    assert_eq!(metadata.size, if size_field.is_some() { 100 } else { 0 });
                    assert_eq!(
                        metadata.name,
                        if name_field.is_some() { "test.bin" } else { "" }
                    );
                    assert_eq!(metadata.defects.missing_line, line_field.is_none());
                    assert_eq!(metadata.defects.missing_size, size_field.is_none());
                    assert_eq!(metadata.defects.missing_name, name_field.is_none());
                    assert!(!metadata.defects.invalid_line);
                    assert!(!metadata.defects.invalid_size);
                }
            }
        }
    }

    #[test]
    fn unparseable_ybegin_numbers_read_as_absent() {
        // sabctools' extract_int requires a digit right after the needle, so
        // `size=-1000` leaves file_size at its default rather than storing -1000.
        let metadata = parse_ybegin_line(b"=ybegin line=abc size=-1000 name=neg.bin\r\n").unwrap();

        assert_eq!(metadata.size, 0);
        assert_eq!(metadata.line_length, 0);
        assert!(metadata.defects.invalid_size);
        assert!(metadata.defects.invalid_line);
        assert!(!metadata.defects.missing_size);
        assert!(!metadata.defects.missing_line);
    }

    #[test]
    fn unparseable_part_field_falls_back_to_single_part() {
        let metadata = parse_ybegin_line(b"=ybegin part=x line=128 size=1 name=a.bin\r\n").unwrap();
        assert_eq!(metadata.part, None);
    }

    #[test]
    fn oversized_numbers_do_not_panic_or_wrap() {
        let huge = "9".repeat(40);
        let header = format!("=ybegin part={huge} total={huge} line={huge} size={huge} name=b\r\n");
        let metadata = parse_ybegin_line(header.as_bytes()).unwrap();

        assert_eq!(metadata.size, 0);
        assert_eq!(metadata.line_length, 0);
        assert_eq!(metadata.part, None);
        assert_eq!(metadata.total, None);
        assert!(metadata.defects.invalid_size);
    }

    #[test]
    fn line_length_wider_than_u32_saturates_rather_than_wrapping() {
        let metadata = parse_ybegin_line(b"=ybegin line=4294967296 size=1 name=b.bin\r\n").unwrap();
        assert_eq!(metadata.line_length, u32::MAX);
    }

    // ── Control-line detection ──────────────────────────────────────────

    #[test]
    fn keyword_detection_requires_a_separator() {
        // Real headers.
        assert!(is_control_line(
            b"=ybegin line=1 size=1 name=a\r\n",
            b"=ybegin"
        ));
        assert!(is_control_line(b"=ybegin\tline=1\r\n", b"=ybegin"));
        assert!(is_control_line(b"=ybegin \r\n", b"=ybegin"));

        // Look-alikes that must not be mistaken for headers.
        for junk in [
            b"=yb\r\n".as_slice(),
            b"=ybegi\r\n".as_slice(),
            b"=ybegin\r\n".as_slice(),
            b"=ybeginner notes\r\n".as_slice(),
            b"=ybegin=1\r\n".as_slice(),
            b"x=ybegin size=1\r\n".as_slice(),
        ] {
            assert!(
                !is_control_line(junk, b"=ybegin"),
                "expected {junk:?} not to be a =ybegin line"
            );
        }
    }

    #[test]
    fn scan_skips_junk_lines_that_contain_equals_and_partial_prefixes() {
        let mut input = Vec::new();
        input.extend_from_slice(b"Subject: something = other\r\n");
        input.extend_from_slice(b"=yb\r\n");
        input.extend_from_slice(b"=ybegi partial\r\n");
        input.extend_from_slice(b"=ybeginner notes here\r\n");
        input.extend_from_slice(b"\r\n");
        input.extend_from_slice(b"=ybegin line=128 size=100 name=real.bin\r\n");
        input.extend_from_slice(b"data\r\n");
        input.extend_from_slice(b"=yend size=100\r\n");

        let parsed = parse_headers(&input).unwrap();
        assert_eq!(parsed.metadata.name, "real.bin");
        assert_eq!(parsed.metadata.size, 100);
        assert!(parsed.metadata.defects.junk_before_ybegin);
    }

    #[test]
    fn scan_finds_ybegin_after_bare_lf_junk() {
        let input = b"junk\n=ybegin line=128 size=100 name=lf.bin\ndata\n=yend size=100\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.name, "lf.bin");
        assert!(parsed.metadata.defects.junk_before_ybegin);
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
    fn ypart_end_exceeds_file_size_is_recorded_not_rejected() {
        let input = b"=ybegin part=1 line=128 size=1000 name=myfile.dat\r\n\
                       =ypart begin=1 end=2000\r\n\
                       data\r\n\
                       =yend size=2000\r\n";
        let parsed = parse_headers(input).unwrap();
        assert_eq!(parsed.metadata.begin, Some(1));
        assert_eq!(parsed.metadata.end, Some(2000));
        assert!(parsed.metadata.defects.ypart_end_exceeds_size);
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
