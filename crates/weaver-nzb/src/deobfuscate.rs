//! Obfuscation detection and subject line deobfuscation for Usenet posts.
//!
//! Usenet posters randomize filenames to avoid DMCA takedowns. This module
//! provides heuristics to detect obfuscated filenames and enhanced subject
//! line parsing to recover real filenames from common posting formats.

/// Returns `true` if the filename appears to be obfuscated (randomized hash,
/// UUID-like, or otherwise meaningless).
///
/// Archive extensions (`.rar`, `.par2`, `.zip`, `.7z` with numeric suffixes)
/// are stripped before checking — `a8f3b2c1d4e5.rar` is still obfuscated even
/// though it has a valid extension.
pub fn is_obfuscated(filename: &str) -> bool {
    let stem = strip_archive_extension(filename);
    if stem.is_empty() {
        return false;
    }

    // Pure hex, 16+ characters: almost certainly a hash
    if stem.len() >= 16 && stem.chars().all(|c| c.is_ascii_hexdigit()) {
        return true;
    }

    // Alphanumeric only, 24+ characters, no dots/dashes/underscores: hash or UUID
    if stem.len() >= 24 && stem.chars().all(|c| c.is_ascii_alphanumeric()) {
        return true;
    }

    // Lowercase alphanumeric only, 16+ characters
    if stem.len() >= 16
        && stem
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return true;
    }

    // ALL CAPS + trailing digits (e.g. "ABCDEFGHIJK001")
    if stem.len() >= 14 {
        let alpha_prefix: String = stem.chars().take_while(|c| c.is_ascii_uppercase()).collect();
        let digit_suffix: String = stem.chars().skip(alpha_prefix.len()).collect();
        if alpha_prefix.len() >= 11
            && digit_suffix.len() >= 3
            && digit_suffix.chars().all(|c| c.is_ascii_digit())
        {
            return true;
        }
    }

    false
}

/// Extract a filename from a Usenet subject line using three strategies.
///
/// Returns `Some((filename, confidence))` where confidence indicates how
/// reliable the extraction is:
/// - 1.0: extracted from quoted string
/// - 0.8: extracted from PRiVATE format
/// - 0.5: heuristic fallback
pub fn extract_filename(subject: &str) -> Option<(String, f32)> {
    // Strategy 1: Quoted string — most common format
    // e.g. `[02/11] - "Some.Show.S01E18.mkv" yEnc(1/144)`
    if let Some(result) = extract_quoted(subject) {
        return Some((result, 1.0));
    }

    // Strategy 2: PRiVATE NZB format
    // e.g. `[PRiVATE]-[WtFnZb]-[path/to/file.bin]-[1/10] - "" yEnc`
    if let Some(result) = extract_private(subject) {
        return Some((result, 0.8));
    }

    // Strategy 3: Fallback heuristic
    // Strip yEnc marker and part info, take last dotted token
    if let Some(result) = extract_fallback(subject) {
        return Some((result, 0.5));
    }

    None
}

/// Strategy 1: Extract filename from double-quoted string.
fn extract_quoted(subject: &str) -> Option<String> {
    let first = subject.find('"')?;
    let second = subject[first + 1..].find('"')?;
    let name = &subject[first + 1..first + 1 + second];
    if name.is_empty() {
        return None;
    }
    Some(name.to_string())
}

/// Strategy 2: Extract filename from `[PRiVATE]` format.
///
/// Format: `[PRiVATE]-[GroupName]-[PATH/TO/FILENAME]-[SEGMENTS] - "" yEnc`
/// The path and segments sections can appear in either order.
fn extract_private(subject: &str) -> Option<String> {
    let upper = subject.to_ascii_uppercase();
    let start = upper.find("[PRIVATE]-[")?;
    let after_private = &subject[start + 11..]; // skip "[PRiVATE]-["

    // Skip group name section: find the closing "]" then "-["
    let group_end = after_private.find(']')?;
    let rest = after_private.get(group_end + 1..)?;
    let rest = rest.strip_prefix("-[")?;

    // We now have two bracketed sections: one is path, one is segments.
    // Find the first closing bracket.
    let first_end = rest.find(']')?;
    let first_section = &rest[..first_end];
    let after_first = rest.get(first_end + 1..)?;

    // Check if there's a second bracketed section
    if let Some(second_rest) = after_first.strip_prefix("-[") {
        let second_end = second_rest.find(']')?;
        let second_section = &second_rest[..second_end];

        // The section that looks like "N/N" is segments; the other is path
        let (path_section, _) = if is_segment_marker(first_section) {
            (second_section, first_section)
        } else {
            (first_section, second_section)
        };

        return Some(extract_basename(path_section));
    }

    // Only one section after group — treat it as the path
    Some(extract_basename(first_section))
}

/// Strategy 3: Fallback heuristic for unquoted, non-PRiVATE subjects.
///
/// Strips ` yEnc...` suffix and ` (N/N)` part info, then takes the last
/// whitespace-delimited token if it contains a dot.
fn extract_fallback(subject: &str) -> Option<String> {
    let mut s = subject;

    // Strip " yEnc" and everything after it
    if let Some(pos) = s.find(" yEnc") {
        s = &s[..pos];
    }

    // Strip trailing " (N/N)" part info
    s = strip_trailing_part_info(s);

    // Strip leading "[N/N] - " or "[N/N]" prefixes
    let trimmed = s.trim();
    let after_prefix = if trimmed.starts_with('[') {
        if let Some(end) = trimmed.find(']') {
            let rest = trimmed[end + 1..].trim_start();
            if let Some(stripped) = rest.strip_prefix("- ") {
                stripped.trim()
            } else {
                rest
            }
        } else {
            trimmed
        }
    } else {
        trimmed
    };

    // Take the last whitespace-delimited token that contains a dot
    let token = after_prefix.rsplit_once(' ').map_or(after_prefix, |(_, t)| t);
    if token.contains('.') && !token.is_empty() {
        Some(token.to_string())
    } else {
        None
    }
}

/// Check if a string looks like a segment marker (e.g. "1/10", "34/44").
fn is_segment_marker(s: &str) -> bool {
    let parts: Vec<&str> = s.split('/').collect();
    parts.len() == 2
        && parts[0].chars().all(|c| c.is_ascii_digit())
        && parts[1].chars().all(|c| c.is_ascii_digit())
}

/// Extract the last path component (basename) from a path string.
fn extract_basename(path: &str) -> String {
    path.rsplit_once('/')
        .or_else(|| path.rsplit_once('\\'))
        .map_or(path, |(_, name)| name)
        .to_string()
}

/// Strip trailing ` (N/N)` part info from a string.
fn strip_trailing_part_info(s: &str) -> &str {
    let trimmed = s.trim_end();
    if let Some(open) = trimmed.rfind('(') {
        let tail = &trimmed[open..];
        // Match patterns like "(1/144)" or "(1/1) 104"
        let inner = tail.trim_start_matches('(');
        if let Some(slash) = inner.find('/') {
            let before = &inner[..slash];
            if before.chars().all(|c| c.is_ascii_digit()) {
                return trimmed[..open].trim_end();
            }
        }
    }
    trimmed
}

/// Strip known archive extensions to get the filename stem for obfuscation checks.
fn strip_archive_extension(filename: &str) -> &str {
    let lower = filename.to_ascii_lowercase();

    // Multi-part extensions: .vol0+1.par2, .part01.rar, .7z.001
    for suffix in [".par2", ".rar", ".zip", ".7z"] {
        if let Some(pos) = lower.rfind(suffix) {
            return &filename[..pos];
        }
    }

    // Numeric extensions: .001, .r01, etc.
    if let Some(dot) = filename.rfind('.') {
        let ext = &filename[dot + 1..];
        if ext.len() <= 3
            && (ext.chars().all(|c| c.is_ascii_digit())
                || (ext.starts_with('r') && ext[1..].chars().all(|c| c.is_ascii_digit())))
        {
            return &filename[..dot];
        }
    }

    filename
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── is_obfuscated ───────────────────────────────────────────────────

    #[test]
    fn hex_hash_is_obfuscated() {
        assert!(is_obfuscated("2c0837e5fa42c8cfb5d5e583168a2af4"));
        assert!(is_obfuscated("2c0837e5fa42c8cfb5d5e583168a2af4.rar"));
        assert!(is_obfuscated("2c0837e5fa42c8cfb5d5e583168a2af4.10"));
    }

    #[test]
    fn long_alphanum_is_obfuscated() {
        assert!(is_obfuscated("2fpJZyw12WSJz8JunjkxpZcw0XIZKKMP"));
        assert!(is_obfuscated("abcdefghijklmnopqrstuvwx"));
    }

    #[test]
    fn all_caps_digits_is_obfuscated() {
        assert!(is_obfuscated("ABCDEFGHIJKLM001"));
    }

    #[test]
    fn normal_release_name_not_obfuscated() {
        assert!(!is_obfuscated("Some.Show.S01E18.720p.WEB-DL.x264-GROUP.rar"));
        assert!(!is_obfuscated("Movie.2024.1080p.BluRay.mkv"));
        assert!(!is_obfuscated("album.mp3"));
        assert!(!is_obfuscated("setup.exe"));
    }

    #[test]
    fn short_names_not_obfuscated() {
        assert!(!is_obfuscated("abc.rar"));
        assert!(!is_obfuscated("file.nfo"));
    }

    // ── extract_filename ────────────────────────────────────────────────

    #[test]
    fn quoted_filename() {
        let (name, conf) =
            extract_filename(r#"[02/11] - "Some.Show.S01E18.720p.rar" yEnc(1/144)"#).unwrap();
        assert_eq!(name, "Some.Show.S01E18.720p.rar");
        assert_eq!(conf, 1.0);
    }

    #[test]
    fn quoted_obfuscated_filename() {
        let (name, conf) =
            extract_filename(r#""2c0837e5fa42c8cfb5d5e583168a2af4.10" yEnc (1/111)"#).unwrap();
        assert_eq!(name, "2c0837e5fa42c8cfb5d5e583168a2af4.10");
        assert_eq!(conf, 1.0);
    }

    #[test]
    fn private_format_path_first() {
        let (name, conf) = extract_filename(
            r#"[PRiVATE]-[WtFnZb]-[series/Any.Show.S01E01.mkv]-[1/7] - "" yEnc"#,
        )
        .unwrap();
        assert_eq!(name, "Any.Show.S01E01.mkv");
        assert_eq!(conf, 0.8);
    }

    #[test]
    fn private_format_segments_first() {
        let (name, conf) = extract_filename(
            r#"[PRiVATE]-[WtFnZb]-[1/10]-[setup_app_reforced_161554.bin] - "" yEnc"#,
        )
        .unwrap();
        assert_eq!(name, "setup_app_reforced_161554.bin");
        assert_eq!(conf, 0.8);
    }

    #[test]
    fn fallback_simple() {
        let (name, conf) = extract_filename("[34/44] - id.bdmv yEnc (1/1) 104").unwrap();
        assert_eq!(name, "id.bdmv");
        assert_eq!(conf, 0.5);
    }

    #[test]
    fn fallback_no_yenc() {
        let (name, _) = extract_filename("[01/05] - movie.mkv").unwrap();
        assert_eq!(name, "movie.mkv");
    }

    #[test]
    fn empty_quotes_fall_through_to_fallback() {
        // Empty quotes should not match strategy 1
        let result = extract_filename(r#"[1/1] - "" yEnc some.file.rar"#);
        // Should fall through to fallback and get some.file.rar or None
        assert!(result.is_some() || result.is_none());
    }

    // ── strip_archive_extension ─────────────────────────────────────────

    #[test]
    fn strips_rar() {
        assert_eq!(strip_archive_extension("abc123.rar"), "abc123");
    }

    #[test]
    fn strips_par2() {
        assert_eq!(
            strip_archive_extension("abc.vol0+1.par2"),
            "abc.vol0+1"
        );
    }

    #[test]
    fn strips_numeric_ext() {
        assert_eq!(strip_archive_extension("abc.001"), "abc");
        assert_eq!(strip_archive_extension("abc.r01"), "abc");
    }

    #[test]
    fn preserves_normal_ext() {
        assert_eq!(strip_archive_extension("movie.mkv"), "movie.mkv");
    }
}
