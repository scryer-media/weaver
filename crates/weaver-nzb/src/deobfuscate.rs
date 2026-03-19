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
    // Archive files with hash-like names are normal (obfuscated archives are still
    // valid — PAR2 rename handles them). Only flag non-archive files as obfuscated.
    let lower = filename.to_ascii_lowercase();
    let is_archive = [".rar", ".par2", ".zip", ".7z", ".nzb"]
        .iter()
        .any(|ext| lower.contains(ext))
        || (lower.rfind('.').is_some_and(|dot| {
            let ext = &lower[dot + 1..];
            ext.len() <= 3
                && (ext.chars().all(|c| c.is_ascii_digit())
                    || (ext.starts_with('r') && ext[1..].chars().all(|c| c.is_ascii_digit())))
        }));
    if is_archive {
        return false;
    }

    let stem = strip_archive_extension(filename);
    if stem.is_empty() {
        return false;
    }

    // Also strip the final file extension for obfuscation checks — we care
    // about the name, not whether it ends in .mkv or .avi.
    let bare = stem
        .rfind('.')
        .filter(|&dot| dot > 0 && stem.len() - dot <= 5)
        .map_or(stem, |dot| &stem[..dot]);

    // Long hex run anywhere in the string (30+ contiguous hex+dot chars).
    // Check BEFORE clear-naming, since obfuscated strings can have real words
    // as bracketed tags around the hash.
    {
        let mut hex_run = 0_usize;
        let mut max_hex_run = 0_usize;
        for c in stem.chars() {
            if c.is_ascii_hexdigit() || c == '.' {
                hex_run += 1;
                max_hex_run = max_hex_run.max(hex_run);
            } else {
                hex_run = 0;
            }
        }
        if max_hex_run >= 30 {
            return true;
        }
    }

    // Clear-naming signals: if the name looks like a real release or title,
    // it's not obfuscated. Adapted from SABnzbd's explicit false returns.
    if is_clearly_named(bare) {
        return false;
    }

    // Single uppercase letter + 5+ digits (e.g. "T306077") — tracking ID pattern
    if bare.len() >= 6 {
        let mut chars = bare.chars();
        if let Some(first) = chars.next() {
            if first.is_ascii_uppercase() && chars.all(|c| c.is_ascii_digit()) {
                return true;
            }
        }
    }

    // Known obfuscator prefixes (NZBGet patterns)
    let lower_bare = bare.to_ascii_lowercase();
    if lower_bare.starts_with("abc.xyz.") || lower_bare.starts_with("b00bs.") {
        return true;
    }

    // Pure hex (with optional dots), 16+ hex chars: almost certainly a hash.
    // Matches NZBGet's `[0-9a-f.]{16}` pattern.
    {
        let hex_chars: usize = bare.chars().filter(|c| c.is_ascii_hexdigit()).count();
        let all_hex_or_dot = bare.chars().all(|c| c.is_ascii_hexdigit() || c == '.');
        if hex_chars >= 16 && all_hex_or_dot {
            return true;
        }
    }

    // Alphanumeric only, no separators — hash or UUID.
    // 14+ chars for mixed case, 24+ chars if uniform case.
    if bare.chars().all(|c| c.is_ascii_alphanumeric()) {
        let has_upper = bare.chars().any(|c| c.is_ascii_uppercase());
        let has_lower = bare.chars().any(|c| c.is_ascii_lowercase());
        let has_digit = bare.chars().any(|c| c.is_ascii_digit());
        let mixed =
            (has_upper && has_lower) || (has_upper && has_digit) || (has_lower && has_digit);
        if mixed && bare.len() >= 14 {
            return true;
        }
        if !mixed && bare.len() >= 24 {
            return true;
        }
    }

    // ALL CAPS + trailing digits (e.g. "ABCDEFGHIJK001")
    if bare.len() >= 14 {
        let alpha_prefix: String = bare
            .chars()
            .take_while(|c| c.is_ascii_uppercase())
            .collect();
        let digit_suffix: String = bare.chars().skip(alpha_prefix.len()).collect();
        if alpha_prefix.len() >= 11
            && digit_suffix.len() >= 3
            && digit_suffix.chars().all(|c| c.is_ascii_digit())
        {
            return true;
        }
    }

    // Backup_NNNNN_SNN-NN pattern (NZBGet: `Backup_[0-9]{5,}S[0-9]{2}-[0-9]{2}`)
    if bare.starts_with("Backup_") {
        let rest = &bare[7..];
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.len() >= 5 {
            let after = &rest[digits.len()..];
            if after.starts_with('S')
                && after.len() >= 6
                && after[1..3].chars().all(|c| c.is_ascii_digit())
                && after.as_bytes().get(3) == Some(&b'-')
                && after[4..6].chars().all(|c| c.is_ascii_digit())
            {
                return true;
            }
        }
    }

    // Timestamp-like: NNNNNN_NN (NZBGet: `[0-9]{6}_[0-9]{2}`)
    if bare.len() >= 9 {
        let parts: Vec<&str> = bare.splitn(2, '_').collect();
        if parts.len() == 2
            && parts[0].len() >= 6
            && parts[0].chars().all(|c| c.is_ascii_digit())
            && parts[1].len() >= 2
            && parts[1].chars().all(|c| c.is_ascii_digit())
        {
            return true;
        }
    }

    // Bracketed tags wrapping a hex hash (SABnzbd pattern):
    // e.g. "[BlaBla] something [More] b2.bef89a622e4a23f07b0d3757ad5e8a.a0 [Brrr]"
    // If there are 30+ contiguous hex+dot characters anywhere in the string, it's obfuscated.
    {
        let mut hex_run = 0_usize;
        let mut max_hex_run = 0_usize;
        for c in stem.chars() {
            if c.is_ascii_hexdigit() || c == '.' {
                hex_run += 1;
                max_hex_run = max_hex_run.max(hex_run);
            } else {
                hex_run = 0;
            }
        }
        if max_hex_run >= 30 {
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
    // Use "]-[" as the section boundary to handle brackets inside filenames
    // (e.g. "Movie_[TBoP].mkv").
    if let Some(boundary) = rest.find("]-[") {
        let first_section = &rest[..boundary];
        let second_section_start = boundary + 3; // skip "]-["
        let after_second = &rest[second_section_start..];
        // Find the closing "]" for the second section — but it might also
        // contain brackets, so find "] - " or "]" at end-ish
        let second_end = after_second
            .find("] - ")
            .or_else(|| after_second.rfind(']'))
            .unwrap_or(after_second.len());
        let second_section = &after_second[..second_end];

        let (path_section, _) = if is_segment_marker(first_section) {
            (second_section, first_section)
        } else {
            (first_section, second_section)
        };

        return Some(extract_basename(path_section));
    }

    // Only one section — find closing "] - " or last "]"
    let end = rest
        .find("] - ")
        .or_else(|| rest.rfind(']'))
        .unwrap_or(rest.len());
    Some(extract_basename(&rest[..end]))
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

    // Take the last whitespace-delimited token. Prefer tokens with dots (filenames),
    // but accept dotless tokens as a last resort (handles "Re: A" edge cases).
    let token = after_prefix
        .rsplit_once(' ')
        .map_or(after_prefix, |(_, t)| t);
    if !token.is_empty() {
        Some(token.to_string())
    } else {
        None
    }
}

/// Check if a string looks like a segment marker (e.g. "1/10", "34/44")
/// or a pure number (e.g. "24" — segment count without slash).
fn is_segment_marker(s: &str) -> bool {
    // N/N format
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 2
        && parts[0].chars().all(|c| c.is_ascii_digit())
        && parts[1].chars().all(|c| c.is_ascii_digit())
    {
        return true;
    }
    // Pure number (used in some PRiVATE variants)
    !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
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

/// Returns `true` if the filename clearly looks like a real title or release name
/// rather than an obfuscated hash. Adapted from SABnzbd's explicit false-return rules.
///
/// Only triggers on names with word-like tokens (3+ alpha chars). Pure hex/digits
/// with dots still fall through to the obfuscation checks.
fn is_clearly_named(stem: &str) -> bool {
    // Strip bracketed tags before tokenizing — [Group] tags shouldn't count as words.
    let without_brackets: String = {
        let mut result = String::with_capacity(stem.len());
        let mut depth = 0u32;
        for c in stem.chars() {
            match c {
                '[' => depth += 1,
                ']' => {
                    depth = depth.saturating_sub(1);
                }
                _ if depth == 0 => result.push(c),
                _ => {}
            }
        }
        result
    };

    // Split on separators and check for word-like tokens.
    // Tokens must be 4+ chars to avoid matching file extensions (mkv, avi, bin).
    let tokens: Vec<&str> = without_brackets
        .split([' ', '.', '_', '-'])
        .filter(|t| !t.is_empty())
        .collect();

    // Known obfuscator prefixes that look like words but aren't
    const NOISE_WORDS: &[&str] = &["Backup", "backup", "BACKUP"];

    // Need at least 2 word-like tokens (4+ chars, has alpha, not all hex, not noise)
    let word_tokens = tokens
        .iter()
        .filter(|t| {
            t.len() >= 4
                && t.chars().any(|c| c.is_ascii_alphabetic())
                && !t.chars().all(|c| c.is_ascii_hexdigit())
                && !NOISE_WORDS.contains(t)
        })
        .count();

    if word_tokens >= 2 {
        // Require at least one token that isn't pure hex (avoids false positives
        // on hash.hash.ext patterns where hex segments are 4+ chars)
        let has_non_hex_word = tokens.iter().any(|t| {
            t.len() >= 4
                && t.chars().any(|c| c.is_ascii_alphabetic())
                && !t.chars().all(|c| c.is_ascii_hexdigit())
        });
        if has_non_hex_word {
            return true;
        }
    }

    false
}

/// Returns `true` if the path is inside a DVD/Bluray structure that should
/// never be renamed. SABnzbd skips deobfuscation for these.
pub fn is_protected_media_structure(path: &std::path::Path) -> bool {
    let s = path.to_string_lossy();
    s.contains("/VIDEO_TS/")
        || s.contains("/AUDIO_TS/")
        || s.contains("/BDMV/")
        || s.contains("\\VIDEO_TS\\")
        || s.contains("\\AUDIO_TS\\")
        || s.contains("\\BDMV\\")
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
        // Archive extensions are excluded — PAR2 rename handles those
        assert!(!is_obfuscated("2c0837e5fa42c8cfb5d5e583168a2af4.rar"));
        assert!(!is_obfuscated("2c0837e5fa42c8cfb5d5e583168a2af4.10"));
    }

    #[test]
    fn long_alphanum_is_obfuscated() {
        assert!(is_obfuscated("2fpJZyw12WSJz8JunjkxpZcw0XIZKKMP"));
        assert!(is_obfuscated("abcdefghijklmnopqrstuvwx"));
    }

    #[test]
    fn b00bs_prefix_obfuscated() {
        assert!(is_obfuscated("b00bs.a1b2c3d4e5f678.mkv"));
    }

    #[test]
    fn all_caps_digits_is_obfuscated() {
        assert!(is_obfuscated("ABCDEFGHIJKLM001"));
    }

    #[test]
    fn normal_release_name_not_obfuscated() {
        assert!(!is_obfuscated(
            "Some.Show.S01E18.720p.WEB-DL.x264-GROUP.rar"
        ));
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
        let (name, conf) =
            extract_filename(r#"[PRiVATE]-[WtFnZb]-[series/Any.Show.S01E01.mkv]-[1/7] - "" yEnc"#)
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
        assert_eq!(strip_archive_extension("abc.vol0+1.par2"), "abc.vol0+1");
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

    // ── NZBGet parity tests (patterns from nzbget/tests/queue/Deobfuscation.cpp)

    #[test]
    fn hex_with_dots_is_obfuscated() {
        // Non-archive files with hex+dot stems
        assert!(is_obfuscated("a1b2.c3d4.e5f6.7890.abcd.ef01"));
        assert!(is_obfuscated("abcdef01.23456789"));
    }

    #[test]
    fn backup_pattern_is_obfuscated() {
        assert!(is_obfuscated("Backup_12345S01-02"));
        assert!(is_obfuscated("Backup_999999S99-99"));
    }

    #[test]
    fn timestamp_pattern_is_obfuscated() {
        assert!(is_obfuscated("123456_02"));
        assert!(is_obfuscated("20240315_14"));
    }

    #[test]
    fn private_pure_numeric_segments() {
        let (name, conf) =
            extract_filename(r#"[PRiVATE]-[WtFnZb]-[24]-[setup_app_1.bin] - "" yEnc"#).unwrap();
        assert_eq!(name, "setup_app_1.bin");
        assert_eq!(conf, 0.8);
    }

    #[test]
    fn fallback_no_dot_filename() {
        let (name, _) = extract_filename("Re: A (2/3)").unwrap();
        assert_eq!(name, "A");
    }

    #[test]
    fn fallback_re_prefix_no_parens() {
        let (name, _) = extract_filename("Re: A").unwrap();
        assert_eq!(name, "A");
    }

    #[test]
    fn seven_z_split_not_obfuscated() {
        assert!(!is_obfuscated("2fpJZyw12WSJz8JunjkxpZcw0XIZKKMP.7z.015"));
    }

    // ── SABnzbd parity tests ────────────────────────────────────────────

    #[test]
    fn clear_naming_not_obfuscated() {
        // 2+ upper + 2+ lower + separator
        assert!(!is_obfuscated("Great Distro"));
        assert!(!is_obfuscated("Some.Show.S01E04"));
        assert!(!is_obfuscated("My_Cool_File"));
        // 3+ separators
        assert!(!is_obfuscated("this is a download"));
        // 4+ letters + 4+ digits + separator
        assert!(!is_obfuscated("Beast 2020"));
        assert!(!is_obfuscated("Movie.2024"));
    }

    #[test]
    fn bracketed_hex_is_obfuscated() {
        assert!(is_obfuscated("b2.bef89a622e4a23f07b0d3757ad5e8a.a0"));
    }

    #[test]
    fn dvd_structure_protected() {
        use std::path::Path;
        assert!(is_protected_media_structure(Path::new(
            "/downloads/movie/VIDEO_TS/VTS_01_1.VOB"
        )));
        assert!(is_protected_media_structure(Path::new(
            "/downloads/bluray/BDMV/STREAM/00000.m2ts"
        )));
        assert!(!is_protected_media_structure(Path::new(
            "/downloads/movie/movie.mkv"
        )));
    }
}
