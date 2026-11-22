// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Obfuscation detection and subject line deobfuscation for Usenet posts.
//
// Usenet posters randomize filenames to avoid DMCA takedowns. This module
// provides heuristics to detect obfuscated filenames and enhanced subject
// line parsing to recover real filenames from common posting formats.

// Returns `true` if the filename appears to be obfuscated (randomized hash,
// UUID-like, or otherwise meaningless).
//
// Archive extensions (`.rar`, `.par2`, `.zip`, `.7z` with numeric suffixes)
// are stripped before checking — `a8f3b2c1d4e5.rar` is still obfuscated even
// though it has a valid extension.
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
    // it's not obfuscated.
    if is_clearly_named(bare) {
        return false;
    }

    // Single uppercase letter + 5+ digits (e.g. "T306077") — tracking ID pattern
    if bare.len() >= 6 {
        let mut chars = bare.chars();
        if let Some(first) = chars.next()
            && first.is_ascii_uppercase()
            && chars.all(|c| c.is_ascii_digit())
        {
            return true;
        }
    }

    // Known obfuscator prefixes
    let lower_bare = bare.to_ascii_lowercase();
    if lower_bare.starts_with("abc.xyz.") || lower_bare.starts_with("b00bs.") {
        return true;
    }

    // Pure hex (with optional dots), 16+ hex chars: almost certainly a hash
    // (`[0-9a-f.]{16}`).
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
        let char_classes = has_upper as u8 + has_lower as u8 + has_digit as u8;
        let mixed = char_classes >= 2;
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

    // Backup_NNNNN_SNN-NN pattern (`Backup_[0-9]{5,}S[0-9]{2}-[0-9]{2}`)
    if let Some(rest) = bare.strip_prefix("Backup_") {
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.len() >= 5 {
            let after = &rest.as_bytes()[digits.len()..];
            if after.len() >= 6
                && after[0] == b'S'
                && after[1..3].iter().all(u8::is_ascii_digit)
                && after[3] == b'-'
                && after[4..6].iter().all(u8::is_ascii_digit)
            {
                return true;
            }
        }
    }

    // Timestamp-like: NNNNNN_NN (`[0-9]{6}_[0-9]{2}`)
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

    // Bracketed tags wrapping a hex hash:
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

// Extract a filename from a Usenet subject line using three strategies.
//
// Returns `Some((filename, confidence))` where confidence indicates how
// reliable the extraction is:
// - 1.0: extracted from quoted string
// - 0.8: extracted from PRiVATE format
// - 0.5: heuristic fallback
pub fn extract_filename(subject: &str) -> Option<(String, f64)> {
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

// Strategy 1: Extract filename from double-quoted string.
fn extract_quoted(subject: &str) -> Option<String> {
    quoted_name(subject).map(str::to_string)
}

// The first closed quote pair of a subject that holds more than spaces,
// with the spaces trimmed (`"([^"]*)"`, then `strip(' "')`). A quote mark is never part of a name:
// an unpaired one is left to the later strategies, which split on it.
pub(crate) fn quoted_name(subject: &str) -> Option<&str> {
    let mut runs = subject.split('"');
    runs.next();
    loop {
        let inside = runs.next()?;
        // The pair is closed only when a quote follows the run.
        runs.next()?;
        let name = inside.trim_matches(' ');
        if !name.is_empty() {
            return Some(name);
        }
    }
}

// The last word of `text`, words split by whitespace and by quote marks, so
// an unpaired quote (`name.mkv.001" yEnc`) is never part of the word. A
// quote escaped twice in the NZB (`&amp;quot;`) reaches here as the text
// `&quot;` once the XML is read, and is a quote mark too: a filename word
// has neither `&` nor `;` in it, so `&quot;Show.Name.mkv&quot;` names `Show.Name.mkv`.
pub(crate) fn last_word(text: &str) -> Option<&str> {
    text.rsplit(|ch: char| ch.is_whitespace() || ch == '"')
        .find_map(|word| word.rsplit(ESCAPED_QUOTE).find(|piece| !piece.is_empty()))
}

// A quote mark as an NZB escaped twice leaves it once read.
const ESCAPED_QUOTE: &str = "&quot;";

// Whether a file posted as `posted` takes the name its article header gives
// (`=ybegin ... name=`, or a uuencode `begin` line), and the name it takes,
// cleaned as a posted name is. The header name is taken when it differs,
// is not `is_probably_obfuscated`, and the posted name has no `.par2` in
// it, which is usually right. A header
// that names nothing is no name.
pub fn header_name(posted: &str, header: &str) -> Option<String> {
    if header.trim().is_empty()
        || posted.to_ascii_lowercase().contains(".par2")
        || is_probably_obfuscated(header)
    {
        return None;
    }
    let name = weaver_model::files::sanitize_download_filename(header);
    (name != posted).then_some(name)
}

// Whether a name is likely obfuscated. The name's last path component
// without its extension (as Python's `os.path.splitext` cuts it) is judged: certainly obfuscated patterns
// first, then the shapes of a clear name, and obfuscated by default. A
// name with nothing left to judge is obfuscated.
pub fn is_probably_obfuscated(name: &str) -> bool {
    let file = name.rsplit(['/', '\\']).next().unwrap_or(name);
    let base = python_splitext_stem(file);
    let lower_hex = |ch: char| matches!(ch, '0'..='9' | 'a'..='f');

    // `^[a-f0-9]{32}$`: `b082fa0beaa644d3aa01045d5b8d0b36.mkv`.
    if base.len() == 32 && base.chars().all(lower_hex) {
        return true;
    }
    // `^[a-f0-9.]{40,}$`: `0675e29e9abfd2.f7d069dab0b853283cc1b069a25f82.6547`.
    if base.len() >= 40 && base.chars().all(|ch| lower_hex(ch) || ch == '.') {
        return true;
    }
    // `[a-f0-9]{30}` with two or more `\[\w+\]`: a hash dressed in tags.
    if longest_run(base, lower_hex) >= 30 && bracketed_words(base) >= 2 {
        return true;
    }
    // `^abc\.xyz`.
    if base.starts_with("abc.xyz") {
        return true;
    }

    let decimals = base.chars().filter(|ch| ch.is_numeric()).count();
    let upperchars = base.chars().filter(|ch| ch.is_uppercase()).count();
    let lowerchars = base.chars().filter(|ch| ch.is_lowercase()).count();
    let spacesdots = base
        .chars()
        .filter(|ch| matches!(ch, ' ' | '.' | '_'))
        .count();

    // "Great Distro".
    if upperchars >= 2 && lowerchars >= 2 && spacesdots >= 1 {
        return false;
    }
    // "this is a download".
    if spacesdots >= 3 {
        return false;
    }
    // "Beast 2020".
    if upperchars + lowerchars >= 4 && decimals >= 4 && spacesdots >= 1 {
        return false;
    }
    // "Catullus": starts with a capital, and most letters are lower case.
    let Some(first) = base.chars().next() else {
        return true;
    };
    if first.is_uppercase() && lowerchars > 2 && upperchars as f64 / lowerchars as f64 <= 0.25 {
        return false;
    }
    true
}

// `name` without its extension, as Python's `os.path.splitext` cuts it: at
// the last dot, unless only dots come before it (`.bashrc` has none).
fn python_splitext_stem(name: &str) -> &str {
    match name.rfind('.') {
        Some(dot) if name[..dot].bytes().any(|byte| byte != b'.') => &name[..dot],
        _ => name,
    }
}

// The longest run of characters `of` holds.
fn longest_run(text: &str, of: impl Fn(char) -> bool) -> usize {
    let (mut run, mut longest) = (0, 0);
    for ch in text.chars() {
        run = if of(ch) { run + 1 } else { 0 };
        longest = longest.max(run);
    }
    longest
}

// How many times `\[\w+\]` matches, as Python's `re.findall` counts: each
// match starts where the one before ended, and a failed start moves on one
// character. A word character is a letter, a digit or `_`.
fn bracketed_words(text: &str) -> usize {
    let word = |ch: char| ch.is_alphanumeric() || ch == '_';
    let mut found = 0;
    let mut rest = text;
    while let Some(open) = rest.find('[') {
        let after = &rest[open + 1..];
        let inside = after.find(|ch: char| !word(ch)).unwrap_or(after.len());
        if inside > 0 && after[inside..].starts_with(']') {
            found += 1;
            rest = &after[inside + 1..];
        } else {
            rest = after;
        }
    }
    found
}

// Strategy 2: Extract filename from `[PRiVATE]` format.
//
// Format: `[PRiVATE]-[GroupName]-[PATH/TO/FILENAME]-[SEGMENTS] - "" yEnc`
// The path and segments sections can appear in either order.
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

// Strategy 3: Fallback heuristic for unquoted, non-PRiVATE subjects.
//
// Strips ` yEnc...` suffix and ` (N/N)` part info, then takes the last
// whitespace-delimited token if it contains a dot.
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

    // Take the last word. Prefer words with dots (filenames), but accept
    // dotless words as a last resort (handles "Re: A" edge cases).
    last_word(after_prefix).map(str::to_string)
}

// Check if a string looks like a segment marker (e.g. "1/10", "34/44")
// or a pure number (e.g. "24" — segment count without slash).
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

// Extract the last path component (basename) from a path string.
fn extract_basename(path: &str) -> String {
    path.rsplit_once('/')
        .or_else(|| path.rsplit_once('\\'))
        .map_or(path, |(_, name)| name)
        .to_string()
}

// Strip trailing ` (N/N)` part info from a string.
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

// Returns `true` if the filename clearly looks like a real title or release name
// rather than an obfuscated hash.
//
// Only triggers on names with word-like tokens (3+ alpha chars). Pure hex/digits
// with dots still fall through to the obfuscation checks.
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

// Returns `true` if the path is inside a DVD/Bluray structure that should
// never be renamed.
pub fn is_protected_media_structure(path: &std::path::Path) -> bool {
    let s = path.to_string_lossy();
    s.contains("/VIDEO_TS/")
        || s.contains("/AUDIO_TS/")
        || s.contains("/BDMV/")
        || s.contains("\\VIDEO_TS\\")
        || s.contains("\\AUDIO_TS\\")
        || s.contains("\\BDMV\\")
}

// Returns `true` if any component of `relative_path` names a disc-structure
// directory.
//
// [`is_protected_media_structure`] answers the same question for an absolute
// path, where the directory name is always flanked by separators. A delivery
// is enumerated relative to its own root, so its first component has no
// leading separator and a one-file `BDMV/index.bdmv` delivery would slip past
// the substring form.
pub fn contains_protected_media_structure(relative_path: &str) -> bool {
    relative_path.split(['/', '\\']).any(|component| {
        PROTECTED_STRUCTURE_DIRS
            .iter()
            .any(|dir| component.eq_ignore_ascii_case(dir))
    })
}

const PROTECTED_STRUCTURE_DIRS: &[&str] = &["VIDEO_TS", "AUDIO_TS", "BDMV"];

// Strip known archive extensions to get the filename stem for obfuscation checks.
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
        // Empty quotes are no name, and the quotes are no word either: the
        // text before the yEnc marker names nothing.
        assert_eq!(extract_filename(r#"[1/1] - "" yEnc some.file.rar"#), None);
    }

    // A quote mark is never part of a posted name. Each row: the subject,
    // the name, and the confidence. The quoted rows read the first closed
    // pair, the rest the last word.
    const QUOTE_SHAPES: &[(&str, &str, f64)] = &[
        (r#""name.ext" yEnc (1/5)"#, "name.ext", 1.0),
        (r#"[1/5] - "name.ext" yEnc (1/5)"#, "name.ext", 1.0),
        // The shape of job f586b14d13974344: the opening quote is missing.
        (
            r#"Show.S01E01.1080p.WEB.H264-GRP.[71ECB6A7].mkv.001" yEnc (1/50)"#,
            "Show.S01E01.1080p.WEB.H264-GRP.[71ECB6A7].mkv.001",
            0.5,
        ),
        (r#"name.mkv.001" yEnc"#, "name.mkv.001", 0.5),
        (r#"[1/5] - name.mkv.001" yEnc (1/5)"#, "name.mkv.001", 0.5),
        // The closing quote is missing.
        (r#"[1/5] - "name.mkv.001 yEnc (1/5)"#, "name.mkv.001", 0.5),
        (r#""name.mkv.001 yEnc (1/5)"#, "name.mkv.001", 0.5),
        // A doubled closing quote.
        (r#"[1/5] - "name.mkv.001"" yEnc (1/5)"#, "name.mkv.001", 1.0),
        // Spaces inside the quotes are trimmed.
        (r#"[1/5] - " name.mkv " yEnc (1/5)"#, "name.mkv", 1.0),
        // An empty or blank pair is passed over for the next one.
        (r#""" - "name.mkv" yEnc (1/5)"#, "name.mkv", 1.0),
        (r#"" " - "name.mkv" yEnc (1/5)"#, "name.mkv", 1.0),
        // Quotes inside the name end it at the first pair.
        (r#"[1/5] - "He said "hi".mkv" yEnc (1/5)"#, "He said", 1.0),
        // A quote the NZB escaped twice (`&amp;quot;`) is left as the text
        // `&quot;` and is a quote mark too.
        (
            r#"&quot;Show.Name.mkv&quot; yEnc (1/50)"#,
            "Show.Name.mkv",
            0.5,
        ),
        (
            r#"[1/5] - &quot;Show.Name.mkv&quot; yEnc (1/5)"#,
            "Show.Name.mkv",
            0.5,
        ),
        (r#"Show.Name.mkv&quot; yEnc (1/5)"#, "Show.Name.mkv", 0.5),
        (
            r#"&quot;&quot; - Show.Name.mkv yEnc (1/5)"#,
            "Show.Name.mkv",
            0.5,
        ),
        // A closed pair of real quotes keeps what it holds.
        (r#""a&quot;b.mkv" yEnc (1/5)"#, "a&quot;b.mkv", 1.0),
    ];

    // The subject of an NZB that escaped its quotes twice, read through the
    // parser, names the file without them.
    #[test]
    fn a_quote_escaped_twice_in_the_nzb_is_no_part_of_the_name() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="p" date="1" subject="&amp;quot;Show.Name.mkv&amp;quot; yEnc (1/50)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="100" number="1">a@b</segment></segments>
  </file>
</nzb>"#;
        let nzb = crate::parse_nzb(xml).unwrap();
        let file = &nzb.files[0];
        assert_eq!(file.subject, "&quot;Show.Name.mkv&quot; yEnc (1/50)");
        assert_eq!(
            file.extract_filename(),
            Some(("Show.Name.mkv".to_string(), 0.5))
        );
        assert_eq!(file.filename(), Some("Show.Name.mkv"));
    }

    // ── is_probably_obfuscated: names in the field, and the header name
    // of the ruling's counter-example.

    #[test]
    fn probably_obfuscated_judges_field_names() {
        for name in [
            "599c1c9e2bdfb5114044bf25152b7eaa.mkv",
            "/my/blabla/directory/stuff/599c1c9e2bdfb5114044bf25152b7eaa.mkv",
            "/my/blabla/directory/A Directory Should Not Count 2020/599c1c9e2bdfb5114044bf25152b7eaa.mkv",
            "/my/blabla/directory/stuff/afgm.avi",
            "/my/blabla/directory/stuff/afgm2020.avi",
            "MUGNjK3zi65TtN.mkv",
            "T306077.avi",
            "bar10nmbkkjjdfr.mkv",
            "4rFF-fdtd480p.bin",
            "e0nFmxBNTprpbQiVQ44WeEwSrBkLlJ7IgaSj3uzFu455FVYG3q.bin",
            "e0nFmxBNTprpbQiVQ44WeEwSrBkLlJ7IgaSj3uzFu455FVYG3q",
            "greatdistro.iso",
            "my.download.2020",
            "abc.xyz.a4c567edbcbf27.BLA",
            "abc.xyz.iso",
            "0675e29e9abfd2.f7d069dab0b853283cc1b069a25f82.6547",
            "[BlaBla] something [More] something b2.bef89a622e4a23f07b0d3757ad5e8a.a0 [Brrr]",
            "b082fa0beaa644d3aa01045d5b8d0b36.mkv",
            // Nothing to judge.
            "",
        ] {
            assert!(is_probably_obfuscated(name), "{name:?}");
        }
        for name in [
            "/my/blabla/directory/stuff/My Favorite Distro S03E04.iso",
            "/my/blabla/directory/stuff/Great Distro (2020).iso",
            "ubuntu.2004.iso",
            "/my/blabla/directory/stuff/GreatDistro2020.iso",
            "Catullus.avi",
            "Der.Mechaniker.HDRip.XviD-SG.avi",
            "Bonjour.1969.FRENCH.BRRiP.XviD.AC3-HuSh.avi",
            "Bonjour.1969.avi",
            "This That S01E11",
            "This_That_S01E11",
            "this_that_S01E11",
            "My.Download.2020",
            "this_that_there_here.avi",
            "Lorem Ipsum.avi",
            "Lorem Ipsum",
            "Show.Name.S01E01.1080p.WEB.h264-GRP.mkv.001",
        ] {
            assert!(!is_probably_obfuscated(name), "{name:?}");
        }
    }

    #[test]
    fn probably_obfuscated_counts_bracketed_words_as_findall_does() {
        assert_eq!(bracketed_words("[a] [b]"), 2);
        assert_eq!(bracketed_words("[ab[cd]"), 1);
        assert_eq!(bracketed_words("[] [x y] [ok_1]"), 1);
        assert_eq!(bracketed_words("[[a]]"), 1);
        // 30 lowercase hex and two tags; and with one tag, no rule fires.
        let hex = "5937bc5e32146ebef89a622e4a23f0";
        assert!(is_probably_obfuscated(&format!("[A] [B] {hex}")));
        assert_eq!(python_splitext_stem("a.b.c"), "a.b");
        assert_eq!(python_splitext_stem("..a"), "..a");
        assert_eq!(python_splitext_stem("a."), "a");
    }

    // ── header_name

    #[test]
    fn a_file_takes_its_header_name_by_the_rules() {
        // A clear name that differs is taken.
        assert_eq!(
            header_name(
                "[71ECB6A7].mkv.001",
                "Show.Name.S01E01.1080p.WEB.h264-GRP.mkv.001"
            ),
            Some("Show.Name.S01E01.1080p.WEB.h264-GRP.mkv.001".to_string())
        );
        // An obfuscated header name is not.
        assert_eq!(
            header_name("Show Name.mkv", "b082fa0beaa644d3aa01045d5b8d0b36.mkv"),
            None
        );
        // Nor one the file already carries.
        assert_eq!(header_name("Great Distro.iso", "Great Distro.iso"), None);
        // A posted PAR2 name is kept, whatever the header says.
        assert_eq!(header_name("lantern.par2", "Great Distro.par2"), None);
        assert_eq!(
            header_name("lantern.vol00+01.PAR2", "Great Distro.iso"),
            None
        );
        // A header that names nothing is no name.
        assert_eq!(header_name("a.bin", ""), None);
        assert_eq!(header_name("a.bin", "   "), None);
        // The header name is cleaned as a posted name is: no directory.
        assert_eq!(
            header_name("a.bin", "../Great Distro.iso"),
            Some(".._Great Distro.iso".to_string())
        );
    }

    #[test]
    fn a_quote_mark_is_never_part_of_a_posted_name() {
        for &(subject, name, confidence) in QUOTE_SHAPES {
            assert_eq!(
                extract_filename(subject),
                Some((name.to_string(), confidence)),
                "{subject}"
            );
            // The quick name a file's role is read from agrees.
            let file = crate::NzbFile {
                poster: String::new(),
                subject: subject.to_string(),
                date: 0,
                groups: Vec::new(),
                segments: Vec::new(),
            };
            assert_eq!(file.filename(), Some(name), "{subject}");
        }
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

    // ── Obfuscation patterns

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
    fn backup_pattern_takes_multibyte_characters() {
        // A character of 2, 3 or 4 bytes where the pattern expects a digit
        // or the dash is no match, at every position.
        for name in [
            "Backup_12345S1é-02",
            "Backup_12345S12-1é",
            "Backup_12345S1€-02",
            "Backup_12345S12-1€",
            "Backup_12345S12€02",
            "Backup_12345S𝄞2-02",
            "Backup_12345S12-𝄞",
            "Backup_12345Sé",
        ] {
            assert!(!is_obfuscated(name), "{name}");
        }
        assert!(is_obfuscated("Backup_12345S01-02é"));
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

    // ── Clear naming and mixed-token heuristics ────────────────────────

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
