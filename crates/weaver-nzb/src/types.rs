use serde::{Deserialize, Serialize};
use weaver_core::classify::FileRole;

/// A parsed NZB document representing a complete download job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Nzb {
    pub meta: NzbMeta,
    pub files: Vec<NzbFile>,
}

/// NZB metadata from the `<head>` section.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NzbMeta {
    pub title: Option<String>,
    pub password: Option<String>,
    /// Arbitrary key-value pairs from `<meta>` elements (excluding title/password).
    pub tags: Vec<(String, String)>,
}

/// A single file in the NZB (one `<file>` element).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbFile {
    pub poster: String,
    pub subject: String,
    pub date: u64,
    pub groups: Vec<String>,
    pub segments: Vec<NzbSegment>,
}

/// A single segment/article within a file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbSegment {
    /// 1-based segment number.
    pub number: u32,
    /// Expected size in bytes.
    pub bytes: u32,
    /// Message-ID without angle brackets.
    pub message_id: String,
}

impl NzbFile {
    /// Extract filename from the subject line.
    ///
    /// Usenet subjects follow patterns like:
    ///   `"Some Post Title - [01/10] - \"filename.rar\" yEnc (1/5)"`
    ///   `"[PRiVATE] Some.Title - \"file.part01.rar\" yEnc (1/50)"`
    ///   `"filename.nfo (1/1)"`
    ///
    /// The filename is typically in double quotes. Falls back to the last word
    /// before a yEnc marker or parenthesized part number.
    pub fn filename(&self) -> Option<&str> {
        let subject = &self.subject;

        // Strategy 1: extract from double-quoted string (most common Usenet pattern).
        if let Some(start) = subject.find('"')
            && let Some(end) = subject[start + 1..].find('"')
        {
            let name = &subject[start + 1..start + 1 + end];
            if !name.is_empty() {
                return Some(name);
            }
        }

        // Strategy 2: grab the last filename-like token before yEnc or (N/N).
        // Strip trailing " yEnc ..." or " (N/N)" and take the last whitespace-delimited token.
        let trimmed = subject
            .find(" yEnc")
            .map(|i| &subject[..i])
            .unwrap_or(subject);

        // Also strip trailing parenthesized part info like " (1/1)"
        let trimmed = trim_trailing_part_info(trimmed);

        let trimmed = trimmed.trim_end();
        if trimmed.is_empty() {
            return None;
        }

        // Take the last whitespace-delimited token if it looks like a filename (has a dot).
        let last_token = trimmed
            .rsplit_once(char::is_whitespace)
            .map_or(trimmed, |(_, t)| t);
        if last_token.contains('.') && !last_token.is_empty() {
            return Some(last_token);
        }

        None
    }

    /// Total expected bytes across all segments.
    pub fn total_bytes(&self) -> u64 {
        self.segments.iter().map(|s| u64::from(s.bytes)).sum()
    }

    /// Infer file role from the filename.
    pub fn role(&self) -> FileRole {
        match self.filename() {
            Some(name) => FileRole::from_filename(name),
            None => FileRole::Unknown,
        }
    }
}

/// Strip a trailing ` (N/N)` pattern from a string.
fn trim_trailing_part_info(s: &str) -> &str {
    let s = s.trim_end();
    if let Some(open) = s.rfind('(') {
        let inside = &s[open + 1..];
        if let Some(close) = inside.find(')') {
            let content = &inside[..close];
            // Check if it matches N/N pattern
            if content.contains('/') && content.chars().all(|c| c.is_ascii_digit() || c == '/') {
                return s[..open].trim_end();
            }
        }
    }
    s
}
