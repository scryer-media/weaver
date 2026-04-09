use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryConfig {
    /// Stable identifier for CRUD operations.
    #[serde(default)]
    pub id: u32,
    /// Unique category name (canonical form).
    pub name: String,
    /// Optional destination directory override. If absent, uses
    /// `{complete_dir}/{name}/` as the default.
    #[serde(default)]
    pub dest_dir: Option<String>,
    /// Comma-separated aliases for matching from RSS/URL/API submissions.
    /// Supports glob-style wildcards (`*` and `?`).
    #[serde(default)]
    pub aliases: String,
}

/// Resolve a category string to a canonical category name.
///
/// 1. Exact case-insensitive match on category names.
/// 2. Glob match against comma-separated aliases.
///
/// Returns the canonical name of the first match, or `None`.
pub fn resolve_category(categories: &[CategoryConfig], input: &str) -> Option<String> {
    let input_lower = input.trim();
    if input_lower.is_empty() {
        return None;
    }

    // Exact name match (case-insensitive).
    for cat in categories {
        if cat.name.eq_ignore_ascii_case(input_lower) {
            return Some(cat.name.clone());
        }
    }

    // Alias glob match.
    for cat in categories {
        for alias in cat.aliases.split(',') {
            let alias = alias.trim();
            if !alias.is_empty() && glob_match_ci(alias, input_lower) {
                return Some(cat.name.clone());
            }
        }
    }

    None
}

/// Simple case-insensitive glob matcher supporting `*` (any sequence) and `?` (any single char).
fn glob_match_ci(pattern: &str, input: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let s: Vec<char> = input.chars().collect();
    let (mut pi, mut si) = (0, 0);
    let (mut star_pi, mut star_si) = (usize::MAX, 0);

    while si < s.len() {
        if pi < p.len() && p[pi] == '*' {
            star_pi = pi;
            star_si = si;
            pi += 1;
        } else if pi < p.len() && (p[pi] == '?' || p[pi].eq_ignore_ascii_case(&s[si])) {
            pi += 1;
            si += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }

    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}
