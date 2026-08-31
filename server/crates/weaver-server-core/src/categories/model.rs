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

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("category must be a single safe filesystem name")]
pub struct CategoryValidationError;

pub fn validate_category_path_component(input: &str) -> Result<String, CategoryValidationError> {
    let trimmed = input.trim();
    if trimmed.is_empty() || weaver_model::files::sanitize_path_component(trimmed) != trimmed {
        return Err(CategoryValidationError);
    }
    Ok(trimmed.to_string())
}

/// Resolve a category string to a canonical category name.
///
/// 1. Exact case-insensitive match on category names.
/// 2. Glob match against comma-separated aliases.
///
/// Returns the canonical name of the first match, or `None`.
pub fn resolve_category(categories: &[CategoryConfig], input: &str) -> Option<String> {
    resolve_category_config(categories, input).map(|category| category.name.clone())
}

pub(crate) fn resolve_category_config<'a>(
    categories: &'a [CategoryConfig],
    input: &str,
) -> Option<&'a CategoryConfig> {
    let input_lower = input.trim();
    if input_lower.is_empty() {
        return None;
    }

    // Exact name match (case-insensitive).
    for cat in categories {
        if cat.name.eq_ignore_ascii_case(input_lower) {
            return Some(cat);
        }
    }

    // Alias glob match.
    for cat in categories {
        for alias in cat.aliases.split(',') {
            let alias = alias.trim();
            if !alias.is_empty() && crate::runtime::glob::glob_match_ci(alias, input_lower) {
                return Some(cat);
            }
        }
    }

    None
}

pub fn resolve_submission_category(
    categories: &[CategoryConfig],
    input: Option<&str>,
) -> Result<Option<String>, CategoryValidationError> {
    let Some(input) = input else {
        return Ok(None);
    };
    let input = input.trim();
    if input.is_empty() {
        return Ok(None);
    }

    if let Some(configured) = resolve_category_config(categories, input) {
        if configured.dest_dir.as_deref().is_none_or(str::is_empty) {
            validate_category_path_component(&configured.name)?;
        }
        return Ok(Some(configured.name.clone()));
    }

    validate_category_path_component(input).map(Some)
}

/// Resolve the configured completion parent for a category.
///
/// Explicit destination overrides are trusted administrator input. Categories
/// without an override remain constrained to a single safe path component
/// beneath `complete_dir`.
pub fn completion_parent(
    complete_dir: &std::path::Path,
    categories: &[CategoryConfig],
    category: Option<&str>,
) -> Result<std::path::PathBuf, String> {
    let Some(category) = category.filter(|category| !category.is_empty()) else {
        return Ok(complete_dir.to_path_buf());
    };

    if let Some(custom_dest) = categories
        .iter()
        .find(|configured| configured.name.eq_ignore_ascii_case(category))
        .and_then(|configured| configured.dest_dir.as_deref())
        .filter(|destination| !destination.is_empty())
    {
        return Ok(std::path::PathBuf::from(custom_dest));
    }

    let category = validate_category_path_component(category)
        .map_err(|error| format!("unsafe completion category: {error}"))?;
    let parent = complete_dir.join(category);
    if !parent.starts_with(complete_dir) {
        return Err("unsafe completion category escaped the complete directory".to_string());
    }
    Ok(parent)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_path_component_validation_is_cross_platform_and_lossless() {
        for (input, expected) in [
            ("movies", "movies"),
            ("TV HD", "TV HD"),
            ("日本語", "日本語"),
            ("movies-4k", "movies-4k"),
            (" .hidden ", ".hidden"),
            (" trailing ", "trailing"),
        ] {
            assert_eq!(
                validate_category_path_component(input).as_deref(),
                Ok(expected),
                "rejected {input:?}"
            );
        }

        for input in [
            "",
            "   ",
            "/tmp",
            "../../outside",
            "movies/4k",
            "movies\\4k",
            ".",
            "..",
            "C:",
            "C:\\outside",
            "\\\\server\\share",
            "CON",
            "NUL.txt",
            "trailing.",
            "bad*name",
            "bad\0name",
        ] {
            assert!(
                validate_category_path_component(input).is_err(),
                "accepted {input:?}"
            );
        }

        assert!(validate_category_path_component(&"a".repeat(241)).is_err());
    }

    #[test]
    fn configured_category_resolution_returns_the_canonical_record() {
        let categories = vec![CategoryConfig {
            id: 1,
            name: "Movies".to_string(),
            dest_dir: None,
            aliases: "film*, cinema".to_string(),
        }];

        assert_eq!(
            resolve_category(&categories, "movies").as_deref(),
            Some("Movies")
        );
        assert_eq!(
            resolve_category(&categories, "FILM-4K").as_deref(),
            Some("Movies")
        );
        assert_eq!(
            resolve_category(&categories, "cinema").as_deref(),
            Some("Movies")
        );
        assert_eq!(resolve_category(&categories, "tv"), None);
    }

    #[test]
    fn submission_resolution_canonicalizes_and_rejects_unsafe_fallbacks() {
        let categories = vec![
            CategoryConfig {
                id: 1,
                name: "Movies".to_string(),
                dest_dir: None,
                aliases: "film*".to_string(),
            },
            CategoryConfig {
                id: 2,
                name: "custom/name".to_string(),
                dest_dir: Some("/trusted/output".to_string()),
                aliases: "custom-safe-alias".to_string(),
            },
        ];

        assert_eq!(
            resolve_submission_category(&categories, Some("FILM-4K"))
                .unwrap()
                .as_deref(),
            Some("Movies")
        );
        assert_eq!(
            resolve_submission_category(&categories, Some(" unknown-safe "))
                .unwrap()
                .as_deref(),
            Some("unknown-safe")
        );
        assert_eq!(
            resolve_submission_category(&categories, Some("custom-safe-alias"))
                .unwrap()
                .as_deref(),
            Some("custom/name")
        );
        assert_eq!(
            resolve_submission_category(&categories, Some("   ")).unwrap(),
            None
        );
        assert!(resolve_submission_category(&categories, Some("../../outside")).is_err());

        let unsafe_without_override = vec![CategoryConfig {
            id: 3,
            name: "unsafe/name".to_string(),
            dest_dir: None,
            aliases: "unsafe-alias".to_string(),
        }];
        assert!(
            resolve_submission_category(&unsafe_without_override, Some("unsafe-alias")).is_err()
        );
    }
}
