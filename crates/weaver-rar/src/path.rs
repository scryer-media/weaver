//! Path sanitization for archive member names.
//!
//! Prevents path traversal attacks by stripping dangerous components
//! from file paths extracted from RAR archives.

/// Sanitize a file path from an archive to prevent path traversal.
///
/// - Strip leading `/` and `\`
/// - Remove `../` and `..\` components
/// - Strip Windows drive letters (e.g., `C:`)
/// - Collapse `./` to empty
/// - Remove null bytes
/// - Convert backslashes to forward slashes
/// - Collapse multiple consecutive slashes
pub fn sanitize_path(raw: &str) -> String {
    // Remove null bytes first.
    let cleaned: String = raw.chars().filter(|&c| c != '\0').collect();

    // Convert backslashes to forward slashes.
    let normalized = cleaned.replace('\\', "/");

    // Split into components and filter dangerous ones.
    let mut parts: Vec<&str> = Vec::new();

    for component in normalized.split('/') {
        match component {
            // Skip empty components (from leading/multiple slashes).
            "" => continue,
            // Skip parent directory traversal.
            ".." => continue,
            // Skip current directory references.
            "." => continue,
            other => {
                // Strip Windows drive letter (e.g., "C:" or "D:") — only for
                // the very first real component.
                if parts.is_empty() && is_drive_letter(other) {
                    continue;
                }
                parts.push(other);
            }
        }
    }

    parts.join("/")
}

/// Validate that a symlink or hardlink target is safe.
///
/// Returns `true` if the target path does not escape the extraction root.
/// A target is unsafe if it contains enough `..` components to traverse above
/// the member's directory, contains absolute paths, or has drive letters.
///
/// `member_path` is the sanitized path of the symlink member itself.
/// `target` is the raw link target from the archive.
pub fn is_safe_link_target(member_path: &str, target: &str) -> bool {
    // Reject absolute paths.
    let target = target.replace('\\', "/");
    if target.starts_with('/') {
        return false;
    }

    // Reject drive letters.
    let bytes = target.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return false;
    }

    // Reject null bytes.
    if target.contains('\0') {
        return false;
    }

    // Count the directory depth of the member's parent directory.
    let member_depth = member_path.split('/').filter(|s| !s.is_empty()).count();
    // The member itself is a file, so its parent has depth - 1.
    let parent_depth = if member_depth > 0 { member_depth - 1 } else { 0 };

    // Walk the target path and track depth relative to the member's parent.
    let mut depth: i64 = parent_depth as i64;
    for component in target.split('/') {
        match component {
            "" | "." => continue,
            ".." => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {
                depth += 1;
            }
        }
    }

    true
}

/// Check if a path component is a Windows drive letter like `C:` or `D:`.
fn is_drive_letter(s: &str) -> bool {
    let bytes = s.as_bytes();
    bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_path_unchanged() {
        assert_eq!(sanitize_path("foo/bar/baz.txt"), "foo/bar/baz.txt");
    }

    #[test]
    fn already_clean_path_unchanged() {
        assert_eq!(sanitize_path("documents/report.pdf"), "documents/report.pdf");
    }

    #[test]
    fn leading_slash_stripped() {
        assert_eq!(sanitize_path("/etc/passwd"), "etc/passwd");
    }

    #[test]
    fn leading_backslash_stripped() {
        assert_eq!(sanitize_path("\\Windows\\system32\\cmd.exe"), "Windows/system32/cmd.exe");
    }

    #[test]
    fn dot_dot_slash_removed() {
        assert_eq!(sanitize_path("foo/../bar.txt"), "foo/bar.txt");
    }

    #[test]
    fn dot_dot_backslash_removed() {
        assert_eq!(sanitize_path("foo\\..\\bar.txt"), "foo/bar.txt");
    }

    #[test]
    fn traversal_etc_passwd() {
        assert_eq!(sanitize_path("../../etc/passwd"), "etc/passwd");
    }

    #[test]
    fn windows_drive_letter_stripped() {
        assert_eq!(sanitize_path("C:\\Windows\\file.txt"), "Windows/file.txt");
    }

    #[test]
    fn lowercase_drive_letter_stripped() {
        assert_eq!(sanitize_path("c:/users/file.txt"), "users/file.txt");
    }

    #[test]
    fn null_bytes_removed() {
        assert_eq!(sanitize_path("foo\0bar.txt"), "foobar.txt");
    }

    #[test]
    fn dot_slash_collapsed() {
        assert_eq!(sanitize_path("./foo"), "foo");
    }

    #[test]
    fn multiple_slashes_collapsed() {
        assert_eq!(sanitize_path("foo///bar//baz.txt"), "foo/bar/baz.txt");
    }

    #[test]
    fn mixed_separators_normalized() {
        assert_eq!(sanitize_path("foo\\bar/baz\\qux.txt"), "foo/bar/baz/qux.txt");
    }

    #[test]
    fn empty_string_stays_empty() {
        assert_eq!(sanitize_path(""), "");
    }

    #[test]
    fn only_traversal_components() {
        assert_eq!(sanitize_path("../../../.."), "");
    }

    #[test]
    fn drive_letter_mid_path_kept() {
        // A drive-letter-like component in the middle is NOT stripped.
        assert_eq!(sanitize_path("foo/C:/bar.txt"), "foo/C:/bar.txt");
    }

    #[test]
    fn multiple_leading_slashes() {
        assert_eq!(sanitize_path("///foo/bar"), "foo/bar");
    }

    #[test]
    fn complex_traversal() {
        assert_eq!(
            sanitize_path("/../.././foo/../bar/./baz"),
            "foo/bar/baz"
        );
    }

    // Link target validation tests

    #[test]
    fn safe_relative_link() {
        assert!(is_safe_link_target("dir/link", "target.txt"));
    }

    #[test]
    fn safe_sibling_link() {
        assert!(is_safe_link_target("dir/link", "../other/file.txt"));
    }

    #[test]
    fn unsafe_absolute_link() {
        assert!(!is_safe_link_target("dir/link", "/etc/passwd"));
    }

    #[test]
    fn unsafe_traversal_link() {
        // dir/link -> ../../escape — goes above root
        assert!(!is_safe_link_target("dir/link", "../../escape"));
    }

    #[test]
    fn safe_deep_relative() {
        // a/b/c/link -> ../../../still_in_root
        assert!(is_safe_link_target("a/b/c/link", "../../../still_in_root"));
    }

    #[test]
    fn unsafe_deep_traversal() {
        // a/b/c/link -> ../../../../escape
        assert!(!is_safe_link_target("a/b/c/link", "../../../../escape"));
    }

    #[test]
    fn unsafe_drive_letter_link() {
        assert!(!is_safe_link_target("link", "C:\\Windows\\system32"));
    }

    #[test]
    fn unsafe_null_byte_link() {
        assert!(!is_safe_link_target("link", "foo\0bar"));
    }

    #[test]
    fn safe_same_dir_link() {
        assert!(is_safe_link_target("link", "other_file"));
    }

    #[test]
    fn unsafe_root_level_dotdot() {
        // link at root -> ../escape
        assert!(!is_safe_link_target("link", "../escape"));
    }
}
