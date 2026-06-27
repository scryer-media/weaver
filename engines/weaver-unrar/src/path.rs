//! Path sanitization for archive member names.
//!
//! Prevents path traversal attacks by stripping dangerous components
//! from file paths extracted from RAR archives.

/// Sanitize a file path from an archive to prevent path traversal.
///
/// - Strip leading `/` and `\`
/// - Cut to the suffix after the last `../` or `..\` traversal
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

    let start = unrar_convert_path_start(&normalized);
    let suffix = &normalized[start..];

    let mut parts: Vec<&str> = Vec::new();

    for component in suffix.split('/') {
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

fn unrar_convert_path_start(path: &str) -> usize {
    let bytes = path.as_bytes();
    let mut dest_pos = 0usize;

    // Match UnRAR ConvertPath: if a path contains /../ or ends with /..,
    // keep only the suffix after the last such traversal sequence.
    for i in 0..bytes.len() {
        if bytes[i] == b'/'
            && i + 2 < bytes.len()
            && bytes[i + 1] == b'.'
            && bytes[i + 2] == b'.'
            && (i + 3 == bytes.len() || bytes[i + 3] == b'/')
        {
            dest_pos = if i + 3 == bytes.len() { i + 3 } else { i + 4 };
        }
    }

    // Then repeatedly strip leading drive specs, UNC roots, and dot/slash
    // prefixes from that suffix, mirroring pathfn.cpp ConvertPath.
    while dest_pos < bytes.len() {
        let mut i = dest_pos;

        if i + 1 < bytes.len() && bytes[i + 1] == b':' {
            i += 2;
        }

        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            let mut slash_count = 0u32;
            for (j, byte) in bytes.iter().enumerate().skip(i + 2) {
                if *byte == b'/' {
                    slash_count += 1;
                    if slash_count == 2 {
                        i = j + 1;
                        break;
                    }
                }
            }
        }

        for (j, byte) in bytes.iter().enumerate().skip(i) {
            if *byte == b'/' {
                i = j + 1;
            } else if *byte != b'.' {
                break;
            }
        }

        if i == dest_pos {
            break;
        }
        dest_pos = i;
    }

    dest_pos
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
    is_safe_link_target_inner(member_path, target, false)
}

pub(crate) fn is_safe_symlink_target_for_member(
    member_path: &str,
    raw_member_path: &str,
    target: &str,
    allow_windows_drive_relative: bool,
) -> bool {
    is_safe_link_target_inner(member_path, target, allow_windows_drive_relative)
        && is_safe_link_target_inner(raw_member_path, target, allow_windows_drive_relative)
}

fn is_safe_link_target_inner(
    member_path: &str,
    target: &str,
    allow_windows_drive_relative: bool,
) -> bool {
    let member_path = member_path.replace('\\', "/");
    if is_full_root_path(&member_path) {
        return false;
    }

    // Reject absolute paths.
    let target = target.replace('\\', "/");
    if target.starts_with('/') {
        return false;
    }

    // Reject drive letters.
    let bytes = target.as_bytes();
    if !allow_windows_drive_relative
        && bytes.len() >= 2
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
    {
        return false;
    }

    // Reject null bytes.
    if target.contains('\0') {
        return false;
    }

    target_parent_traversal_count(&target) <= allowed_link_source_depth(&member_path)
}

fn is_full_root_path(path: &str) -> bool {
    path.starts_with('/')
        || (path.as_bytes().len() >= 2
            && path.as_bytes()[0].is_ascii_alphabetic()
            && path.as_bytes()[1] == b':')
}

fn allowed_link_source_depth(member_path: &str) -> usize {
    let bytes = member_path.as_bytes();
    let mut depth = 0i32;
    for i in 0..bytes.len() {
        if bytes[i] != b'/' {
            continue;
        }
        let dot = i + 1 < bytes.len()
            && bytes[i + 1] == b'.'
            && (i + 2 == bytes.len() || bytes[i + 2] == b'/');
        let dot2 = i + 2 < bytes.len()
            && bytes[i + 1] == b'.'
            && bytes[i + 2] == b'.'
            && (i + 3 == bytes.len() || bytes[i + 3] == b'/');
        if dot2 {
            depth -= 1;
        } else if !dot {
            depth += 1;
        }
    }
    depth.max(0) as usize
}

fn target_parent_traversal_count(target: &str) -> usize {
    target
        .split('/')
        .filter(|component| *component == "..")
        .count()
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
        assert_eq!(
            sanitize_path("documents/report.pdf"),
            "documents/report.pdf"
        );
    }

    #[test]
    fn leading_slash_stripped() {
        assert_eq!(sanitize_path("/etc/passwd"), "etc/passwd");
    }

    #[test]
    fn leading_backslash_stripped() {
        assert_eq!(
            sanitize_path("\\Windows\\system32\\cmd.exe"),
            "Windows/system32/cmd.exe"
        );
    }

    #[test]
    fn dot_dot_slash_cuts_to_suffix_like_unrar() {
        assert_eq!(sanitize_path("foo/../bar.txt"), "bar.txt");
    }

    #[test]
    fn dot_dot_backslash_cuts_to_suffix_like_unrar() {
        assert_eq!(sanitize_path("foo\\..\\bar.txt"), "bar.txt");
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
        assert_eq!(
            sanitize_path("foo\\bar/baz\\qux.txt"),
            "foo/bar/baz/qux.txt"
        );
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
        assert_eq!(sanitize_path("///foo/bar"), "bar");
    }

    #[test]
    fn complex_traversal() {
        assert_eq!(sanitize_path("/../.././foo/../bar/./baz"), "bar/baz");
    }

    #[test]
    fn repeated_internal_traversal_uses_last_suffix_like_unrar() {
        assert_eq!(sanitize_path("foo/../../bar/baz.txt"), "bar/baz.txt");
        assert_eq!(sanitize_path("a/../b/../c.txt"), "c.txt");
    }

    #[test]
    fn trailing_internal_traversal_removes_everything_like_unrar() {
        assert_eq!(sanitize_path("foo/bar/.."), "");
    }

    #[test]
    fn unc_prefix_stripped_like_unrar() {
        assert_eq!(sanitize_path("//server/share/dir/file.txt"), "dir/file.txt");
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
    fn safe_windows_drive_relative_symlink_when_explicitly_allowed() {
        assert!(is_safe_symlink_target_for_member(
            "link",
            "link",
            "C:\\Windows\\system32",
            true
        ));
        assert!(is_safe_symlink_target_for_member(
            "link",
            "link",
            "C:/Windows/system32",
            true
        ));
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

    #[test]
    fn unsafe_when_target_has_too_many_total_dotdots_like_unrar() {
        assert!(!is_safe_link_target("dir/link", "sub/../../escape"));
    }

    #[test]
    fn unsafe_when_raw_member_name_has_less_allowed_depth_like_unrar() {
        assert!(!is_safe_symlink_target_for_member(
            "bar/link",
            "foo/../../bar/link",
            "../escape",
            false
        ));
    }

    #[test]
    fn unsafe_when_raw_member_name_is_rooted_like_unrar() {
        assert!(!is_safe_symlink_target_for_member(
            "dir/link",
            "/dir/link",
            "../other",
            false
        ));
        assert!(!is_safe_symlink_target_for_member(
            "dir/link",
            "C:/dir/link",
            "../other",
            false
        ));
    }
}
