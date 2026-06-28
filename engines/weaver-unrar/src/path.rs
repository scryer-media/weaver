//! Path sanitization for archive member names.
//!
//! Prevents path traversal attacks by stripping dangerous components
//! from file paths extracted from RAR archives.

use crate::types::{ArchiveFormat, HostOs};
use unicode_normalization::UnicodeNormalization;

/// Sanitize a file path from an archive to prevent path traversal.
///
/// - Strip leading `/` and `\`
/// - Cut to the suffix after the last `../` or `..\` traversal
/// - Strip Windows drive letters (e.g., `C:`)
/// - Strip leading dot/slash prefixes
/// - Truncate at the first null byte
/// - Convert backslashes to forward slashes
pub fn sanitize_path(raw: &str) -> String {
    let cleaned = raw.split_once('\0').map_or(raw, |(prefix, _)| prefix);

    // Convert backslashes to forward slashes.
    let normalized = cleaned.replace('\\', "/");

    let start = unrar_convert_path_start(&normalized, true);
    normalized[start..].to_string()
}

pub(crate) fn sanitize_file_redirection_path(raw: &str) -> String {
    let cleaned = raw.split_once('\0').map_or(raw, |(prefix, _)| prefix);
    let normalized = cleaned.replace('\\', "/");

    let start = unrar_convert_path_start(&normalized, cfg!(windows));
    normalized[start..].to_string()
}

pub(crate) fn sanitize_member_path(format: ArchiveFormat, host_os: HostOs, raw: &str) -> String {
    sanitize_member_path_for_target(format, host_os, raw, cfg!(windows))
}

fn sanitize_member_path_for_target(
    format: ArchiveFormat,
    host_os: HostOs,
    raw: &str,
    target_is_windows: bool,
) -> String {
    let cleaned = raw.split_once('\0').map_or(raw, |(prefix, _)| prefix);
    let converted = match format {
        ArchiveFormat::Rar5 if target_is_windows || matches!(host_os, HostOs::Windows) => {
            cleaned.replace('\\', "_")
        }
        ArchiveFormat::Rar5 => cleaned.to_string(),
        ArchiveFormat::Rar4 | ArchiveFormat::Rar14 => cleaned.replace('\\', "/"),
    };

    let start = unrar_convert_path_start(&converted, target_is_windows);
    let mut sanitized = converted[start..].to_string();
    if target_is_windows {
        sanitized = sanitized.replace(':', "_");
        if matches!(host_os, HostOs::Unix | HostOs::Darwin) {
            sanitized = sanitized.nfc().collect();
        }
        sanitized = make_windows_member_name_compatible(&sanitized);
    }
    sanitized
}

fn make_windows_member_name_compatible(path: &str) -> String {
    let mut converted = String::with_capacity(path.len());
    for (idx, component) in path.split('/').enumerate() {
        if idx != 0 {
            converted.push('/');
        }
        converted.push_str(&make_windows_component_compatible(component));
    }
    converted
}

fn make_windows_component_compatible(component: &str) -> String {
    let mut converted: String = component
        .chars()
        .map(|ch| {
            if ch < '\u{20}' || matches!(ch, '?' | '*' | '<' | '>' | '|' | '"' | ':') {
                '_'
            } else {
                ch
            }
        })
        .collect();

    if !matches!(converted.as_str(), "." | "..")
        && converted.ends_with(['.', ' '])
        && let Some((idx, _)) = converted.char_indices().next_back()
    {
        converted.replace_range(idx.., "_");
    }

    if windows_reserved_device_component(&converted) {
        converted.insert(0, '_');
    }

    converted
}

fn windows_reserved_device_component(component: &str) -> bool {
    let upper = component.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && upper.as_bytes()[3].is_ascii_digit()
}

fn unrar_convert_path_start(path: &str, strip_drive_specs: bool) -> usize {
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

        if strip_drive_specs && i + 1 < bytes.len() && bytes[i + 1] == b':' {
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
    is_safe_link_target_inner(member_path, target, false, true, true, true)
}

pub(crate) fn is_safe_symlink_target_for_member(
    member_path: &str,
    raw_member_path: &str,
    target: &str,
    allow_windows_drive_relative: bool,
) -> bool {
    is_safe_link_target_inner(
        member_path,
        target,
        allow_windows_drive_relative,
        true,
        true,
        true,
    ) && is_safe_link_target_inner(
        raw_member_path,
        target,
        allow_windows_drive_relative,
        true,
        true,
        true,
    )
}

pub(crate) fn is_safe_symlink_target_for_archive_member(
    format: ArchiveFormat,
    member_path: &str,
    raw_member_path: &str,
    target: &str,
) -> bool {
    let raw_member_backslash_is_separator = !matches!(format, ArchiveFormat::Rar5);
    is_safe_link_target_inner(member_path, target, true, false, false, false)
        && is_safe_link_target_inner(
            raw_member_path,
            target,
            true,
            raw_member_backslash_is_separator,
            false,
            false,
        )
}

fn is_safe_link_target_inner(
    member_path: &str,
    target: &str,
    allow_windows_drive_relative: bool,
    member_backslash_is_separator: bool,
    target_backslash_is_separator: bool,
    member_windows_drive_is_root: bool,
) -> bool {
    let member_path =
        normalize_backslash_for_path_check(member_path, member_backslash_is_separator);
    if is_full_root_path(&member_path, member_windows_drive_is_root) {
        return false;
    }

    // Reject absolute paths.
    let target = normalize_backslash_for_path_check(target, target_backslash_is_separator);
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

fn normalize_backslash_for_path_check(path: &str, backslash_is_separator: bool) -> String {
    if backslash_is_separator {
        path.replace('\\', "/")
    } else {
        path.to_string()
    }
}

fn is_full_root_path(path: &str, windows_drive_is_root: bool) -> bool {
    path.starts_with('/')
        || (windows_drive_is_root
            && path.len() >= 2
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

    #[cfg(not(windows))]
    #[test]
    fn file_redirection_drive_prefix_is_relative_like_unrar_on_unix() {
        assert_eq!(
            sanitize_file_redirection_path("C:\\dir\\source.txt"),
            "C:/dir/source.txt"
        );
    }

    #[cfg(windows)]
    #[test]
    fn file_redirection_drive_prefix_is_stripped_like_unrar_on_windows() {
        assert_eq!(
            sanitize_file_redirection_path("C:\\dir\\source.txt"),
            "dir/source.txt"
        );
    }

    #[test]
    fn file_redirection_still_strips_roots_and_traversal() {
        assert_eq!(
            sanitize_file_redirection_path("//server/share/dir/source.txt"),
            "dir/source.txt"
        );
        assert_eq!(
            sanitize_file_redirection_path("a/../dir/source.txt"),
            "dir/source.txt"
        );
    }

    #[test]
    fn null_byte_truncates_like_unrar() {
        assert_eq!(sanitize_path("foo\0bar.txt"), "foo");
    }

    #[test]
    fn dot_slash_collapsed() {
        assert_eq!(sanitize_path("./foo"), "foo");
    }

    #[test]
    fn internal_multiple_slashes_preserved_like_unrar() {
        assert_eq!(sanitize_path("foo///bar//baz.txt"), "foo///bar//baz.txt");
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
        assert_eq!(sanitize_path("/../.././foo/../bar/./baz"), "bar/./baz");
    }

    #[test]
    fn repeated_internal_traversal_uses_last_suffix_like_unrar() {
        assert_eq!(sanitize_path("foo/../../bar/baz.txt"), "bar/baz.txt");
        assert_eq!(sanitize_path("a/../b/../c.txt"), "c.txt");
    }

    #[test]
    fn internal_dot_components_preserved_like_unrar() {
        assert_eq!(sanitize_path("foo/./bar.txt"), "foo/./bar.txt");
    }

    #[test]
    fn trailing_internal_traversal_removes_everything_like_unrar() {
        assert_eq!(sanitize_path("foo/bar/.."), "");
    }

    #[test]
    fn unc_prefix_stripped_like_unrar() {
        assert_eq!(sanitize_path("//server/share/dir/file.txt"), "dir/file.txt");
    }

    #[test]
    fn rar5_windows_member_backslash_becomes_underscore_like_unrar_on_unix() {
        assert_eq!(
            sanitize_member_path(ArchiveFormat::Rar5, HostOs::Windows, "dir\\file.txt"),
            "dir_file.txt"
        );
    }

    #[test]
    fn windows_member_names_replace_colon_to_avoid_ntfs_streams_like_unrar() {
        assert_eq!(
            sanitize_member_path_for_target(
                ArchiveFormat::Rar5,
                HostOs::Unix,
                "dir:name/file.txt",
                true
            ),
            "dir_name/file.txt"
        );
        assert_eq!(
            sanitize_member_path_for_target(
                ArchiveFormat::Rar5,
                HostOs::Darwin,
                "dir\\literal.txt",
                true
            ),
            "dir_literal.txt"
        );
    }

    #[test]
    fn windows_member_names_precompose_unix_origin_unicode_like_unrar() {
        assert_eq!(
            sanitize_member_path_for_target(
                ArchiveFormat::Rar5,
                HostOs::Unix,
                "Cafe\u{301}/resume\u{301}.txt",
                true,
            ),
            "Caf\u{e9}/resum\u{e9}.txt"
        );
        assert_eq!(
            sanitize_member_path_for_target(
                ArchiveFormat::Rar5,
                HostOs::Darwin,
                "A\u{30a}.txt",
                true,
            ),
            "\u{c5}.txt"
        );
    }

    #[test]
    fn windows_member_names_do_not_precompose_windows_origin_unicode_like_unrar() {
        assert_eq!(
            sanitize_member_path_for_target(
                ArchiveFormat::Rar5,
                HostOs::Windows,
                "Cafe\u{301}.txt",
                true,
            ),
            "Cafe\u{301}.txt"
        );
    }

    #[test]
    fn windows_member_names_replace_incompatible_chars_like_unrar_fallback() {
        assert_eq!(
            sanitize_member_path_for_target(
                ArchiveFormat::Rar5,
                HostOs::Unix,
                "bad?name*/a<b>|c\"d\u{1f}.txt",
                true,
            ),
            "bad_name_/a_b__c_d_.txt"
        );
    }

    #[test]
    fn windows_member_names_replace_trailing_dot_or_space_like_unrar() {
        assert_eq!(
            sanitize_member_path_for_target(
                ArchiveFormat::Rar5,
                HostOs::Unix,
                "dir./file .txt/trailing./space ",
                true,
            ),
            "dir_/file .txt/trailing_/space_"
        );
        assert_eq!(
            sanitize_member_path_for_target(ArchiveFormat::Rar5, HostOs::Unix, "././file", true,),
            "file"
        );
        assert_eq!(
            make_windows_member_name_compatible("./../still"),
            "./../still"
        );
    }

    #[test]
    fn windows_member_names_prefix_pure_reserved_devices_like_unrar() {
        assert_eq!(
            sanitize_member_path_for_target(
                ArchiveFormat::Rar5,
                HostOs::Unix,
                "aux/COM1/Lpt9",
                true,
            ),
            "_aux/_COM1/_Lpt9"
        );
        assert_eq!(
            sanitize_member_path_for_target(ArchiveFormat::Rar5, HostOs::Unix, "aux.txt", true,),
            "aux.txt"
        );
    }

    #[test]
    fn windows_member_drive_prefix_is_stripped_before_colon_replacement() {
        assert_eq!(
            sanitize_member_path_for_target(
                ArchiveFormat::Rar5,
                HostOs::Windows,
                "C:/dir/file.txt",
                true
            ),
            "dir/file.txt"
        );
        assert_eq!(
            sanitize_member_path_for_target(
                ArchiveFormat::Rar4,
                HostOs::Windows,
                "C:\\dir\\file.txt",
                true
            ),
            "dir/file.txt"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn member_drive_prefix_is_relative_like_unrar_on_unix() {
        assert_eq!(
            sanitize_member_path(ArchiveFormat::Rar5, HostOs::Windows, "C:/dir/file.txt"),
            "C:/dir/file.txt"
        );
        assert_eq!(
            sanitize_member_path(ArchiveFormat::Rar4, HostOs::Windows, "C:\\dir\\file.txt"),
            "C:/dir/file.txt"
        );
    }

    #[cfg(windows)]
    #[test]
    fn member_drive_prefix_is_stripped_like_unrar_on_windows() {
        assert_eq!(
            sanitize_member_path(ArchiveFormat::Rar5, HostOs::Windows, "C:/dir/file.txt"),
            "dir/file.txt"
        );
        assert_eq!(
            sanitize_member_path(ArchiveFormat::Rar4, HostOs::Windows, "C:\\dir\\file.txt"),
            "dir/file.txt"
        );
    }

    #[test]
    fn rar5_unix_member_backslash_remains_literal_like_unrar_on_unix() {
        assert_eq!(
            sanitize_member_path(ArchiveFormat::Rar5, HostOs::Unix, "dir\\file.txt"),
            "dir\\file.txt"
        );
        assert_eq!(
            sanitize_member_path(ArchiveFormat::Rar5, HostOs::Darwin, "dir\\file.txt"),
            "dir\\file.txt"
        );
    }

    #[test]
    fn rar4_member_backslash_remains_path_separator_like_unrar() {
        assert_eq!(
            sanitize_member_path(ArchiveFormat::Rar4, HostOs::Unix, "dir\\file.txt"),
            "dir/file.txt"
        );
        assert_eq!(
            sanitize_member_path(ArchiveFormat::Rar4, HostOs::Windows, "dir\\file.txt"),
            "dir/file.txt"
        );
    }

    #[test]
    fn rar5_windows_slash_traversal_still_cuts_after_backslash_conversion() {
        assert_eq!(
            sanitize_member_path(ArchiveFormat::Rar5, HostOs::Windows, "a/../b\\c.txt"),
            "b_c.txt"
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
    fn rar5_unix_backslash_member_does_not_grant_symlink_parent_depth() {
        assert!(!is_safe_symlink_target_for_archive_member(
            ArchiveFormat::Rar5,
            "dir\\link",
            "dir\\link",
            "../escape"
        ));
    }

    #[test]
    fn rar4_backslash_member_still_grants_symlink_parent_depth_like_unrar() {
        assert!(is_safe_symlink_target_for_archive_member(
            ArchiveFormat::Rar4,
            "dir/link",
            "dir\\link",
            "../sibling"
        ));
    }

    #[test]
    fn unix_symlink_target_backslash_dotdot_is_literal_like_unrar() {
        assert!(is_safe_symlink_target_for_archive_member(
            ArchiveFormat::Rar5,
            "link",
            "link",
            "..\\not-parent"
        ));
    }

    #[test]
    fn archive_symlink_target_drive_letter_is_relative_like_unrar_on_unix() {
        assert!(is_safe_symlink_target_for_archive_member(
            ArchiveFormat::Rar5,
            "link",
            "link",
            "C:\\literal"
        ));
    }

    #[test]
    fn archive_symlink_member_drive_letter_is_not_root_like_unrar_on_unix() {
        assert!(is_safe_symlink_target_for_archive_member(
            ArchiveFormat::Rar5,
            "_literal",
            "C:\\literal",
            "target"
        ));
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
