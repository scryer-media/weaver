//! PAR2 filename translation.
//!
//! PAR2 filenames are archive-local relative paths. They are not trusted
//! filesystem paths, so translation preserves useful relative structure while
//! encoding components that could escape the job directory.

use std::path::{Component, Path, PathBuf};

use crate::error::{Par2Error, Result};

fn encode_byte(byte: u8, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.push('%');
    out.push(HEX[(byte >> 4) as usize] as char);
    out.push(HEX[(byte & 0x0F) as usize] as char);
}

fn encode_char(ch: char, out: &mut String) {
    let mut bytes = [0u8; 4];
    for byte in ch.encode_utf8(&mut bytes).as_bytes() {
        encode_byte(*byte, out);
    }
}

fn encode_component(component: &str) -> String {
    let mut encoded = String::with_capacity(component.len());
    for ch in component.chars() {
        let illegal =
            ch.is_control() || matches!(ch, '"' | '*' | ':' | '<' | '>' | '?' | '|' | '\0');
        if illegal {
            encode_char(ch, &mut encoded);
        } else {
            encoded.push(ch);
        }
    }
    encoded
}

/// Translate a PAR2 filename into a safe relative path string.
///
/// This intentionally preserves safe subdirectories instead of flattening to a
/// basename. Absolute roots and parent-directory components are percent-encoded
/// so the final path remains job-local while staying recognizable in logs.
pub fn translate_par2_name_to_relative(par2_name: &str) -> Result<String> {
    let normalized = par2_name.replace('\\', "/");
    let mut parts = Vec::new();
    let mut saw_name_component = false;

    for (index, part) in normalized.split('/').enumerate() {
        if part.is_empty() {
            if index == 0 {
                parts.push("%2F".to_string());
            }
            continue;
        }
        saw_name_component = true;

        let encoded = match part {
            "." => "%2E".to_string(),
            ".." => "%2E%2E".to_string(),
            other => encode_component(other),
        };
        if !encoded.is_empty() {
            parts.push(encoded);
        }
    }

    if !saw_name_component {
        return Err(Par2Error::InvalidFileDescPacket {
            reason: "filename is empty after translation".to_string(),
        });
    }

    Ok(parts.join("/"))
}

/// Resolve a PAR2 filename under a base directory after safe translation.
pub fn translate_par2_name_to_local_path(par2_name: &str, base_dir: &Path) -> Result<PathBuf> {
    let relative = translate_par2_name_to_relative(par2_name)?;
    let relative_path = Path::new(&relative);

    if relative_path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(Par2Error::InvalidFileDescPacket {
            reason: format!("translated filename is not relative: {relative}"),
        });
    }

    Ok(base_dir.join(relative_path))
}

pub(crate) fn is_generated_par2_artifact_name(name: &str) -> bool {
    name.starts_with(".swap.")
        || name.starts_with(".chunk.")
        || name.starts_with(".weaver-par2-repair")
        || name.contains(".weaver-par2-backup.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_safe_subdirectories() {
        assert_eq!(
            translate_par2_name_to_relative("season/episode.rar").unwrap(),
            "season/episode.rar"
        );
    }

    #[test]
    fn normalizes_windows_separators() {
        assert_eq!(
            translate_par2_name_to_relative("season\\episode.rar").unwrap(),
            "season/episode.rar"
        );
    }

    #[test]
    fn encodes_absolute_and_parent_paths() {
        assert_eq!(
            translate_par2_name_to_relative("/tmp/../episode.rar").unwrap(),
            "%2F/tmp/%2E%2E/episode.rar"
        );
    }

    #[test]
    fn encodes_control_and_platform_illegal_chars() {
        assert_eq!(
            translate_par2_name_to_relative("bad:name\u{1}.rar").unwrap(),
            "bad%3Aname%01.rar"
        );
    }

    #[test]
    fn rejects_empty_names() {
        assert!(translate_par2_name_to_relative("").is_err());
        assert!(translate_par2_name_to_relative("///").is_err());
    }

    #[test]
    fn identifies_generated_repair_artifacts() {
        assert!(is_generated_par2_artifact_name(".swap.movie.rar.tmp"));
        assert!(is_generated_par2_artifact_name(".chunk.movie.rar"));
        assert!(is_generated_par2_artifact_name(".weaver-par2-repair-abc"));
        assert!(is_generated_par2_artifact_name(
            "movie.rar.weaver-par2-backup.123"
        ));
        assert!(!is_generated_par2_artifact_name("movie.rar"));
    }
}
