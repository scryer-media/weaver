use std::path::{Path, PathBuf};

use crate::jobs::ids::JobId;

pub const WORKING_DIR_MARKER: &str = ".weaver-job-dir";
pub const OUTPUT_DIR_MARKER: &str = ".weaver-output-dir";

pub fn sanitize_dirname(name: &str) -> String {
    weaver_model::files::sanitize_path_component(name)
}

pub fn compute_working_dir(intermediate_dir: &Path, job_id: JobId, name: &str) -> PathBuf {
    let dir_name = sanitize_dirname(name);
    let candidate = intermediate_dir.join(&dir_name);
    if !candidate.exists() {
        candidate
    } else {
        intermediate_dir.join(weaver_model::files::path_component_with_suffix(
            &dir_name,
            &format!(".#{}", job_id.0),
        ))
    }
}

pub fn working_dir_marker_path(dir: &Path) -> PathBuf {
    dir.join(WORKING_DIR_MARKER)
}

pub fn is_weaver_owned_working_dir(dir: &Path) -> bool {
    working_dir_marker_path(dir).is_file()
}

pub fn mark_weaver_owned_output_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::write(dir.join(OUTPUT_DIR_MARKER), output_marker_value(dir)?)
}

pub fn is_weaver_owned_output_dir(dir: &Path) -> bool {
    let Ok(directory_metadata) = std::fs::symlink_metadata(dir) else {
        return false;
    };
    if directory_metadata.file_type().is_symlink() || !directory_metadata.is_dir() {
        return false;
    }
    let marker = dir.join(OUTPUT_DIR_MARKER);
    let Ok(marker_metadata) = std::fs::symlink_metadata(&marker) else {
        return false;
    };
    if marker_metadata.file_type().is_symlink() || !marker_metadata.is_file() {
        return false;
    }
    let Ok(expected) = output_marker_value(dir) else {
        return false;
    };
    std::fs::read(&marker).is_ok_and(|stored| stored == expected)
}

fn output_marker_value(dir: &Path) -> std::io::Result<Vec<u8>> {
    let canonical = std::fs::canonicalize(dir)?;
    let digest = blake3::hash(canonical.as_os_str().as_encoded_bytes());
    Ok(format!("weaver-output-v1:{}\n", digest.to_hex()).into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_dirname_bounds_long_names() {
        let sanitized = sanitize_dirname(&"a".repeat(400));

        assert!(sanitized.len() <= weaver_model::files::DOWNLOAD_FILENAME_MAX_BYTES);
    }

    #[test]
    fn sanitize_dirname_disarms_windows_device_names() {
        assert_eq!(sanitize_dirname("CON"), "_CON");
    }

    #[test]
    fn compute_working_dir_bounds_collision_suffix() {
        let temp = tempfile::tempdir().unwrap();
        let long_name = "a".repeat(400);
        let original = compute_working_dir(temp.path(), JobId(42), &long_name);
        std::fs::create_dir(&original).unwrap();

        let suffixed = compute_working_dir(temp.path(), JobId(42), &long_name);
        let file_name = suffixed.file_name().unwrap().to_string_lossy();

        assert!(file_name.ends_with(".#42"));
        assert!(file_name.len() <= weaver_model::files::DOWNLOAD_FILENAME_MAX_BYTES);
    }

    #[test]
    fn output_ownership_marker_is_bound_to_its_directory() {
        let temp = tempfile::tempdir().unwrap();
        let owned = temp.path().join("owned");
        let copied = temp.path().join("copied");
        std::fs::create_dir(&owned).unwrap();
        std::fs::create_dir(&copied).unwrap();
        mark_weaver_owned_output_dir(&owned).unwrap();

        assert!(is_weaver_owned_output_dir(&owned));
        std::fs::copy(
            owned.join(OUTPUT_DIR_MARKER),
            copied.join(OUTPUT_DIR_MARKER),
        )
        .unwrap();
        assert!(!is_weaver_owned_output_dir(&copied));
    }
}
