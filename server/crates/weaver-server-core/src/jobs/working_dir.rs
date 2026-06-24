use std::path::{Path, PathBuf};

use crate::jobs::ids::JobId;

pub const WORKING_DIR_MARKER: &str = ".weaver-job-dir";

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
}
