use std::path::{Path, PathBuf};

use crate::jobs::ids::JobId;

pub fn sanitize_dirname(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | '<' | '>' | '?' | '*' | '|' | '"' | ':' => '_',
            _ => c,
        })
        .take(200)
        .collect();
    sanitized.trim_end_matches(['.', ' ']).to_string()
}

pub fn compute_working_dir(intermediate_dir: &Path, job_id: JobId, name: &str) -> PathBuf {
    let dir_name = sanitize_dirname(name);
    let candidate = intermediate_dir.join(&dir_name);
    if !candidate.exists() {
        candidate
    } else {
        intermediate_dir.join(format!("{}.#{}", dir_name, job_id.0))
    }
}
