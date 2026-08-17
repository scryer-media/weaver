use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    emit_git_commit();
    compile_windows_resources();
}

/// Stamp the short commit into the binary for `weaver_build_info{commit=...}`.
///
/// Release tarballs and container builds are routinely produced from an
/// exported source tree with no `.git` at all, so every failure mode here has
/// to degrade to `unknown` rather than fail the build.
fn emit_git_commit() {
    let commit = git_commit().unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=WEAVER_GIT_COMMIT={commit}");
}

fn git_commit() -> Option<String> {
    let git_dir = locate_git_dir()?;
    // Rebuild when the checked-out revision changes. `.git/HEAD` covers branch
    // switches; the ref file it points at covers new commits on that branch.
    watch_head(&git_dir);

    let output = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let commit = String::from_utf8(output.stdout).ok()?.trim().to_string();
    if commit.is_empty() {
        return None;
    }
    Some(commit)
}

/// Find the repository's git directory, walking up from the crate.
///
/// Worktrees store a `.git` *file* pointing at the real directory, so the
/// presence check must accept both.
fn locate_git_dir() -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").ok()?);
    let mut current: Option<&Path> = Some(manifest_dir.as_path());
    while let Some(dir) = current {
        let candidate = dir.join(".git");
        if candidate.exists() {
            return Some(candidate);
        }
        current = dir.parent();
    }
    None
}

fn watch_head(git_dir: &Path) {
    if git_dir.is_file() {
        // Worktree pointer file: its content changes when the worktree moves.
        println!("cargo:rerun-if-changed={}", git_dir.display());
        return;
    }
    let head = git_dir.join("HEAD");
    if !head.exists() {
        return;
    }
    println!("cargo:rerun-if-changed={}", head.display());
    let Ok(contents) = std::fs::read_to_string(&head) else {
        return;
    };
    if let Some(reference) = contents.strip_prefix("ref:") {
        let reference = reference.trim();
        let ref_path = git_dir.join(reference);
        if ref_path.exists() {
            println!("cargo:rerun-if-changed={}", ref_path.display());
        }
    }
}

fn compile_windows_resources() {
    println!("cargo:rerun-if-changed=resources/windows/weaver.rc");
    println!("cargo:rerun-if-changed=resources/windows/weaver.exe.manifest");

    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }

    embed_resource::compile("resources/windows/weaver.rc", embed_resource::NONE)
        .manifest_required()
        .unwrap_or_else(|error| panic!("failed to embed Windows application manifest: {error}"));
}
