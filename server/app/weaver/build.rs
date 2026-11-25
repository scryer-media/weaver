// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    emit_git_commit();
    compile_windows_resources();
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    generate_skill_operations(&out_dir);
}

// Embed every operator-skill operation file, `skills/*/operations/*.graphql`,
// for the MCP catalog. The list is sorted so the generated file is stable.
fn generate_skill_operations(out_dir: &Path) {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let skills = manifest_dir.join("../../../skills");
    // A directory is scanned whole, so a new skill or operation file reruns
    // this as well as an edited one.
    println!("cargo:rerun-if-changed={}", skills.display());
    let mut rows = Vec::new();
    // No skills directory is an empty catalog, not a failed build; the
    // rerun line above picks the directory up when it appears.
    let skill_dirs = match std::fs::read_dir(&skills) {
        Ok(entries) => entries.collect::<Vec<_>>(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("read {}: {error}", skills.display()),
    };
    for skill in skill_dirs {
        let skill = skill.expect("skill entry").path();
        let operations = skill.join("operations");
        if !operations.is_dir() {
            continue;
        }
        println!("cargo:rerun-if-changed={}", operations.display());
        let skill_name = skill
            .file_name()
            .and_then(|name| name.to_str())
            .expect("skill directory name")
            .to_string();
        for file in std::fs::read_dir(&operations)
            .unwrap_or_else(|error| panic!("read {}: {error}", operations.display()))
        {
            let path = file.expect("operation entry").path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("graphql") {
                continue;
            }
            let file_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("operation file name")
                .to_string();
            // Joined, not canonicalized: a canonical Windows path is a
            // verbatim `\\?\` path.
            let embedded = skills.join(&skill_name).join("operations").join(&file_name);
            rows.push((skill_name.clone(), file_name, embedded));
        }
    }
    rows.sort();
    let mut code = String::from("pub(crate) static SKILL_OPERATIONS: &[SkillOperationFile] = &[\n");
    for (skill, file, path) in &rows {
        let path = path.to_str().expect("operation file path is UTF-8");
        code.push_str(&format!(
            "    SkillOperationFile {{ skill: {skill:?}, file: {file:?}, text: include_str!({path:?}) }},\n"
        ));
    }
    code.push_str("];\n");
    std::fs::write(out_dir.join("skill_operations.rs"), code).expect("write skill_operations.rs");
}

// Stamp the short commit into the binary for `weaver_build_info{commit=...}`.
//
// Release tarballs and container builds are routinely produced from an
// exported source tree with no `.git` at all, so every failure mode here has
// to degrade to `unknown` rather than fail the build.
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

// Find the repository's git directory, walking up from the crate.
//
// Worktrees store a `.git` *file* pointing at the real directory, so the
// presence check must accept both.
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
    println!("cargo:rerun-if-changed=resources/windows/weaver.ico");
    println!("cargo:rerun-if-changed=resources/windows/weaver.exe.manifest");

    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }

    let resource_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("missing crate manifest directory"))
            .join("resources/windows");
    embed_resource::compile(
        resource_dir.join("weaver.rc"),
        embed_resource::ParamsIncludeDirs([&resource_dir]),
    )
    .manifest_required()
    .unwrap_or_else(|error| panic!("failed to embed Windows application manifest: {error}"));
}
