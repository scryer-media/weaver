use crate::{PgoCollectArgs, TaskContext};
use anyhow::{Context, Result, bail};
use chrono::Local;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn run_collect(ctx: &TaskContext, args: PgoCollectArgs) -> Result<()> {
    crate::require_command("task")?;
    crate::require_command("llvm-profdata")?;

    let e2e_dir = resolve_e2e_dir(ctx, args.e2e_dir)?;
    let out_dir = resolve_out_dir(ctx, args.out_dir);
    let profraw_dir = out_dir.join("profraw");
    let e2e_artifacts_dir = out_dir.join("e2e");
    let target_dir = out_dir.join("target");

    reset_dir(&profraw_dir)?;
    reset_dir(&e2e_artifacts_dir)?;
    fs::create_dir_all(&target_dir)?;

    println!("PGO output dir: {}", out_dir.display());
    println!("E2E dir: {}", e2e_dir.display());

    let weaver_bin = build_instrumented_weaver(ctx, &target_dir, &profraw_dir)?;
    build_e2e_cli(&e2e_dir)?;
    run_e2e_pgo(
        &e2e_dir,
        &weaver_bin,
        &profraw_dir,
        &e2e_artifacts_dir,
        &args.args,
    )?;

    let profraw_files = collect_profraw_files(&profraw_dir)?;
    if profraw_files.is_empty() {
        bail!(
            "no .profraw files were written to {}; ensure the e2e run used an instrumented Weaver binary",
            profraw_dir.display()
        );
    }

    let profdata_path = out_dir.join("weaver.profdata");
    merge_profiles(ctx, &profdata_path, &profraw_files)?;

    println!("Instrumented Weaver: {}", weaver_bin.display());
    println!("Raw profiles: {}", profraw_dir.display());
    println!("Merged profdata: {}", profdata_path.display());
    println!(
        "Release build command: RUSTFLAGS='{} -Cprofile-use={} -Cllvm-args=-pgo-warn-missing-function' cargo build --release -p weaver --locked",
        crate::host_local_rustflags(),
        profdata_path.display()
    );

    Ok(())
}

fn resolve_e2e_dir(ctx: &TaskContext, configured: Option<PathBuf>) -> Result<PathBuf> {
    let path = if let Some(path) = configured {
        resolve_path(&ctx.repo_root, path)
    } else {
        ctx.repo_root
            .parent()
            .map(|parent| parent.join("e2e"))
            .context("weaver repo root has no parent for sibling e2e checkout")?
    };
    if !path.is_dir() {
        bail!("e2e checkout not found: {}", path.display());
    }
    Ok(path)
}

fn resolve_out_dir(ctx: &TaskContext, configured: Option<PathBuf>) -> PathBuf {
    configured.map_or_else(
        || ctx.path(&format!("tmp/pgo/{}", Local::now().format("%Y%m%d-%H%M%S"))),
        |path| resolve_path(&ctx.repo_root, path),
    )
}

fn resolve_path(repo_root: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    }
}

fn reset_dir(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)
            .with_context(|| format!("remove existing directory {}", path.display()))?;
    }
    fs::create_dir_all(path).with_context(|| format!("create directory {}", path.display()))
}

fn build_instrumented_weaver(
    ctx: &TaskContext,
    target_dir: &Path,
    profraw_dir: &Path,
) -> Result<PathBuf> {
    println!("Building instrumented Weaver for profile generation...");
    let rustflags = format!(
        "{} -Cprofile-generate={}",
        crate::host_local_rustflags(),
        profraw_dir.display()
    );

    let mut build = ctx.command_in("cargo", &ctx.repo_root);
    build
        .env("CARGO_TARGET_DIR", target_dir)
        .env("RUSTFLAGS", rustflags)
        .args(["build", "--release", "-p", "weaver", "--locked"]);
    crate::run_checked(&mut build)?;

    let bin = target_dir
        .join("release")
        .join(format!("weaver{}", std::env::consts::EXE_SUFFIX));
    if !bin.is_file() {
        bail!("instrumented weaver binary missing at {}", bin.display());
    }
    Ok(bin)
}

fn build_e2e_cli(e2e_dir: &Path) -> Result<()> {
    println!("Building weaver-e2e CLI...");
    let mut task = std::process::Command::new("task");
    task.current_dir(e2e_dir).arg("build:weaver");
    crate::run_checked(&mut task)
}

fn run_e2e_pgo(
    e2e_dir: &Path,
    weaver_bin: &Path,
    profraw_dir: &Path,
    e2e_artifacts_dir: &Path,
    forwarded_args: &[String],
) -> Result<()> {
    println!("Running representative Weaver e2e flows for PGO data...");
    let mut task = std::process::Command::new("task");
    task.current_dir(e2e_dir)
        .arg("weaver:pgo")
        .env("WEAVER_BIN", weaver_bin)
        .env("E2E_WEAVER_PROFILE_DIR", profraw_dir)
        .env("E2E_WEAVER_PGO_OUTPUT_DIR", e2e_artifacts_dir)
        .env("DOWNLOAD_BENCH_LOCAL_WEAVER", "1");
    if !forwarded_args.is_empty() {
        task.arg("--").args(forwarded_args);
    }
    crate::run_checked(&mut task)
}

fn collect_profraw_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = fs::read_dir(dir)
        .with_context(|| format!("read profile directory {}", dir.display()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("profraw"))
        .collect::<Vec<_>>();
    files.sort();
    Ok(files)
}

fn merge_profiles(
    ctx: &TaskContext,
    profdata_path: &Path,
    profraw_files: &[PathBuf],
) -> Result<()> {
    println!("Merging {} raw profile(s)...", profraw_files.len());
    let mut merge = ctx.command("llvm-profdata");
    merge.args(["merge", "-sparse", "-o"]).arg(profdata_path);
    for file in profraw_files {
        merge.arg(file);
    }
    crate::run_checked(&mut merge)
}
