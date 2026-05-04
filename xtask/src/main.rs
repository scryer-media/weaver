use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use semver::Version;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdout, Command, ExitStatus, Output, Stdio};
use std::thread;
use std::time::{Duration as StdDuration, Instant};
use toml_edit::{DocumentMut, value};

mod monitor;
mod par2_kit;
mod perf;
mod profile_local;

const BLUE: &str = "\x1b[0;34m";
const GREEN: &str = "\x1b[0;32m";
const YELLOW: &str = "\x1b[1;33m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";
const LSOF_PORT_PROBE_TIMEOUT: StdDuration = StdDuration::from_secs(3);
const TCP_PORT_PROBE_TIMEOUT: StdDuration = StdDuration::from_millis(200);
const RELEASE_DRY_RUN_CACHE_FILE: &str = "tmp/xtask-release-dry-run.json";
const RELEASE_DRY_RUN_CACHE_DIR: &str = "tmp/xtask-release-dry-run-cache";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask has a repo root parent")
        .to_path_buf()
}

#[derive(Parser)]
#[command(name = "cargo xtask")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Release(ReleaseArgs),
    Ci(CiArgs),
    Dev(DevArgs),
    Deploy(DeployArgs),
    Monitor,
    Profile(ProfileArgs),
    Perf(PerfArgs),
    Par2(Par2Args),
}

#[derive(Args)]
struct ReleaseArgs {
    #[arg(long, conflicts_with_all = ["minor", "patch", "version"])]
    major: bool,
    #[arg(long, conflicts_with_all = ["major", "patch", "version"])]
    minor: bool,
    #[arg(long, conflicts_with_all = ["major", "minor", "version"])]
    patch: bool,
    #[arg(long)]
    dry_run: bool,
    version: Option<String>,
}

#[derive(Args)]
struct CiArgs {
    #[command(subcommand)]
    command: CiCommand,
}

#[derive(Subcommand)]
enum CiCommand {
    Clippy(ClippyArgs),
}

#[derive(Args)]
struct ClippyArgs {
    #[arg(long)]
    linux_only: bool,
}

#[derive(Args)]
struct DevArgs {
    target: Option<String>,
}

#[derive(Args)]
struct DeployArgs {
    #[command(subcommand)]
    command: DeployCommand,
}

#[derive(Subcommand)]
enum DeployCommand {
    Local(DeployLocalArgs),
}

#[derive(Args)]
struct DeployLocalArgs {
    target: Option<String>,
    /// Port to run the local Weaver server on (default: 9090).
    #[arg(long, short = 'p', default_value_t = 9090)]
    port: u16,
    /// Skip the frontend and Rust release builds; reuse the existing
    /// `target/release/weaver` binary as-is. Fails if that binary is missing.
    #[arg(long)]
    no_build: bool,
}

#[derive(Args)]
struct ProfileArgs {
    #[command(subcommand)]
    command: ProfileCommand,
}

#[derive(Subcommand)]
enum ProfileCommand {
    Local(ProfileLocalArgs),
}

#[derive(Args)]
struct ProfileLocalArgs {
    duration_seconds: Option<String>,
    interval_millis: Option<String>,
}

#[derive(Args)]
struct PerfArgs {
    #[command(subcommand)]
    command: PerfCommand,
}

#[derive(Subcommand)]
enum PerfCommand {
    #[command(name = "par2-x86", disable_help_flag = true)]
    Par2X86(ForwardArgs),
    #[command(name = "real-download", disable_help_flag = true)]
    RealDownload(ForwardArgs),
}

#[derive(Args)]
struct Par2Args {
    #[command(subcommand)]
    command: Par2Command,
}

#[derive(Subcommand)]
enum Par2Command {
    #[command(name = "finalize-kit", disable_help_flag = true)]
    FinalizeKit(ForwardArgs),
}

#[derive(Args)]
struct ForwardArgs {
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

#[derive(Copy, Clone, Eq, PartialEq, ValueEnum)]
enum VersionBump {
    Patch,
    Minor,
    Major,
}

#[derive(Clone)]
struct TaskContext {
    repo_root: PathBuf,
}

impl TaskContext {
    fn new() -> Self {
        Self {
            repo_root: repo_root(),
        }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.repo_root.join(relative)
    }

    fn command(&self, program: impl AsRef<OsStr>) -> Command {
        Command::new(program)
    }

    fn command_in(&self, program: impl AsRef<OsStr>, cwd: &Path) -> Command {
        let mut command = Command::new(program);
        command.current_dir(cwd);
        command
    }
}

#[derive(Debug, Deserialize)]
struct GhRelease {
    #[serde(rename = "tagName")]
    tag_name: String,
    #[serde(rename = "publishedAt")]
    published_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReleaseDryRunCache {
    success: bool,
    created_at: String,
    git_commit: String,
    branch: String,
    worktree_clean_at_start: bool,
    release_args: String,
    latest_tag_seen: Option<String>,
    next_version: String,
    tag_name: String,
    validated_steps: Vec<String>,
    failure_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReleaseDryRunExpectations<'a> {
    git_commit: &'a str,
    release_args: &'a str,
    latest_tag_seen: Option<&'a str>,
    next_version: &'a str,
    tag_name: &'a str,
}

#[derive(Default)]
struct PortProbeState {
    lsof_timed_out: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let ctx = TaskContext::new();

    match cli.command {
        Commands::Release(args) => run_release(&ctx, args),
        Commands::Ci(args) => match args.command {
            CiCommand::Clippy(args) => run_clippy_ci(&ctx, args),
        },
        Commands::Dev(args) => run_dev(&ctx, args),
        Commands::Deploy(args) => match args.command {
            DeployCommand::Local(args) => run_deploy_local(&ctx, args),
        },
        Commands::Monitor => monitor::run(&ctx),
        Commands::Profile(args) => match args.command {
            ProfileCommand::Local(args) => profile_local::run(&ctx, args),
        },
        Commands::Perf(args) => match args.command {
            PerfCommand::Par2X86(args) => perf::run_par2_x86(&ctx, args.args),
            PerfCommand::RealDownload(args) => perf::run_real_download(&ctx, args.args),
        },
        Commands::Par2(args) => match args.command {
            Par2Command::FinalizeKit(args) => par2_kit::run(&ctx, args.args),
        },
    }
}

fn step(message: impl AsRef<str>) {
    println!("\n{BLUE}{BOLD}▶  {}{RESET}", message.as_ref());
}

fn ok(message: impl AsRef<str>) {
    println!("   {GREEN}✓  {}{RESET}", message.as_ref());
}

fn warn(message: impl AsRef<str>) {
    eprintln!("   {YELLOW}⚠  {}{RESET}", message.as_ref());
}

fn prefixed_step(prefix: &str, message: impl AsRef<str>) {
    println!("{prefix}{BLUE}{BOLD}▶  {}{RESET}", message.as_ref());
}

fn prefixed_ok(prefix: &str, message: impl AsRef<str>) {
    println!("{prefix}{GREEN}✓  {}{RESET}", message.as_ref());
}

fn command_available(command: &str) -> Result<bool> {
    let status = Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {command} >/dev/null 2>&1"))
        .status()?;
    Ok(status.success())
}

fn require_command(command: &str) -> Result<()> {
    if command_available(command)? {
        Ok(())
    } else {
        bail!("{command} is required")
    }
}

fn run_status(command: &mut Command) -> Result<ExitStatus> {
    Ok(command.status()?)
}

fn run_checked(command: &mut Command) -> Result<()> {
    let debug = format!("{command:?}");
    let status = run_status(command)?;
    if !status.success() {
        bail!("command failed: {debug}");
    }
    Ok(())
}

fn run_capture(command: &mut Command) -> Result<String> {
    let debug = format!("{command:?}");
    let output = command.output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("command failed: {debug}\n{stderr}");
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn run_output_with_timeout(command: &mut Command, timeout: StdDuration) -> Result<Option<Output>> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let deadline = Instant::now() + timeout;

    loop {
        if child.try_wait()?.is_some() {
            return Ok(Some(child.wait_with_output()?));
        }
        if Instant::now() >= deadline {
            terminate_child(&mut child);
            return Ok(None);
        }
        thread::sleep(StdDuration::from_millis(25));
    }
}

fn terminate_child(child: &mut Child) {
    let _ = child.kill();
    let deadline = Instant::now() + StdDuration::from_millis(500);
    while Instant::now() < deadline {
        if matches!(child.try_wait(), Ok(Some(_))) {
            return;
        }
        thread::sleep(StdDuration::from_millis(25));
    }
}

fn read_pid_command_line(pid: &str) -> Result<Option<String>> {
    let output = Command::new("ps")
        .args(["-o", "command=", "-p", pid])
        .output()?;
    if !output.status.success() {
        return Ok(None);
    }
    let command = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if command.is_empty() {
        Ok(None)
    } else {
        Ok(Some(command))
    }
}

fn command_matches_local_weaver(command_line: &str, binaries: &[PathBuf]) -> bool {
    binaries.iter().any(|binary| {
        let binary = binary.display().to_string();
        command_line == binary || command_line.starts_with(&format!("{binary} "))
    })
}

fn list_port_pids(ctx: &TaskContext, port: u16, probe: &mut PortProbeState) -> Result<Vec<String>> {
    if probe.lsof_timed_out {
        if port_accepts_tcp(port) {
            bail!(
                "Port {port} is reachable, but a previous lsof check timed out. \
                 Refusing to continue without a safe owner check."
            );
        }
        return Ok(Vec::new());
    }

    let mut lsof = ctx.command("lsof");
    lsof.args([&format!("-tiTCP:{port}"), "-sTCP:LISTEN"]);
    let Some(output) = run_output_with_timeout(&mut lsof, LSOF_PORT_PROBE_TIMEOUT)? else {
        probe.lsof_timed_out = true;
        if port_accepts_tcp(port) {
            bail!(
                "Timed out while identifying the process listening on port {port}. \
                 The port is reachable, so refusing to continue without a safe owner check."
            );
        }
        println!(
            "    lsof timed out while checking port {port}; no localhost listener detected, continuing."
        );
        return Ok(Vec::new());
    };
    if !output.status.success() {
        return Ok(Vec::new());
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

fn port_accepts_tcp(port: u16) -> bool {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&address, TCP_PORT_PROBE_TIMEOUT).is_ok()
}

fn signal_pids(ctx: &TaskContext, pids: &[String], signal: Option<&str>) -> Result<()> {
    if pids.is_empty() {
        return Ok(());
    }
    let mut kill = ctx.command("kill");
    if let Some(signal) = signal {
        kill.arg(signal);
    }
    kill.args(pids);
    let _ = run_status(&mut kill);
    Ok(())
}

fn run_streaming(command: &mut Command, prefix: &'static str) -> Result<()> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_thread = stdout.map(|stdout| pipe_stdout(stdout, prefix));
    let stderr_thread = stderr.map(|stderr| pipe_stderr(stderr, prefix));
    let status = child.wait()?;
    if let Some(thread) = stdout_thread {
        let _ = thread.join();
    }
    if let Some(thread) = stderr_thread {
        let _ = thread.join();
    }
    if !status.success() {
        bail!("command failed: {command:?}");
    }
    Ok(())
}

fn pipe_stdout(stream: ChildStdout, prefix: &'static str) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let reader = BufReader::new(stream);
        for line in reader.lines().map_while(Result::ok) {
            println!("{prefix}{line}");
        }
    })
}

fn pipe_stderr(stream: ChildStderr, prefix: &'static str) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let reader = BufReader::new(stream);
        for line in reader.lines().map_while(Result::ok) {
            eprintln!("{prefix}{line}");
        }
    })
}

fn git_capture(ctx: &TaskContext, args: &[&str]) -> Result<String> {
    let mut command = ctx.command_in("git", &ctx.repo_root);
    command.args(args);
    run_capture(&mut command)
}

fn latest_weaver_tag(ctx: &TaskContext) -> Result<Option<String>> {
    let tags = git_capture(ctx, &["tag", "--sort=-version:refname"])?;
    let weaver_tag = tags
        .lines()
        .find(|line| line.starts_with("weaver-v"))
        .map(ToOwned::to_owned);
    if weaver_tag.is_some() {
        return Ok(weaver_tag);
    }
    Ok(tags
        .lines()
        .find(|line| {
            let trimmed = line.trim();
            trimmed
                .split('.')
                .all(|segment| !segment.is_empty() && segment.chars().all(|ch| ch.is_ascii_digit()))
                && trimmed.matches('.').count() == 2
        })
        .map(ToOwned::to_owned))
}

fn git_status_porcelain(ctx: &TaskContext) -> Result<String> {
    git_capture(ctx, &["status", "--porcelain"])
}

fn git_tracked_dirty_paths(ctx: &TaskContext) -> Result<Vec<PathBuf>> {
    let mut command = ctx.command_in("git", &ctx.repo_root);
    command.args(["diff", "--name-only", "HEAD", "--"]);
    let output = run_capture(&mut command)?;
    Ok(output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| ctx.path(line))
        .collect())
}

fn current_branch(ctx: &TaskContext) -> Result<String> {
    git_capture(ctx, &["rev-parse", "--abbrev-ref", "HEAD"]).map(|value| value.trim().to_string())
}

fn current_head_commit(ctx: &TaskContext) -> Result<String> {
    git_capture(ctx, &["rev-parse", "HEAD"]).map(|value| value.trim().to_string())
}

fn prompt_continue_if_dirty(ctx: &TaskContext) -> Result<()> {
    let status = git_status_porcelain(ctx)?;
    if status.trim().is_empty() {
        return Ok(());
    }
    warn("Working tree has uncommitted changes:");
    for line in status.lines() {
        eprintln!("     {line}");
    }
    eprint!("\n   Continue anyway? [y/N] ");
    io::stderr().flush()?;
    let mut response = String::new();
    io::stdin().read_line(&mut response)?;
    if !matches!(response.trim(), "y" | "Y") {
        bail!("aborted");
    }
    Ok(())
}

fn changed_file(ctx: &TaskContext, path: &Path) -> Result<bool> {
    let output = git_capture(ctx, &["status", "--short", "--", &path.to_string_lossy()])?;
    Ok(!output.trim().is_empty())
}

fn git_checkout_paths(ctx: &TaskContext, paths: &[PathBuf]) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let mut command = ctx.command_in("git", &ctx.repo_root);
    command.arg("checkout").arg("--");
    command.args(paths);
    run_checked(&mut command)
}

fn git_checkout_paths_from_source(
    ctx: &TaskContext,
    source: Option<&str>,
    paths: &[PathBuf],
) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let mut command = ctx.command_in("git", &ctx.repo_root);
    command.arg("checkout");
    if let Some(source) = source {
        command.arg(source);
    }
    command.arg("--");
    command.args(paths);
    run_checked(&mut command)
}

fn git_stash_create_snapshot(ctx: &TaskContext) -> Result<Option<String>> {
    let snapshot = git_capture(ctx, &["stash", "create", "xtask release dry run snapshot"])?;
    let snapshot = snapshot.trim();
    if snapshot.is_empty() {
        Ok(None)
    } else {
        Ok(Some(snapshot.to_string()))
    }
}

fn release_args_signature(explicit: Option<&Version>, bump: VersionBump) -> String {
    explicit.map_or_else(
        || format!("bump:{}", version_bump_label(bump)),
        |version| format!("version:{version}"),
    )
}

fn version_bump_label(bump: VersionBump) -> &'static str {
    match bump {
        VersionBump::Patch => "patch",
        VersionBump::Minor => "minor",
        VersionBump::Major => "major",
    }
}

fn parse_bump(args: &ReleaseArgs) -> Result<(VersionBump, Option<Version>)> {
    let explicit = match &args.version {
        Some(version) => Some(Version::parse(version.trim_start_matches('v'))?),
        None => None,
    };
    let bump = if args.major {
        VersionBump::Major
    } else if args.minor {
        VersionBump::Minor
    } else {
        VersionBump::Patch
    };
    Ok((bump, explicit))
}

fn release_dry_run_cache_path(ctx: &TaskContext) -> PathBuf {
    ctx.path(RELEASE_DRY_RUN_CACHE_FILE)
}

fn release_dry_run_cache_dir(ctx: &TaskContext) -> PathBuf {
    ctx.path(RELEASE_DRY_RUN_CACHE_DIR)
}

fn clear_release_dry_run_cache(ctx: &TaskContext) -> Result<()> {
    let cache_path = release_dry_run_cache_path(ctx);
    if cache_path.exists() {
        fs::remove_file(&cache_path)
            .with_context(|| format!("failed to remove {}", cache_path.display()))?;
    }

    let cache_dir = release_dry_run_cache_dir(ctx);
    if cache_dir.exists() {
        fs::remove_dir_all(&cache_dir)
            .with_context(|| format!("failed to remove {}", cache_dir.display()))?;
    }

    Ok(())
}

fn write_release_dry_run_cache(ctx: &TaskContext, cache: &ReleaseDryRunCache) -> Result<()> {
    let path = release_dry_run_cache_path(ctx);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&path, serde_json::to_string_pretty(cache)? + "\n")
        .with_context(|| format!("failed to write {}", path.display()))
}

fn load_release_dry_run_cache(ctx: &TaskContext) -> Result<ReleaseDryRunCache> {
    let path = release_dry_run_cache_path(ctx);
    let raw =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

fn release_dry_run_cache_rejection_reason(
    cache: &ReleaseDryRunCache,
    expected: &ReleaseDryRunExpectations<'_>,
) -> Option<String> {
    if !cache.success {
        return Some("previous dry run did not complete successfully".to_string());
    }
    if !cache.worktree_clean_at_start {
        return Some("dry run started from a dirty worktree".to_string());
    }
    if cache.git_commit != expected.git_commit {
        return Some("HEAD commit changed since dry run".to_string());
    }
    if cache.release_args != expected.release_args {
        return Some("release arguments changed since dry run".to_string());
    }
    if cache.latest_tag_seen.as_deref() != expected.latest_tag_seen {
        return Some("latest release tag changed since dry run".to_string());
    }
    if cache.next_version != expected.next_version {
        return Some("computed next version changed since dry run".to_string());
    }
    if cache.tag_name != expected.tag_name {
        return Some("computed release tag changed since dry run".to_string());
    }
    None
}

fn restore_dry_run_tracked_changes(
    ctx: &TaskContext,
    tracked_dirty_paths_before_validation: &[PathBuf],
    dirty_snapshot: Option<&str>,
) -> Result<()> {
    let tracked_dirty_paths_after_validation = git_tracked_dirty_paths(ctx)?;
    if tracked_dirty_paths_after_validation.is_empty() {
        return Ok(());
    }

    let tracked_dirty_before = tracked_dirty_paths_before_validation
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let tracked_dirty_after = tracked_dirty_paths_after_validation
        .into_iter()
        .collect::<BTreeSet<_>>();

    let restore_from_head = tracked_dirty_after
        .difference(&tracked_dirty_before)
        .cloned()
        .collect::<Vec<_>>();
    let restore_from_snapshot = tracked_dirty_after
        .intersection(&tracked_dirty_before)
        .cloned()
        .collect::<Vec<_>>();

    git_checkout_paths(ctx, &restore_from_head)?;
    if !restore_from_snapshot.is_empty() {
        let snapshot = dirty_snapshot.ok_or_else(|| {
            anyhow!("missing dry-run snapshot for preexisting tracked worktree changes")
        })?;
        git_checkout_paths_from_source(ctx, Some(snapshot), &restore_from_snapshot)?;
    }

    Ok(())
}

fn next_version(current: &Version, bump: VersionBump) -> Version {
    let mut next = current.clone();
    match bump {
        VersionBump::Patch => next.patch += 1,
        VersionBump::Minor => {
            next.minor += 1;
            next.patch = 0;
        }
        VersionBump::Major => {
            next.major += 1;
            next.minor = 0;
            next.patch = 0;
        }
    }
    next.pre = Default::default();
    next.build = Default::default();
    next
}

fn write_workspace_version(path: &Path, version: &Version) -> Result<()> {
    let mut document = fs::read_to_string(path)?
        .parse::<DocumentMut>()
        .with_context(|| format!("failed to parse {}", path.display()))?;
    document["workspace"]["package"]["version"] = value(version.to_string());
    fs::write(path, document.to_string())?;
    Ok(())
}

fn run_clippy_ci(ctx: &TaskContext, args: ClippyArgs) -> Result<()> {
    let linux_target = "x86_64-unknown-linux-gnu";
    let mut rustc = ctx.command("rustc");
    rustc.arg("-vV");
    let host_target = run_capture(&mut rustc)?
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or_else(|| anyhow!("failed to determine host target"))?
        .trim()
        .to_string();
    let linux_image = std::env::var("WEAVER_LINUX_CLIPPY_IMAGE")
        .unwrap_or_else(|_| "rust:1.94-bookworm".to_string());
    let linux_platform =
        std::env::var("WEAVER_LINUX_CLIPPY_PLATFORM").unwrap_or_else(|_| "linux/arm64".to_string());

    if !args.linux_only {
        println!("Running cargo clippy for host target: {host_target}");
        let mut command = ctx.command_in("cargo", &ctx.repo_root);
        command.args([
            "clippy",
            "--workspace",
            "--lib",
            "--bins",
            "--tests",
            "--examples",
            "--",
            "-D",
            "warnings",
        ]);
        run_checked(&mut command)?;
    }

    if args.linux_only || host_target != linux_target {
        if command_available("docker")? {
            println!("Running cargo clippy in Linux container: {linux_image}");
            let mut command = ctx.command("docker");
            command.args([
                "run",
                "--rm",
                "--platform",
                &linux_platform,
                "-v",
                &format!("{}:/work", ctx.repo_root.display()),
                "-w",
                "/work",
                "-e",
                "CARGO_HOME=/tmp/cargo",
                "-e",
                "CARGO_TARGET_DIR=/tmp/target",
                "-e",
                "CARGO_TERM_COLOR=always",
                &linux_image,
                "bash",
                "-lc",
                "set -euo pipefail; /usr/local/cargo/bin/rustup component add clippy; toolchain=\"$('/usr/local/cargo/bin/rustup' show active-toolchain | cut -d' ' -f1)\"; toolchain_bin=\"/usr/local/rustup/toolchains/${toolchain}/bin\"; export PATH=\"${toolchain_bin}:$PATH\"; \"${toolchain_bin}/cargo-clippy\" clippy --workspace --lib --bins --tests --examples -- -D warnings",
            ]);
            run_checked(&mut command)?;
        } else if command_available("x86_64-linux-gnu-gcc")? {
            let mut target_add = ctx.command("rustup");
            target_add.args(["target", "add", linux_target]);
            run_checked(&mut target_add)?;

            let mut command = ctx.command_in("cargo", &ctx.repo_root);
            command.args([
                "clippy",
                "--workspace",
                "--lib",
                "--bins",
                "--tests",
                "--examples",
                "--target",
                linux_target,
                "--",
                "-D",
                "warnings",
            ]);
            run_checked(&mut command)?;
        } else {
            bail!("cannot run Linux CI clippy locally; install Docker or x86_64-linux-gnu-gcc");
        }
    }

    Ok(())
}

fn run_release(ctx: &TaskContext, args: ReleaseArgs) -> Result<()> {
    step("Determining next version");
    let latest_tag = latest_weaver_tag(ctx)?;
    let current_version = latest_tag
        .as_deref()
        .map(|tag| Version::parse(tag.trim_start_matches("weaver-v")))
        .transpose()?
        .unwrap_or_else(|| Version::new(0, 0, 0));
    let (bump, explicit) = parse_bump(&args)?;
    let release_args = release_args_signature(explicit.as_ref(), bump);
    let next_version = explicit.unwrap_or_else(|| next_version(&current_version, bump));
    let tag_name = format!("weaver-v{next_version}");

    println!(
        "   Latest tag : {}",
        latest_tag.as_deref().unwrap_or("none")
    );
    println!("   Next tag   : {tag_name}");
    if args.dry_run {
        println!("   {YELLOW}(dry run — no commits, tags, or pushes){RESET}");
    }

    step("Pre-flight checks");
    let tags = git_capture(ctx, &["tag"])?;
    if tags.lines().any(|line| line == tag_name) {
        bail!("Tag {tag_name} already exists");
    }
    let branch = current_branch(ctx)?;
    let git_commit = current_head_commit(ctx)?;
    println!("   Branch : {branch}");
    let worktree_clean_at_start = git_status_porcelain(ctx)?.trim().is_empty();
    if !worktree_clean_at_start {
        prompt_continue_if_dirty(ctx)?;
    }
    require_command("gh")?;
    ok("Pre-flight OK");

    let cache_path = release_dry_run_cache_path(ctx);
    let mut reused_dry_run_cache = false;
    if args.dry_run {
        clear_release_dry_run_cache(ctx)?;
        write_release_dry_run_cache(
            ctx,
            &ReleaseDryRunCache {
                success: false,
                created_at: Utc::now().to_rfc3339(),
                git_commit: git_commit.clone(),
                branch: branch.clone(),
                worktree_clean_at_start,
                release_args: release_args.clone(),
                latest_tag_seen: latest_tag.clone(),
                next_version: next_version.to_string(),
                tag_name: tag_name.clone(),
                validated_steps: Vec::new(),
                failure_message: Some("dry run did not complete".to_string()),
            },
        )?;
    } else if worktree_clean_at_start && cache_path.is_file() {
        match load_release_dry_run_cache(ctx) {
            Ok(cache) => {
                let next_version_text = next_version.to_string();
                let expected = ReleaseDryRunExpectations {
                    git_commit: &git_commit,
                    release_args: &release_args,
                    latest_tag_seen: latest_tag.as_deref(),
                    next_version: &next_version_text,
                    tag_name: &tag_name,
                };
                if let Some(reason) = release_dry_run_cache_rejection_reason(&cache, &expected) {
                    println!("   {YELLOW}Skipping dry-run cache reuse: {reason}{RESET}");
                } else {
                    ok("Reused dry-run cache; skipping validations");
                    reused_dry_run_cache = true;
                }
            }
            Err(error) => {
                println!("   {YELLOW}Skipping dry-run cache reuse: {error:#}{RESET}");
            }
        }
    }

    let tracked_dirty_paths_before_validation = if args.dry_run && !reused_dry_run_cache {
        git_tracked_dirty_paths(ctx)?
    } else {
        Vec::new()
    };
    let dirty_snapshot = if args.dry_run && !tracked_dirty_paths_before_validation.is_empty() {
        git_stash_create_snapshot(ctx)?
    } else {
        None
    };

    if !reused_dry_run_cache {
        let validation_result = {
            step("Running web and Rust validation in parallel");
            let web_ctx = ctx.clone();
            let rust_ctx = ctx.clone();
            let web = thread::spawn(move || run_weaver_web_validation(&web_ctx, "[web] "));
            let rust = thread::spawn(move || run_weaver_rust_validation(&rust_ctx, "[rust] "));
            let web_result = web
                .join()
                .map_err(|_| anyhow!("web validation thread panicked"))?;
            let rust_result = rust
                .join()
                .map_err(|_| anyhow!("rust validation thread panicked"))?;
            if let Err(error) = &web_result {
                warn(format!("Web validation failed: {error:#}"));
            }
            if let Err(error) = &rust_result {
                warn(format!("Rust validation failed: {error:#}"));
            }
            web_result?;
            rust_result?;
            ok("Parallel validation passed");
            Ok::<Vec<String>, anyhow::Error>(vec![
                "web_validation".to_string(),
                "rust_validation".to_string(),
            ])
        };

        if args.dry_run {
            let restore_result = restore_dry_run_tracked_changes(
                ctx,
                &tracked_dirty_paths_before_validation,
                dirty_snapshot.as_deref(),
            );

            match validation_result {
                Ok(validated_steps) => {
                    restore_result?;
                    write_release_dry_run_cache(
                        ctx,
                        &ReleaseDryRunCache {
                            success: true,
                            created_at: Utc::now().to_rfc3339(),
                            git_commit: git_commit.clone(),
                            branch: branch.clone(),
                            worktree_clean_at_start,
                            release_args: release_args.clone(),
                            latest_tag_seen: latest_tag.clone(),
                            next_version: next_version.to_string(),
                            tag_name: tag_name.clone(),
                            validated_steps,
                            failure_message: None,
                        },
                    )?;
                    println!(
                        "\n{YELLOW}{BOLD}Dry run complete — stopping before commit/tag/push.{RESET}"
                    );
                    println!("  Version {next_version} validated OK.");
                    println!("  Dry-run cache: {}", cache_path.display());
                    return Ok(());
                }
                Err(error) => {
                    restore_result?;
                    return Err(error);
                }
            }
        }

        validation_result?;
    }

    let cargo_toml = ctx.path("Cargo.toml");
    step(format!("Updating workspace version to {next_version}"));
    write_workspace_version(&cargo_toml, &next_version)?;
    ok(format!("Workspace version updated to {next_version}"));

    if reused_dry_run_cache {
        ok("Skipped post-bump cargo check via dry-run cache reuse");
    } else {
        step("Running cargo check after version bump");
        let mut cargo_check = ctx.command_in("cargo", &ctx.repo_root);
        cargo_check.arg("check");
        run_checked(&mut cargo_check)?;
        ok("cargo check passed");
    }

    step("Committing version bump");
    let mut changed = Vec::new();
    if changed_file(ctx, &cargo_toml)? {
        changed.push(cargo_toml.clone());
    }
    let cargo_lock = ctx.path("Cargo.lock");
    let npm_lock = ctx.path("apps/weaver-web/package-lock.json");
    if cargo_lock.exists() && changed_file(ctx, &cargo_lock)? {
        changed.push(cargo_lock.clone());
    }
    if npm_lock.exists() && changed_file(ctx, &npm_lock)? {
        changed.push(npm_lock.clone());
    }
    if !changed.is_empty() {
        let mut add = ctx.command_in("git", &ctx.repo_root);
        add.arg("add");
        add.args(&changed);
        run_checked(&mut add)?;
        let mut commit = ctx.command_in("git", &ctx.repo_root);
        commit.args([
            "commit",
            "-m",
            &format!("release: bump weaver to {next_version}"),
        ]);
        run_checked(&mut commit)?;
        ok("Committed version bump");
    } else {
        ok("Nothing to commit");
    }

    prune_weaver_release_history(ctx)?;

    step(format!("Creating signed tag {tag_name}"));
    let mut tag = ctx.command_in("git", &ctx.repo_root);
    tag.args(["tag", "-s", &tag_name, "-m", &format!("Release {tag_name}")]);
    run_checked(&mut tag)?;
    ok(format!("Tag {tag_name} created"));

    step("Pushing to origin");
    let mut push_branch = ctx.command_in("git", &ctx.repo_root);
    push_branch.args(["push", "origin", &branch]);
    run_checked(&mut push_branch)?;
    let mut push_tag = ctx.command_in("git", &ctx.repo_root);
    push_tag.args(["push", "origin", &tag_name]);
    run_checked(&mut push_tag)?;
    ok(format!("Pushed {branch} and tag {tag_name}"));

    println!("\n{GREEN}{BOLD}Released {tag_name}{RESET}");
    println!(
        "\n{YELLOW}{BOLD}Reminder:{RESET} Update the weaver tag reference in Scryer's Cargo.toml to {BOLD}{tag_name}{RESET}"
    );
    Ok(())
}

fn run_weaver_web_validation(ctx: &TaskContext, prefix: &'static str) -> Result<()> {
    let web_dir = ctx.path("apps/weaver-web");
    prefixed_step(prefix, "Running npm audit fix");
    let mut audit = ctx.command_in("npm", &web_dir);
    audit.args(["audit", "fix"]);
    run_streaming(&mut audit, prefix)?;
    prefixed_ok(prefix, "npm audit fix complete");

    prefixed_step(prefix, "Running TypeScript + ESLint");
    let mut lint = ctx.command_in("npm", &web_dir);
    lint.args(["run", "lint"]);
    run_streaming(&mut lint, prefix)?;
    prefixed_ok(prefix, "Web lint passed");

    prefixed_step(prefix, "Running web build");
    let mut build = ctx.command_in("npm", &web_dir);
    build.args(["run", "build"]);
    run_streaming(&mut build, prefix)?;
    prefixed_ok(prefix, "Web build passed");
    Ok(())
}

fn run_weaver_rust_validation(ctx: &TaskContext, prefix: &'static str) -> Result<()> {
    prefixed_step(prefix, "Running cargo fmt --all");
    let mut fmt_fix = ctx.command_in("cargo", &ctx.repo_root);
    fmt_fix.args(["fmt", "--all"]);
    run_streaming(&mut fmt_fix, prefix)?;
    prefixed_ok(prefix, "cargo fmt complete");

    prefixed_step(prefix, "Running cargo fmt --all --check");
    let mut fmt = ctx.command_in("cargo", &ctx.repo_root);
    fmt.args(["fmt", "--all", "--check"]);
    run_streaming(&mut fmt, prefix)?;
    prefixed_ok(prefix, "cargo fmt passed");

    prefixed_step(prefix, "Updating Cargo.lock (cargo update)");
    let mut update = ctx.command_in("cargo", &ctx.repo_root);
    update.arg("update");
    run_streaming(&mut update, prefix)?;
    prefixed_ok(prefix, "Cargo.lock updated");

    prefixed_step(prefix, "Running cargo audit");
    if !command_available("cargo-audit")? {
        warn("cargo-audit not installed — installing");
        let mut install = ctx.command_in("cargo", &ctx.repo_root);
        install.args(["install", "--locked", "cargo-audit"]);
        run_streaming(&mut install, prefix)?;
    }
    let mut audit = ctx.command_in("cargo", &ctx.repo_root);
    audit.arg("audit");
    run_streaming(&mut audit, prefix)?;
    prefixed_ok(prefix, "cargo audit passed");

    prefixed_step(
        prefix,
        "Running Rust tests (cargo nextest run --workspace --locked)",
    );
    if !command_available("cargo-nextest")? {
        warn("cargo-nextest not installed — installing");
        let mut install = ctx.command_in("cargo", &ctx.repo_root);
        install.args(["install", "--locked", "cargo-nextest"]);
        run_streaming(&mut install, prefix)?;
    }
    let mut nextest = ctx.command_in("cargo", &ctx.repo_root);
    nextest.args(["nextest", "run", "--workspace", "--locked"]);
    run_streaming(&mut nextest, prefix)?;
    prefixed_ok(prefix, "Rust tests passed");

    prefixed_step(prefix, "Running cargo clippy (linux ci target)");
    let mut clippy = ctx.command_in("cargo", &ctx.repo_root);
    clippy.args(["xtask", "ci", "clippy", "--linux-only"]);
    run_streaming(&mut clippy, prefix)?;
    prefixed_ok(prefix, "Clippy passed");
    Ok(())
}

fn prune_weaver_release_history(ctx: &TaskContext) -> Result<()> {
    const KEEP_RELEASES: usize = 4;
    step(format!(
        "Culling old release artifacts (keeping {KEEP_RELEASES} most recent)"
    ));

    let mut list = ctx.command_in("gh", &ctx.repo_root);
    list.args([
        "release",
        "list",
        "--limit",
        "100",
        "--json",
        "tagName,publishedAt",
    ]);
    let mut releases: Vec<GhRelease> = serde_json::from_str(&run_capture(&mut list)?)?;
    releases.sort_by_key(|release| release.published_at.clone());
    releases.reverse();

    let old_releases = releases
        .iter()
        .skip(KEEP_RELEASES)
        .map(|release| release.tag_name.clone())
        .collect::<Vec<_>>();
    if old_releases.is_empty() {
        ok("No old releases to cull");
    } else {
        for tag in &old_releases {
            println!("   deleting release: {tag}");
            let mut delete = ctx.command_in("gh", &ctx.repo_root);
            delete.args(["release", "delete", tag, "--yes"]);
            let _ = run_checked(&mut delete);
        }
        ok("Old releases deleted");
    }

    let mut package_check = ctx.command_in("gh", &ctx.repo_root);
    package_check.args(["api", "orgs/scryer-media/packages/container/weaver"]);
    if !run_status(&mut package_check)?.success() {
        ok("No GHCR package found — skipping Docker cleanup");
        return Ok(());
    }

    let mut versions = ctx.command_in("gh", &ctx.repo_root);
    versions.args([
        "api",
        "orgs/scryer-media/packages/container/weaver/versions",
        "--paginate",
        "--jq",
        ".[] | [(.id | tostring), .created_at, ((.metadata.container.tags | length) | tostring)] | @tsv",
    ]);
    let versions = run_capture(&mut versions)?;
    let mut rows = Vec::new();
    for row in versions.lines() {
        let mut fields = row.split('\t');
        let Some(id) = fields.next() else {
            continue;
        };
        let Some(created_at) = fields.next() else {
            continue;
        };
        let Some(tag_count) = fields.next() else {
            continue;
        };
        rows.push((
            id.to_string(),
            DateTime::parse_from_rfc3339(created_at)?.with_timezone(&Utc),
            tag_count.parse::<usize>()?,
        ));
    }
    let mut tagged = rows
        .iter()
        .filter(|(_, _, tag_count)| *tag_count > 0)
        .map(|(_, created_at, _)| *created_at)
        .collect::<Vec<_>>();
    tagged.sort_by_key(|created_at| *created_at);
    tagged.reverse();
    if tagged.len() < KEEP_RELEASES {
        ok(format!(
            "Fewer than {KEEP_RELEASES} Docker releases — nothing to cull"
        ));
        return Ok(());
    }
    let cutoff = tagged[KEEP_RELEASES - 1] - Duration::seconds(60);
    let mut deleted = 0;
    for (id, created_at, _) in rows {
        if created_at >= cutoff {
            continue;
        }
        let mut delete = ctx.command_in("gh", &ctx.repo_root);
        delete.args([
            "api",
            "--method",
            "DELETE",
            &format!("orgs/scryer-media/packages/container/weaver/versions/{id}"),
        ]);
        let _ = run_checked(&mut delete);
        deleted += 1;
    }
    if deleted == 0 {
        ok("No old Docker images to cull");
    } else {
        ok(format!("Deleted {deleted} old Docker image versions"));
    }
    Ok(())
}

fn log_target_for_component(component: &str) -> String {
    let component = component.replace('-', "_");
    match component.as_str() {
        "app" | "weaver" => "weaver".to_string(),
        "api" | "server_api" | "serverapi" => "weaver_server_api".to_string(),
        "core" | "server_core" | "servercore" | "scheduler" => "weaver_server_core".to_string(),
        "nntp" | "nzb" | "yenc" | "par2" | "rar" => format!("weaver_{component}"),
        "auth" | "bandwidth" | "categories" | "history" | "ingest" | "jobs" | "operations"
        | "persistence" | "pipeline" | "rss" | "runtime" | "servers" | "settings" => {
            format!("weaver_server_core::{component}")
        }
        _ => format!("weaver_server_core::{component}"),
    }
}

fn build_rust_log(spec: Option<&str>) -> String {
    let mut rust_log = "info,weaver::pipeline=debug".to_string();
    let Some(spec) = spec.filter(|value| !value.is_empty()) else {
        return rust_log;
    };

    let Some((level, targets)) = spec.split_once(':') else {
        return spec.to_string();
    };
    if targets == "all" {
        return level.to_string();
    }
    for target in targets.split(',').filter(|value| !value.is_empty()) {
        if target.contains("::") || target.contains('.') {
            rust_log.push(',');
            rust_log.push_str(target);
            rust_log.push('=');
            rust_log.push_str(level);
        } else {
            rust_log.push(',');
            rust_log.push_str(&log_target_for_component(target));
            rust_log.push('=');
            rust_log.push_str(level);
        }
    }
    rust_log
}

fn stop_port(port: u16) -> Result<()> {
    let mut lsof = Command::new("lsof");
    lsof.args(["-tiTCP", &port.to_string(), "-sTCP:LISTEN"]);
    let output = lsof.output()?;
    let pids = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if pids.is_empty() {
        return Ok(());
    }

    println!("==> Stopping listener(s) on port {port}...");
    let mut kill = Command::new("kill");
    kill.args(&pids);
    let _ = run_status(&mut kill);
    thread::sleep(std::time::Duration::from_secs(1));

    let mut lsof = Command::new("lsof");
    lsof.args(["-tiTCP", &port.to_string(), "-sTCP:LISTEN"]);
    let output = lsof.output()?;
    let stubborn = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if !stubborn.is_empty() {
        println!("    Port {port} still busy, sending SIGKILL...");
        let mut kill = Command::new("kill");
        kill.arg("-9").args(&stubborn);
        let _ = run_status(&mut kill);
        thread::sleep(std::time::Duration::from_secs(1));
    }
    Ok(())
}

fn wait_for_backend(pid: u32, port: u16, log_path: &Path) -> Result<()> {
    for _ in 0..60 {
        let status = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()?;
        if !status.success() {
            let tail = tail_file(log_path, 50)?;
            bail!(
                "Weaver failed to start. Tail of {}:\n{tail}",
                log_path.display()
            );
        }
        let status = Command::new("curl")
            .args(["-fsS", &format!("http://127.0.0.1:{port}/")])
            .status()?;
        if status.success() {
            return Ok(());
        }
        thread::sleep(std::time::Duration::from_secs(1));
    }

    let tail = tail_file(log_path, 50)?;
    bail!(
        "Timed out waiting for Weaver on http://127.0.0.1:{port}/. Tail of {}:\n{tail}",
        log_path.display()
    )
}

fn tail_file(path: &Path, lines: usize) -> Result<String> {
    let content = fs::read_to_string(path).unwrap_or_default();
    let collected = content.lines().rev().take(lines).collect::<Vec<_>>();
    Ok(collected.into_iter().rev().collect::<Vec<_>>().join("\n"))
}

fn load_state_encryption_key(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(fs::read_to_string(path)?.replace(['\r', '\n'], "")))
}

fn seed_runtime_dirs(db_path: &Path, data_dir: &Path) -> Result<()> {
    let intermediate_dir = data_dir.join("intermediate");
    let complete_dir = data_dir.join("complete");
    fs::create_dir_all(&intermediate_dir)?;
    fs::create_dir_all(&complete_dir)?;

    let escape = |value: &Path| value.display().to_string().replace('\'', "''");
    let sql = format!(
        "INSERT INTO settings(key, value) VALUES ('data_dir', '{data_dir}') \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value; \
         INSERT INTO settings(key, value) VALUES ('intermediate_dir', '{intermediate_dir}') \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value; \
         INSERT INTO settings(key, value) VALUES ('complete_dir', '{complete_dir}') \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
        data_dir = escape(data_dir),
        intermediate_dir = escape(&intermediate_dir),
        complete_dir = escape(&complete_dir),
    );

    let mut command = Command::new("sqlite3");
    command.arg(db_path).arg(sql);
    run_checked(&mut command)
}

fn bootstrap_state_dir(
    ctx: &TaskContext,
    state_dir: &Path,
    backend_port: u16,
    backend_log: &Path,
    encryption_key: Option<&str>,
) -> Result<()> {
    if state_dir.join("weaver.db").exists() {
        return Ok(());
    }

    println!("==> Bootstrapping dev state in {}", state_dir.display());
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(backend_log)?;
    let log_err = log.try_clone()?;
    let mut child = ctx.command_in("cargo", &ctx.repo_root);
    child
        .env("RUST_LOG", "warn")
        .args([
            "run",
            "-p",
            "weaver",
            "--",
            "--config",
            &state_dir.display().to_string(),
            "serve",
            "--port",
            &backend_port.to_string(),
        ])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    if let Some(key) = encryption_key {
        child.env("WEAVER_ENCRYPTION_KEY", key);
    }
    let mut child = child.spawn()?;
    let pid = child.id();
    wait_for_backend(pid, backend_port, backend_log)?;
    let _ = child.kill();
    let _ = child.wait();
    Ok(())
}

fn run_dev(ctx: &TaskContext, args: DevArgs) -> Result<()> {
    require_command("sqlite3")?;
    let web_dir = ctx.path("apps/weaver-web");
    let backend_port = std::env::var("WEAVER_DEV_BACKEND_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(9090);
    let frontend_port = std::env::var("WEAVER_DEV_FRONTEND_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(5173);
    let backend_log = PathBuf::from(
        std::env::var("WEAVER_DEV_BACKEND_LOG")
            .unwrap_or_else(|_| "/tmp/weaver-dev-backend.log".to_string()),
    );
    let state_dir = PathBuf::from(
        std::env::var("WEAVER_DEV_STATE_DIR")
            .unwrap_or_else(|_| ctx.path("tmp/dev-instance").display().to_string()),
    );
    let state_key_file = state_dir.join("encryption.key");
    let rust_log = build_rust_log(args.target.as_deref());

    let keep_data = std::env::var("WEAVER_DEV_KEEP_DATA")
        .map(|value| value == "1")
        .unwrap_or(false);
    let data_dir = if let Ok(data_dir) = std::env::var("WEAVER_DEV_DATA_DIR") {
        (PathBuf::from(data_dir), None)
    } else {
        fs::create_dir_all(ctx.path("tmp"))?;
        for entry in fs::read_dir(ctx.path("tmp"))? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && entry.file_name().to_string_lossy().starts_with("dev-data.")
            {
                let _ = fs::remove_dir_all(entry.path());
            }
        }
        let temp = tempfile::Builder::new()
            .prefix("dev-data.")
            .tempdir_in(ctx.path("tmp"))?;
        (temp.path().to_path_buf(), Some(temp))
    };
    let (data_dir, temp_dir) = data_dir;

    stop_port(backend_port)?;
    stop_port(frontend_port)?;

    if let Some(parent) = backend_log.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&backend_log, "")?;
    fs::create_dir_all(&state_dir)?;

    let encryption_key = load_state_encryption_key(&state_key_file)?;
    bootstrap_state_dir(
        ctx,
        &state_dir,
        backend_port,
        &backend_log,
        encryption_key.as_deref(),
    )?;
    let encryption_key = load_state_encryption_key(&state_key_file)?;
    seed_runtime_dirs(&state_dir.join("weaver.db"), &data_dir)?;

    println!("==> Starting Weaver backend with cargo run on :{backend_port}");
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&backend_log)?;
    let log_err = log.try_clone()?;
    let mut backend = ctx.command_in("cargo", &ctx.repo_root);
    backend
        .env("RUST_LOG", &rust_log)
        .args([
            "run",
            "-p",
            "weaver",
            "--",
            "--config",
            &state_dir.display().to_string(),
            "serve",
            "--port",
            &backend_port.to_string(),
        ])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    if let Some(key) = encryption_key.as_deref() {
        backend.env("WEAVER_ENCRYPTION_KEY", key);
    }
    let mut backend = backend.spawn()?;
    let backend_pid = backend.id();
    wait_for_backend(backend_pid, backend_port, &backend_log)?;

    println!("==> Weaver backend ready");
    println!("    Backend:  http://127.0.0.1:{backend_port}");
    println!("    Frontend: http://127.0.0.1:{frontend_port}");
    println!("    State:    {}", state_dir.display());
    println!("    Data:     {}", data_dir.display());
    println!("    Log:      tail -f {}", backend_log.display());
    println!();
    println!("==> Starting Vite dev server with live updates...");

    let mut vite = ctx.command_in("npm", &web_dir);
    vite.args([
        "run",
        "dev",
        "--",
        "--host",
        "0.0.0.0",
        "--port",
        &frontend_port.to_string(),
    ]);
    let result = run_status(&mut vite);

    let _ = backend.kill();
    let _ = backend.wait();
    if !keep_data {
        drop(temp_dir);
    }

    result?;
    Ok(())
}

fn run_deploy_local(ctx: &TaskContext, args: DeployLocalArgs) -> Result<()> {
    let port = args.port;
    let log_file = if port == 9090 {
        PathBuf::from("/tmp/weaver.log")
    } else {
        PathBuf::from(format!("/tmp/weaver-{port}.log"))
    };
    let binary = ctx.path("target/release/weaver");
    let debug_binary = ctx.path("target/debug/weaver");
    let local_binaries = vec![binary.clone(), debug_binary];
    let pid_file = if port == 9090 {
        ctx.path("tmp/weaver-local.pid")
    } else {
        ctx.repo_root.join(format!("tmp/weaver-local-{port}.pid"))
    };
    let rust_log = build_rust_log(args.target.as_deref());
    let config_file = ctx.path("weaver.toml");
    let backup_dir = ctx.path("tmp/config-backups");
    let mut port_probe = PortProbeState::default();

    if config_file.exists() {
        fs::create_dir_all(&backup_dir)?;
        let backup = backup_dir.join(format!(
            "weaver.toml.{}",
            Utc::now().format("%Y%m%d-%H%M%S")
        ));
        fs::copy(&config_file, backup)?;
        let mut backups = fs::read_dir(&backup_dir)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        backups.sort();
        backups.reverse();
        for stale in backups.into_iter().skip(5) {
            let _ = fs::remove_file(stale);
        }
    }

    println!("==> Stopping existing weaver on port {port}...");
    if let Some(pid) = fs::read_to_string(&pid_file)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        && let Some(command_line) = read_pid_command_line(&pid)?
        && command_matches_local_weaver(&command_line, &local_binaries)
    {
        signal_pids(ctx, &[pid], None)?;
    }

    let initial_port_pids = list_port_pids(ctx, port, &mut port_probe)?;
    let mut weaver_port_pids = BTreeSet::new();
    let mut foreign_port_owners = Vec::new();
    for pid in initial_port_pids {
        match read_pid_command_line(&pid)? {
            Some(command_line) if command_matches_local_weaver(&command_line, &local_binaries) => {
                weaver_port_pids.insert(pid);
            }
            Some(command_line) => foreign_port_owners.push((pid, command_line)),
            None => foreign_port_owners.push((pid, "<unknown command>".to_string())),
        }
    }
    if !foreign_port_owners.is_empty() {
        let owners = foreign_port_owners
            .into_iter()
            .map(|(pid, command)| format!("pid {pid}: {command}"))
            .collect::<Vec<_>>()
            .join("\n");
        bail!(
            "Refusing to stop non-Weaver process(es) on port {port}.\n{owners}\nFree the port or choose a different --port before running deploy local."
        );
    }
    let weaver_port_pids = weaver_port_pids.into_iter().collect::<Vec<_>>();
    signal_pids(ctx, &weaver_port_pids, None)?;
    thread::sleep(std::time::Duration::from_secs(1));

    let remaining_weaver_pids = list_port_pids(ctx, port, &mut port_probe)?
        .into_iter()
        .filter_map(|pid| match read_pid_command_line(&pid) {
            Ok(Some(command_line))
                if command_matches_local_weaver(&command_line, &local_binaries) =>
            {
                Some(Ok(pid))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>>>()?;
    if !remaining_weaver_pids.is_empty() {
        println!("    Weaver still owns port {port}, sending SIGKILL...");
        signal_pids(ctx, &remaining_weaver_pids, Some("-9"))?;
        thread::sleep(std::time::Duration::from_secs(1));
    }
    let remaining_port_pids = list_port_pids(ctx, port, &mut port_probe)?;
    if !remaining_port_pids.is_empty() {
        let owners = remaining_port_pids
            .into_iter()
            .map(|pid| {
                let command =
                    read_pid_command_line(&pid)?.unwrap_or_else(|| "<unknown command>".to_string());
                Ok(format!("pid {pid}: {command}"))
            })
            .collect::<Result<Vec<_>>>()?
            .join("\n");
        bail!(
            "Port {port} is still occupied after stopping local Weaver.\n{owners}\nRefusing to kill non-Weaver processes."
        );
    }

    println!("==> Cleaning old logs...");
    fs::write(&log_file, "")?;
    if let Some(parent) = pid_file.parent() {
        fs::create_dir_all(parent)?;
    }

    if args.no_build {
        if !binary.exists() {
            bail!(
                "--no-build requested but {} is missing; run a normal `cargo xtask deploy local` first to produce the release binary",
                binary.display()
            );
        }
        println!("==> Skipping frontend and release build (--no-build)");
    } else {
        println!("==> Building frontend...");
        let mut frontend = ctx.command_in("npm", &ctx.path("apps/weaver-web"));
        frontend.args(["run", "build"]);
        run_checked(&mut frontend)?;

        println!("==> Building release...");
        let mut build = ctx.command_in("cargo", &ctx.repo_root);
        build
            .env("RUSTFLAGS", "-C target-cpu=native")
            .args(["build", "--release", "-p", "weaver"]);
        run_checked(&mut build)?;
    }

    println!(
        "==> Starting weaver (RUST_LOG={rust_log}, logging to {})...",
        log_file.display()
    );
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)?;
    let log_err = log.try_clone()?;
    let mut child = ctx.command(&binary);
    child
        .env("RUST_LOG", &rust_log)
        .arg("serve")
        .arg("--port")
        .arg(port.to_string())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    let mut child = child.spawn()?;
    let child_id = child.id();
    if let Err(error) = wait_for_backend(child_id, port, &log_file) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }

    fs::write(&pid_file, format!("{child_id}\n"))?;
    println!("==> Weaver running (PID={child_id}, port={port})");
    println!("==> Log: tail -f {}", log_file.display());
    let preview = tail_file(&log_file, 10)?;
    if !preview.is_empty() {
        println!("{preview}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_release_dry_run_cache() -> ReleaseDryRunCache {
        ReleaseDryRunCache {
            success: true,
            created_at: "2026-05-02T00:00:00Z".to_string(),
            git_commit: "abc123".to_string(),
            branch: "main".to_string(),
            worktree_clean_at_start: true,
            release_args: "bump:patch".to_string(),
            latest_tag_seen: Some("weaver-v0.2.7".to_string()),
            next_version: "0.2.8".to_string(),
            tag_name: "weaver-v0.2.8".to_string(),
            validated_steps: vec!["web_validation".to_string(), "rust_validation".to_string()],
            failure_message: None,
        }
    }

    fn sample_release_dry_run_expectations<'a>() -> ReleaseDryRunExpectations<'a> {
        ReleaseDryRunExpectations {
            git_commit: "abc123",
            release_args: "bump:patch",
            latest_tag_seen: Some("weaver-v0.2.7"),
            next_version: "0.2.8",
            tag_name: "weaver-v0.2.8",
        }
    }

    #[test]
    fn release_args_signature_uses_bump_mode_when_version_not_explicit() {
        assert_eq!(
            release_args_signature(None, VersionBump::Minor),
            "bump:minor"
        );
    }

    #[test]
    fn release_args_signature_uses_explicit_version_when_present() {
        let version = Version::parse("1.2.3").unwrap();
        assert_eq!(
            release_args_signature(Some(&version), VersionBump::Patch),
            "version:1.2.3"
        );
    }

    #[test]
    fn release_dry_run_cache_round_trips_through_json() {
        let cache = sample_release_dry_run_cache();
        let json = serde_json::to_string(&cache).unwrap();
        let decoded: ReleaseDryRunCache = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, cache);
    }

    #[test]
    fn release_dry_run_cache_rejects_unsuccessful_prior_run() {
        let mut cache = sample_release_dry_run_cache();
        cache.success = false;
        let reason =
            release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
        assert_eq!(
            reason.as_deref(),
            Some("previous dry run did not complete successfully")
        );
    }

    #[test]
    fn release_dry_run_cache_rejects_dirty_start() {
        let mut cache = sample_release_dry_run_cache();
        cache.worktree_clean_at_start = false;
        let reason =
            release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
        assert_eq!(
            reason.as_deref(),
            Some("dry run started from a dirty worktree")
        );
    }

    #[test]
    fn release_dry_run_cache_rejects_commit_mismatch() {
        let mut cache = sample_release_dry_run_cache();
        cache.git_commit = "def456".to_string();
        let reason =
            release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
        assert_eq!(reason.as_deref(), Some("HEAD commit changed since dry run"));
    }

    #[test]
    fn release_dry_run_cache_rejects_args_mismatch() {
        let mut cache = sample_release_dry_run_cache();
        cache.release_args = "bump:minor".to_string();
        let reason =
            release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
        assert_eq!(
            reason.as_deref(),
            Some("release arguments changed since dry run")
        );
    }

    #[test]
    fn release_dry_run_cache_rejects_latest_tag_mismatch() {
        let mut cache = sample_release_dry_run_cache();
        cache.latest_tag_seen = Some("weaver-v0.2.6".to_string());
        let reason =
            release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
        assert_eq!(
            reason.as_deref(),
            Some("latest release tag changed since dry run")
        );
    }

    #[test]
    fn release_dry_run_cache_rejects_next_tag_mismatch() {
        let mut cache = sample_release_dry_run_cache();
        cache.tag_name = "weaver-v0.2.9".to_string();
        let reason =
            release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
        assert_eq!(
            reason.as_deref(),
            Some("computed release tag changed since dry run")
        );
    }

    #[test]
    fn release_dry_run_cache_accepts_matching_inputs() {
        let cache = sample_release_dry_run_cache();
        let reason =
            release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
        assert!(reason.is_none());
    }
}
