use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdout, Command, ExitStatus, Output, Stdio};
#[cfg(unix)]
use std::sync::atomic::{AtomicI32, Ordering};
use std::thread;
use std::time::{Duration as StdDuration, Instant};
use toml_edit::{DocumentMut, value};

mod monitor;
mod par2_kit;
mod perf;
mod pgo;
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
const BACKEND_SHUTDOWN_GRACE_PERIOD: StdDuration = StdDuration::from_secs(5);
const RELEASE_ALLOWED_CARGO_AUDIT_IDS: &[ReleaseAuditAllow] = &[
    ReleaseAuditAllow {
        id: "RUSTSEC-2023-0071",
        expires_on: "2026-09-30",
        reason: "transitive dependency still under review; release must revisit before expiry",
    },
    ReleaseAuditAllow {
        id: "RUSTSEC-2025-0134",
        expires_on: "2026-09-30",
        reason: "slab advisory suppression is temporary while upstream dependency path is updated",
    },
];
const RELEASE_LOCAL_PATH_TOKENS: &[&str] = &[
    concat!("/", "Users/"),
    concat!("/", "home/"),
    concat!("C:\\", "Users\\"),
    concat!("C:/", "Users/"),
];
const RELEASE_SIBLING_E2E_TOKENS: &[&str] = &[concat!("..", "/e2e/"), concat!("..\\", "e2e\\")];
const WINGET_PACKAGE_IDENTIFIER: &str = "ScryerMedia.Weaver";
const WINGET_PACKAGE_NAME: &str = "Weaver";
const WINGET_MONIKER: &str = "weaver-usenet";
const WINGET_PORTABLE_COMMAND_ALIAS: &str = "weaver";
const WINGET_MANIFEST_VERSION: &str = "1.12.0";
const WINGET_WINDOWS_X64_ASSET: &str = "weaver-windows-x86_64.zip";
const WINGET_WINDOWS_ARM64_ASSET: &str = "weaver-windows-arm64.zip";

struct ReleaseAuditAllow {
    id: &'static str,
    expires_on: &'static str,
    reason: &'static str,
}

#[cfg(unix)]
static SIGNAL_FORWARD_PROCESS_GROUP: AtomicI32 = AtomicI32::new(0);

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
    Serve(ServeArgs),
    Deploy(DeployArgs),
    Monitor,
    Profile(ProfileArgs),
    Perf(PerfArgs),
    Pgo(PgoArgs),
    Par2(Par2Args),
    PrintRustflags(PrintRustflagsArgs),
    VerifyCryptoTarget(VerifyCryptoTargetArgs),
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
    Audit,
    Clippy(ClippyArgs),
    Winget(WingetArgs),
}

#[derive(Args)]
struct ClippyArgs {
    #[arg(long)]
    linux_only: bool,
}

#[derive(Args)]
struct WingetArgs {
    #[arg(long, help = "Weaver version without the weaver-v prefix")]
    version: String,
    #[arg(long, help = "Release tag that owns the Windows assets")]
    tag: Option<String>,
    #[arg(long, default_value = "scryer-media/weaver")]
    repository: String,
    #[arg(long, default_value = "release-artifacts")]
    artifacts_dir: PathBuf,
    #[arg(long, default_value = "target/winget")]
    output_dir: PathBuf,
    #[arg(long, help = "Release date in YYYY-MM-DD format; defaults to today")]
    release_date: Option<String>,
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long, help = "Reset the dev SQLite database before starting Weaver")]
    clean: bool,
    target: Option<String>,
}

#[derive(Args)]
struct PrintRustflagsArgs {
    #[arg(long)]
    target: Option<String>,
    #[arg(long)]
    lane: Option<ReleaseLane>,
    #[arg(long)]
    ci: bool,
}

#[derive(Args)]
struct VerifyCryptoTargetArgs {
    #[arg(long)]
    target: String,
    #[arg(long)]
    lane: ReleaseLane,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ReleaseLane {
    Portable,
    Haswell,
    #[value(name = "cortex-a76")]
    CortexA76,
    #[value(name = "apple-m1")]
    AppleM1,
}

impl ReleaseLane {
    fn as_suffix(self) -> &'static str {
        match self {
            Self::Portable => "portable",
            Self::Haswell => "haswell",
            Self::CortexA76 => "cortex-a76",
            Self::AppleM1 => "apple-m1",
        }
    }
}

#[cfg(unix)]
struct SignalForwarder {
    previous_sigint: libc::sigaction,
    previous_sigterm: libc::sigaction,
}

#[cfg(unix)]
impl Drop for SignalForwarder {
    fn drop(&mut self) {
        SIGNAL_FORWARD_PROCESS_GROUP.store(0, Ordering::Relaxed);
        unsafe {
            let _ = libc::sigaction(libc::SIGINT, &self.previous_sigint, std::ptr::null_mut());
            let _ = libc::sigaction(libc::SIGTERM, &self.previous_sigterm, std::ptr::null_mut());
        }
    }
}

#[cfg(not(unix))]
struct SignalForwarder;

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

#[derive(Args)]
struct PgoArgs {
    #[command(subcommand)]
    command: PgoCommand,
}

#[derive(Subcommand)]
enum PgoCommand {
    Collect(PgoCollectArgs),
}

#[derive(Args)]
struct PgoCollectArgs {
    /// Absolute or repo-relative path to the sibling e2e checkout.
    #[arg(long)]
    e2e_dir: Option<PathBuf>,
    /// Absolute or repo-relative output directory for the instrumented build,
    /// raw profiles, merged profdata, and e2e artifacts.
    #[arg(long)]
    out_dir: Option<PathBuf>,
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
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
    #[serde(default)]
    validated_tree: Option<String>,
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
    validated_tree: &'a str,
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
            CiCommand::Audit => run_cargo_audit_validation(&ctx, "ci-audit"),
            CiCommand::Clippy(args) => run_clippy_ci(&ctx, args),
            CiCommand::Winget(args) => run_ci_winget(&ctx, args),
        },
        Commands::Serve(args) => run_serve(&ctx, args),
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
        Commands::Pgo(args) => match args.command {
            PgoCommand::Collect(args) => pgo::run_collect(&ctx, args),
        },
        Commands::Par2(args) => match args.command {
            Par2Command::FinalizeKit(args) => par2_kit::run(&ctx, args.args),
        },
        Commands::PrintRustflags(args) => run_print_rustflags(args),
        Commands::VerifyCryptoTarget(args) => run_verify_crypto_target(args),
    }
}

fn run_print_rustflags(args: PrintRustflagsArgs) -> Result<()> {
    let flags = if args.ci {
        let target = args
            .target
            .as_deref()
            .context("--target is required with --ci")?;
        let lane = args.lane.context("--lane is required with --ci")?;
        ci_rustflags_for_target(target, lane)?
    } else {
        if args.lane.is_some() {
            bail!("--lane is only supported with --ci");
        }
        host_local_rustflags()
    };
    println!("{flags}");
    Ok(())
}

fn run_verify_crypto_target(args: VerifyCryptoTargetArgs) -> Result<()> {
    verify_crypto_target(&args.target, args.lane)?;
    println!(
        "native crypto config ok for {} ({})",
        args.target,
        args.lane.as_suffix()
    );
    Ok(())
}

fn host_local_rustflags() -> String {
    local_rustflags_for_host(std::env::consts::ARCH, std::env::consts::OS)
}

fn local_rustflags_for_host(host_arch: &str, host_os: &str) -> String {
    let mut flags = vec!["-C target-cpu=native".to_string()];
    if host_arch == "aarch64" {
        flags.push("--cfg aes_armv8".to_string());
        if host_os != "windows" {
            flags.push("--cfg chacha20_force_neon".to_string());
        }
    }
    flags.join(" ")
}

fn ci_rustflags_for_target(target: &str, lane: ReleaseLane) -> Result<String> {
    if !release_lane_supported_for_target(target, lane) {
        bail!(
            "unsupported lane {} for rustflags target {target}",
            lane.as_suffix()
        );
    }

    let mut flags = Vec::new();
    if target.starts_with("x86_64-pc-windows-") {
        if lane == ReleaseLane::Haswell {
            flags.push("-C target-cpu=haswell");
        }
        flags.push("-C target-feature=+crt-static");
        flags.push("-C linker=rust-lld");
    } else if target.starts_with("x86_64-") {
        if lane == ReleaseLane::Haswell {
            flags.push("-C target-cpu=haswell");
        }
    } else if target.starts_with("aarch64-apple-") {
        if lane == ReleaseLane::AppleM1 {
            flags.push("-C target-cpu=apple-m1");
        }
        flags.push("--cfg aes_armv8");
        flags.push("--cfg chacha20_force_neon");
    } else if target.starts_with("aarch64-") && target.contains("-windows-") {
        if lane == ReleaseLane::CortexA76 {
            flags.push("-C target-cpu=cortex-a76");
        }
        flags.push("--cfg aes_armv8");
        flags.push("-C target-feature=+crt-static");
        flags.push("-C linker=rust-lld");
    } else if target.starts_with("aarch64-") && target.contains("-linux-") {
        if lane == ReleaseLane::CortexA76 {
            flags.push("-C target-cpu=cortex-a76");
        }
        flags.push("--cfg aes_armv8");
        flags.push("--cfg chacha20_force_neon");
    } else {
        bail!("unsupported target for rustflags: {target}");
    }

    Ok(flags.join(" "))
}

fn native_crypto_supported_target(target: &str) -> bool {
    matches!(
        target,
        "x86_64-apple-darwin"
            | "aarch64-apple-darwin"
            | "x86_64-unknown-linux-musl"
            | "aarch64-unknown-linux-musl"
            | "x86_64-pc-windows-msvc"
            | "aarch64-pc-windows-msvc"
    )
}

fn release_lane_supported_for_target(target: &str, lane: ReleaseLane) -> bool {
    match lane {
        ReleaseLane::Portable => {
            native_crypto_supported_target(target) && !target.starts_with("x86_64-apple-")
        }
        ReleaseLane::Haswell => {
            target.starts_with("x86_64-apple-") || target.starts_with("x86_64-unknown-linux-musl")
        }
        ReleaseLane::CortexA76 => target.starts_with("aarch64-unknown-linux-musl"),
        ReleaseLane::AppleM1 => target.starts_with("aarch64-apple-"),
    }
}

fn verify_crypto_target(target: &str, lane: ReleaseLane) -> Result<()> {
    if !native_crypto_supported_target(target) {
        bail!("unsupported native crypto target: {target}");
    }
    if !release_lane_supported_for_target(target, lane) {
        bail!(
            "unsupported lane {} for native crypto target {target}",
            lane.as_suffix()
        );
    }

    let flags = ci_rustflags_for_target(target, lane)?;
    let has = |needle: &str| flags.split_whitespace().any(|token| token == needle);
    let has_prefix = |prefix: &str| {
        flags
            .split_whitespace()
            .any(|token| token.starts_with(prefix))
    };

    if target.starts_with("x86_64-apple-") || target.starts_with("x86_64-unknown-linux-musl") {
        if lane == ReleaseLane::Haswell && !has("target-cpu=haswell") {
            bail!("missing haswell target-cpu for {target}: {flags}");
        }
        if lane == ReleaseLane::Portable && has("target-cpu=haswell") {
            bail!("unexpected haswell target-cpu for portable {target}: {flags}");
        }
        if has("aes_armv8") || has("chacha20_force_neon") || has("linker=rust-lld") {
            bail!("unexpected ARM or Windows flags for {target}: {flags}");
        }
    } else if target.starts_with("x86_64-pc-windows-") {
        if !has("linker=rust-lld") {
            bail!("missing Windows x86_64 linker flag for {target}: {flags}");
        }
        if !has("target-feature=+crt-static") {
            bail!("missing Windows x86_64 static CRT flag for {target}: {flags}");
        }
        if has_prefix("target-cpu=") {
            bail!("unexpected Windows x86_64 optimized target-cpu for {target}: {flags}");
        }
        if has("aes_armv8") || has("chacha20_force_neon") {
            bail!("unexpected ARM flags for {target}: {flags}");
        }
    } else if target.starts_with("aarch64-apple-") {
        if !has("aes_armv8") || !has("chacha20_force_neon") {
            bail!("missing Apple ARM crypto flags for {target}: {flags}");
        }
        if lane == ReleaseLane::AppleM1 && !has("target-cpu=apple-m1") {
            bail!("missing Apple ARM crypto flags for {target}: {flags}");
        }
        if lane == ReleaseLane::Portable && has("target-cpu=apple-m1") {
            bail!("unexpected Apple ARM optimized target-cpu for {target}: {flags}");
        }
        if has("linker=rust-lld") {
            bail!("unexpected Windows linker flag for {target}: {flags}");
        }
    } else if target.starts_with("aarch64-unknown-linux-musl") {
        if !has("aes_armv8") || !has("chacha20_force_neon") {
            bail!("missing Linux ARM crypto flags for {target}: {flags}");
        }
        if lane == ReleaseLane::CortexA76 && !has("target-cpu=cortex-a76") {
            bail!("missing Linux ARM optimized target-cpu for {target}: {flags}");
        }
        if lane == ReleaseLane::Portable && has("target-cpu=cortex-a76") {
            bail!("unexpected Linux ARM optimized target-cpu for {target}: {flags}");
        }
        if has("linker=rust-lld") {
            bail!("unexpected Windows linker flag for {target}: {flags}");
        }
    } else if target.starts_with("aarch64-pc-windows-") {
        if !has("aes_armv8") || !has("linker=rust-lld") {
            bail!("missing Windows ARM crypto/linker flags for {target}: {flags}");
        }
        if !has("target-feature=+crt-static") {
            bail!("missing Windows ARM static CRT flag for {target}: {flags}");
        }
        if has_prefix("target-cpu=") {
            bail!("unexpected Windows ARM optimized target-cpu for {target}: {flags}");
        }
        if has("chacha20_force_neon") {
            bail!("unexpected non-Windows ARM neon flag for {target}: {flags}");
        }
    } else {
        bail!("no crypto verification rules for {target}");
    }

    Ok(())
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

fn git_tracked_files(ctx: &TaskContext) -> Result<Vec<PathBuf>> {
    let mut command = ctx.command_in("git", &ctx.repo_root);
    command.args(["ls-files", "-z"]);
    let debug = format!("{command:?}");
    let output = command.output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("command failed: {debug}\n{stderr}");
    }

    Ok(output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| ctx.path(String::from_utf8_lossy(entry).as_ref()))
        .collect())
}

fn scan_release_hygiene_content(path: &Path, content: &str) -> Vec<String> {
    let mut violations = Vec::new();

    for (line_number, line) in content.lines().enumerate() {
        let line_number = line_number + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if RELEASE_LOCAL_PATH_TOKENS
            .iter()
            .any(|token| line.contains(token))
        {
            violations.push(format!(
                "{}:{line_number}: local absolute path reference: {trimmed}",
                path.display()
            ));
        }

        if RELEASE_SIBLING_E2E_TOKENS
            .iter()
            .any(|token| line.contains(token))
        {
            violations.push(format!(
                "{}:{line_number}: sibling e2e repo reference: {trimmed}",
                path.display()
            ));
        }
    }

    violations
}

fn release_hygiene_violations(ctx: &TaskContext) -> Result<Vec<String>> {
    let mut violations = Vec::new();

    for path in git_tracked_files(ctx)? {
        let Ok(bytes) = fs::read(&path) else {
            continue;
        };
        if bytes.contains(&0) {
            continue;
        }

        let relative = path
            .strip_prefix(&ctx.repo_root)
            .unwrap_or(path.as_path())
            .to_path_buf();
        let content = String::from_utf8_lossy(&bytes);
        violations.extend(scan_release_hygiene_content(&relative, &content));
    }

    violations.sort();
    Ok(violations)
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

fn current_tracked_tree(ctx: &TaskContext) -> Result<String> {
    if git_status_porcelain(ctx)?.trim().is_empty() {
        return git_capture(ctx, &["rev-parse", "HEAD^{tree}"])
            .map(|value| value.trim().to_string());
    }

    let snapshot = git_stash_create_snapshot(ctx)?
        .ok_or_else(|| anyhow!("failed to snapshot tracked worktree state"))?;
    git_capture(ctx, &["rev-parse", &format!("{snapshot}^{{tree}}")])
        .map(|value| value.trim().to_string())
}

fn new_tracked_dirty_paths(
    ctx: &TaskContext,
    tracked_dirty_paths_before: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    let tracked_dirty_before = tracked_dirty_paths_before
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let tracked_dirty_after = git_tracked_dirty_paths(ctx)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    Ok(tracked_dirty_after
        .difference(&tracked_dirty_before)
        .cloned()
        .collect())
}

fn commit_release_generated_changes(
    ctx: &TaskContext,
    paths: &[PathBuf],
    message: &str,
) -> Result<bool> {
    if paths.is_empty() {
        return Ok(false);
    }

    let mut add = ctx.command_in("git", &ctx.repo_root);
    add.arg("add");
    add.args(paths);
    run_checked(&mut add)?;

    let mut commit = ctx.command_in("git", &ctx.repo_root);
    commit.args(["commit", "-m", message]);
    run_checked(&mut commit)?;
    Ok(true)
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
    if cache.validated_tree.as_deref() != Some(expected.validated_tree) {
        return Some("prepared tracked tree changed since dry run".to_string());
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

fn linux_clippy_defaults(host_target: &str) -> (&'static str, &'static str) {
    if host_target.starts_with("aarch64-") {
        ("aarch64-unknown-linux-musl", "linux/arm64")
    } else {
        ("x86_64-unknown-linux-musl", "linux/amd64")
    }
}

fn cargo_target_env_key(target: &str, suffix: &str) -> String {
    format!(
        "CARGO_TARGET_{}_{}",
        target.replace('-', "_").to_ascii_uppercase(),
        suffix
    )
}

fn run_clippy_ci(ctx: &TaskContext, args: ClippyArgs) -> Result<()> {
    let mut rustc = ctx.command("rustc");
    rustc.arg("-vV");
    let host_target = run_capture(&mut rustc)?
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or_else(|| anyhow!("failed to determine host target"))?
        .trim()
        .to_string();
    let (default_linux_target, default_linux_platform) = linux_clippy_defaults(&host_target);
    let linux_target =
        std::env::var("WEAVER_LINUX_CLIPPY_TARGET").unwrap_or_else(|_| default_linux_target.into());
    let linux_image = std::env::var("WEAVER_LINUX_CLIPPY_IMAGE")
        .unwrap_or_else(|_| "rust:1.96-bookworm".to_string());
    let linux_platform = std::env::var("WEAVER_LINUX_CLIPPY_PLATFORM")
        .unwrap_or_else(|_| default_linux_platform.into());
    let linux_rustflags = ci_rustflags_for_target(&linux_target, ReleaseLane::Portable)?;
    let linux_rustflags_env = format!(
        "{}={linux_rustflags}",
        cargo_target_env_key(&linux_target, "RUSTFLAGS")
    );
    let linux_linker_env = format!("{}=musl-gcc", cargo_target_env_key(&linux_target, "LINKER"));
    let linux_clippy_script = format!(
        "set -euo pipefail; export PATH=\"/usr/local/cargo/bin:$PATH\"; apt-get update >/dev/null; apt-get install -y --no-install-recommends musl-tools >/dev/null; /usr/local/cargo/bin/rustup component add clippy; /usr/local/cargo/bin/rustup target add {linux_target}; cargo clippy --workspace --lib --bins --tests --examples --target {linux_target} -- -D warnings"
    );

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
                "-e",
                &linux_rustflags_env,
                "-e",
                &linux_linker_env,
                &linux_image,
                "bash",
                "-lc",
                &linux_clippy_script,
            ]);
            run_checked(&mut command)?;
        } else if command_available("musl-gcc")? {
            let mut target_add = ctx.command("rustup");
            target_add.args(["target", "add", &linux_target]);
            run_checked(&mut target_add)?;

            let mut command = ctx.command_in("cargo", &ctx.repo_root);
            command.env(
                cargo_target_env_key(&linux_target, "RUSTFLAGS"),
                &linux_rustflags,
            );
            command.env(cargo_target_env_key(&linux_target, "LINKER"), "musl-gcc");
            command.args([
                "clippy",
                "--workspace",
                "--lib",
                "--bins",
                "--tests",
                "--examples",
                "--target",
                &linux_target,
                "--",
                "-D",
                "warnings",
            ]);
            run_checked(&mut command)?;
        } else {
            bail!("cannot run Linux CI clippy locally; install Docker or musl-gcc");
        }
    }

    Ok(())
}

fn run_ci_winget(ctx: &TaskContext, args: WingetArgs) -> Result<()> {
    step("Preparing WinGet portable manifests");
    let version = normalize_winget_version(&args.version)?;
    let tag_name = args
        .tag
        .unwrap_or_else(|| format!("weaver-v{version}"))
        .trim()
        .to_string();
    let expected_tag = format!("weaver-v{version}");
    if tag_name != expected_tag {
        bail!("WinGet tag/version mismatch: expected {expected_tag}, got {tag_name}");
    }
    let repository = normalize_github_repository(&args.repository)?;
    let release_date = args
        .release_date
        .unwrap_or_else(|| Utc::now().date_naive().to_string());
    validate_winget_release_date(&release_date)?;

    let artifacts_dir = if args.artifacts_dir.is_absolute() {
        args.artifacts_dir
    } else {
        ctx.repo_root.join(args.artifacts_dir)
    };
    let output_dir = if args.output_dir.is_absolute() {
        args.output_dir
    } else {
        ctx.repo_root.join(args.output_dir)
    };
    let artifacts = collect_winget_artifacts(&repository, &tag_name, &artifacts_dir)?;
    let manifest_dir = write_winget_manifests(&output_dir, &version, &release_date, &artifacts)?;

    ok(format!(
        "Generated WinGet manifests in {}",
        manifest_dir.display()
    ));
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WingetArtifact {
    architecture: &'static str,
    installer_url: String,
    installer_sha256: String,
}

fn normalize_winget_version(raw: &str) -> Result<Version> {
    let trimmed = raw.trim();
    let version = trimmed
        .strip_prefix("weaver-v")
        .or_else(|| trimmed.strip_prefix('v'))
        .unwrap_or(trimmed);
    Version::parse(version).with_context(|| format!("invalid Weaver version: {raw}"))
}

fn normalize_github_repository(raw: &str) -> Result<String> {
    let repository = raw.trim().trim_matches('/').to_string();
    let mut parts = repository.split('/');
    let owner = parts.next().filter(|part| !part.is_empty());
    let name = parts.next().filter(|part| !part.is_empty());
    if owner.is_none() || name.is_none() || parts.next().is_some() || repository.contains("://") {
        bail!("GitHub repository must be owner/name, got {raw}");
    }
    Ok(repository)
}

fn validate_winget_release_date(release_date: &str) -> Result<()> {
    NaiveDate::parse_from_str(release_date, "%Y-%m-%d")
        .with_context(|| format!("release date must be YYYY-MM-DD, got {release_date}"))?;
    Ok(())
}

fn collect_winget_artifacts(
    repository: &str,
    tag_name: &str,
    artifacts_dir: &Path,
) -> Result<Vec<WingetArtifact>> {
    [
        ("x64", WINGET_WINDOWS_X64_ASSET),
        ("arm64", WINGET_WINDOWS_ARM64_ASSET),
    ]
    .into_iter()
    .map(|(architecture, asset_name)| {
        let path = artifacts_dir.join(asset_name);
        validate_winget_portable_zip(&path)?;
        let bytes =
            fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let installer_sha256 = sha256_hex(&bytes).to_ascii_uppercase();
        let installer_url =
            format!("https://github.com/{repository}/releases/download/{tag_name}/{asset_name}");
        Ok(WingetArtifact {
            architecture,
            installer_url,
            installer_sha256,
        })
    })
    .collect()
}

fn validate_winget_portable_zip(path: &Path) -> Result<()> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let entries = zip_central_directory_entries(&bytes)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    if entries.iter().any(|entry| entry == "weaver.exe") {
        Ok(())
    } else {
        bail!(
            "{} must contain weaver.exe at the zip root for WinGet portable install",
            path.display()
        )
    }
}

fn zip_central_directory_entries(bytes: &[u8]) -> Result<Vec<String>> {
    const EOCD_SIGNATURE: &[u8; 4] = b"PK\x05\x06";
    const CENTRAL_DIRECTORY_SIGNATURE: u32 = 0x0201_4b50;

    if bytes.len() < 22 {
        bail!("zip is too short to contain an end-of-central-directory record");
    }

    let search_start = bytes.len().saturating_sub(22 + u16::MAX as usize);
    let eocd_offset = (search_start..=bytes.len() - 22)
        .rev()
        .find(|offset| bytes.get(*offset..*offset + 4) == Some(EOCD_SIGNATURE.as_slice()))
        .context("missing zip end-of-central-directory record")?;
    let central_directory_size = read_le_u32(bytes, eocd_offset + 12)? as usize;
    let central_directory_offset = read_le_u32(bytes, eocd_offset + 16)? as usize;
    let central_directory_end = central_directory_offset
        .checked_add(central_directory_size)
        .context("zip central directory overflows usize")?;
    if central_directory_end > bytes.len() {
        bail!("zip central directory points beyond file length");
    }

    let mut offset = central_directory_offset;
    let mut entries = Vec::new();
    while offset < central_directory_end {
        let signature = read_le_u32(bytes, offset)?;
        if signature != CENTRAL_DIRECTORY_SIGNATURE {
            bail!("invalid zip central directory header at byte {offset}");
        }
        let file_name_len = read_le_u16(bytes, offset + 28)? as usize;
        let extra_len = read_le_u16(bytes, offset + 30)? as usize;
        let comment_len = read_le_u16(bytes, offset + 32)? as usize;
        let name_start = offset + 46;
        let name_end = name_start
            .checked_add(file_name_len)
            .context("zip file name length overflows usize")?;
        let record_end = name_end
            .checked_add(extra_len)
            .and_then(|end| end.checked_add(comment_len))
            .context("zip central directory record length overflows usize")?;
        if record_end > central_directory_end {
            bail!("zip central directory record extends beyond declared directory");
        }
        let name = std::str::from_utf8(&bytes[name_start..name_end])
            .context("zip entry name is not utf-8")?;
        entries.push(name.to_string());
        offset = record_end;
    }
    Ok(entries)
}

fn read_le_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let raw = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| anyhow!("unexpected end of zip while reading u16 at byte {offset}"))?;
    Ok(u16::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_le_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let raw = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| anyhow!("unexpected end of zip while reading u32 at byte {offset}"))?;
    Ok(u32::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn write_winget_manifests(
    output_dir: &Path,
    version: &Version,
    release_date: &str,
    artifacts: &[WingetArtifact],
) -> Result<PathBuf> {
    let manifest_dir = output_dir
        .join(WINGET_PACKAGE_IDENTIFIER)
        .join(version.to_string());
    if manifest_dir.exists() {
        fs::remove_dir_all(&manifest_dir)
            .with_context(|| format!("failed to clear {}", manifest_dir.display()))?;
    }
    fs::create_dir_all(&manifest_dir)
        .with_context(|| format!("failed to create {}", manifest_dir.display()))?;

    write_text_file(
        &manifest_dir.join(format!("{WINGET_PACKAGE_IDENTIFIER}.yaml")),
        &winget_version_manifest(version),
    )?;
    write_text_file(
        &manifest_dir.join(format!("{WINGET_PACKAGE_IDENTIFIER}.locale.en-US.yaml")),
        &winget_locale_manifest(version),
    )?;
    write_text_file(
        &manifest_dir.join(format!("{WINGET_PACKAGE_IDENTIFIER}.installer.yaml")),
        &winget_installer_manifest(version, release_date, artifacts),
    )?;
    Ok(manifest_dir)
}

fn write_text_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))
}

fn winget_version_manifest(version: &Version) -> String {
    format!(
        "# yaml-language-server: $schema=https://aka.ms/winget-manifest.version.{WINGET_MANIFEST_VERSION}.schema.json\n\n\
PackageIdentifier: {WINGET_PACKAGE_IDENTIFIER}\n\
PackageVersion: {version}\n\
DefaultLocale: en-US\n\
ManifestType: version\n\
ManifestVersion: {WINGET_MANIFEST_VERSION}\n"
    )
}

fn winget_locale_manifest(version: &Version) -> String {
    format!(
        "# yaml-language-server: $schema=https://aka.ms/winget-manifest.defaultLocale.{WINGET_MANIFEST_VERSION}.schema.json\n\n\
PackageIdentifier: {WINGET_PACKAGE_IDENTIFIER}\n\
PackageVersion: {version}\n\
PackageLocale: en-US\n\
Publisher: Scryer Media\n\
PublisherUrl: https://www.scryer.media/\n\
PublisherSupportUrl: https://github.com/scryer-media/weaver/issues\n\
Author: Scryer Media\n\
PackageName: {WINGET_PACKAGE_NAME}\n\
PackageUrl: https://github.com/scryer-media/weaver\n\
License: GPL-3.0-or-later with UnRAR restriction\n\
LicenseUrl: https://github.com/scryer-media/weaver/blob/main/LICENSE\n\
Copyright: Copyright (c) Scryer Media\n\
ShortDescription: High-performance Usenet binary downloader.\n\
Description: Weaver is a Usenet binary downloader that handles downloading, decoding, PAR2 verification and repair, and archive extraction.\n\
Moniker: {WINGET_MONIKER}\n\
Tags:\n\
- downloader\n\
- media\n\
- nntp\n\
- par2\n\
- usenet\n\
ReleaseNotesUrl: https://github.com/scryer-media/weaver/releases/tag/weaver-v{version}\n\
ManifestType: defaultLocale\n\
ManifestVersion: {WINGET_MANIFEST_VERSION}\n"
    )
}

fn winget_installer_manifest(
    version: &Version,
    release_date: &str,
    artifacts: &[WingetArtifact],
) -> String {
    let installers = artifacts
        .iter()
        .map(|artifact| {
            format!(
                "- Architecture: {}\n  InstallerUrl: {}\n  InstallerSha256: {}",
                artifact.architecture, artifact.installer_url, artifact.installer_sha256
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "# yaml-language-server: $schema=https://aka.ms/winget-manifest.installer.{WINGET_MANIFEST_VERSION}.schema.json\n\n\
PackageIdentifier: {WINGET_PACKAGE_IDENTIFIER}\n\
PackageVersion: {version}\n\
InstallerType: zip\n\
NestedInstallerType: portable\n\
NestedInstallerFiles:\n\
- RelativeFilePath: weaver.exe\n  PortableCommandAlias: {WINGET_PORTABLE_COMMAND_ALIAS}\n\
InstallModes:\n\
- silent\n\
UpgradeBehavior: install\n\
ReleaseDate: {release_date}\n\
Installers:\n\
{installers}\n\
ManifestType: installer\n\
ManifestVersion: {WINGET_MANIFEST_VERSION}\n"
    )
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
                validated_tree: None,
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
    }

    let tracked_dirty_paths_before_validation = if args.dry_run || !reused_dry_run_cache {
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
        step("Running release prep validation");
        let prep_result = run_weaver_release_prep(ctx, "[prep] ");
        if let Err(error) = &prep_result {
            warn(format!("Release prep validation failed: {error:#}"));
        }

        let validation_result = (|| {
            prep_result?;
            ok("Release prep validation passed");

            let prepared_tree = current_tracked_tree(ctx)?;
            let prep_generated_paths =
                new_tracked_dirty_paths(ctx, &tracked_dirty_paths_before_validation)?;

            if !args.dry_run {
                step("Committing prep-generated changes");
                if commit_release_generated_changes(ctx, &prep_generated_paths, "chore: fmt")? {
                    ok("Committed prep-generated changes");
                } else {
                    ok("No prep-generated changes to commit");
                }
            }

            if !args.dry_run && worktree_clean_at_start && cache_path.is_file() {
                match load_release_dry_run_cache(ctx) {
                    Ok(cache) => {
                        let next_version_text = next_version.to_string();
                        let expected = ReleaseDryRunExpectations {
                            validated_tree: &prepared_tree,
                            release_args: &release_args,
                            latest_tag_seen: latest_tag.as_deref(),
                            next_version: &next_version_text,
                            tag_name: &tag_name,
                        };
                        if let Some(reason) =
                            release_dry_run_cache_rejection_reason(&cache, &expected)
                        {
                            println!("   {YELLOW}Skipping dry-run cache reuse: {reason}{RESET}");
                        } else {
                            ok("Reused dry-run cache; skipping nextest and ci-clippy");
                            reused_dry_run_cache = true;
                        }
                    }
                    Err(error) => {
                        println!("   {YELLOW}Skipping dry-run cache reuse: {error:#}{RESET}");
                    }
                }
            }

            if !reused_dry_run_cache {
                step("Running Rust validation");
                let rust_result = run_weaver_rust_heavy_validation(ctx, "[rust] ");
                if let Err(error) = &rust_result {
                    warn(format!("Rust validation failed: {error:#}"));
                }
                rust_result?;
                ok("Rust validation passed");
            }

            Ok::<(Vec<String>, String), anyhow::Error>((
                vec![
                    "release_prep_validation".to_string(),
                    "rust_validation".to_string(),
                ],
                prepared_tree,
            ))
        })();

        if args.dry_run {
            let restore_result = restore_dry_run_tracked_changes(
                ctx,
                &tracked_dirty_paths_before_validation,
                dirty_snapshot.as_deref(),
            );

            match validation_result {
                Ok((validated_steps, prepared_tree)) => {
                    restore_result?;
                    write_release_dry_run_cache(
                        ctx,
                        &ReleaseDryRunCache {
                            success: true,
                            created_at: Utc::now().to_rfc3339(),
                            git_commit: git_commit.clone(),
                            validated_tree: Some(prepared_tree),
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

    sync_release_workspace_lockfile(ctx)?;

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
    Ok(())
}

fn sync_release_workspace_lockfile(ctx: &TaskContext) -> Result<()> {
    step("Syncing Cargo.lock after version bump");
    let mut update = ctx.command_in("cargo", &ctx.repo_root);
    update.args(["update", "--workspace"]);
    run_checked(&mut update)?;
    ok("Cargo.lock synced for release version bump");
    Ok(())
}

fn run_weaver_release_prep(ctx: &TaskContext, prefix: &'static str) -> Result<()> {
    run_weaver_rust_prep_validation(ctx, prefix)?;
    run_weaver_web_validation(ctx, prefix)?;
    run_weaver_release_hygiene_validation(ctx, prefix)
}

fn run_weaver_release_hygiene_validation(ctx: &TaskContext, prefix: &'static str) -> Result<()> {
    prefixed_step(prefix, "Checking release hygiene");
    let violations = release_hygiene_violations(ctx)?;
    if !violations.is_empty() {
        bail!(
            "release hygiene check failed:\n{}",
            violations
                .into_iter()
                .map(|violation| format!("  - {violation}"))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
    prefixed_ok(prefix, "Release hygiene check passed");
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

fn release_allowed_cargo_audit_ids() -> Result<Vec<&'static str>> {
    let today = Utc::now().date_naive();
    let mut ids = Vec::with_capacity(RELEASE_ALLOWED_CARGO_AUDIT_IDS.len());
    for advisory in RELEASE_ALLOWED_CARGO_AUDIT_IDS {
        let expires_on = NaiveDate::parse_from_str(advisory.expires_on, "%Y-%m-%d")
            .with_context(|| format!("invalid expiry for {}", advisory.id))?;
        if today > expires_on {
            bail!(
                "cargo audit allowlist entry {} expired on {}: {}",
                advisory.id,
                advisory.expires_on,
                advisory.reason
            );
        }
        ids.push(advisory.id);
    }
    Ok(ids)
}

fn run_cargo_audit_validation(ctx: &TaskContext, prefix: &'static str) -> Result<()> {
    prefixed_step(prefix, "Running cargo audit");
    if !command_available("cargo-audit")? {
        warn("cargo-audit not installed — installing");
        let mut install = ctx.command_in("cargo", &ctx.repo_root);
        install.args(["install", "--locked", "cargo-audit"]);
        run_streaming(&mut install, prefix)?;
    }
    let mut audit = ctx.command_in("cargo", &ctx.repo_root);
    audit.arg("audit");
    let allowed_advisories = release_allowed_cargo_audit_ids()?;
    for advisory in allowed_advisories {
        audit.args(["--ignore", advisory]);
    }
    run_streaming(&mut audit, prefix)?;
    prefixed_ok(prefix, "cargo audit passed");
    Ok(())
}

fn run_weaver_rust_prep_validation(ctx: &TaskContext, prefix: &'static str) -> Result<()> {
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

    run_cargo_audit_validation(ctx, prefix)?;

    Ok(())
}

fn run_weaver_rust_heavy_validation(ctx: &TaskContext, prefix: &'static str) -> Result<()> {
    ensure_cargo_nextest(ctx, prefix)?;

    prefixed_step(prefix, "Running cargo nextest and ci-clippy in parallel");
    let nextest_ctx = ctx.clone();
    let clippy_ctx = ctx.clone();
    let nextest = thread::spawn(move || run_weaver_nextest_validation(&nextest_ctx, "[nextest] "));
    let clippy = thread::spawn(move || run_weaver_ci_clippy_validation(&clippy_ctx, "[clippy] "));

    let nextest_result = nextest
        .join()
        .map_err(|_| anyhow!("nextest validation thread panicked"))?;
    let clippy_result = clippy
        .join()
        .map_err(|_| anyhow!("ci-clippy validation thread panicked"))?;

    if let Err(error) = &nextest_result {
        warn(format!("Rust nextest failed: {error:#}"));
    }
    if let Err(error) = &clippy_result {
        warn(format!("Rust ci-clippy failed: {error:#}"));
    }

    match (nextest_result, clippy_result) {
        (Ok(()), Ok(())) => {
            prefixed_ok(prefix, "Parallel nextest + ci-clippy passed");
            Ok(())
        }
        (nextest_result, clippy_result) => {
            let mut failures = Vec::new();
            if let Err(error) = nextest_result {
                failures.push(format!("nextest failed: {error:#}"));
            }
            if let Err(error) = clippy_result {
                failures.push(format!("ci-clippy failed: {error:#}"));
            }
            bail!(failures.join("\n"));
        }
    }
}

fn ensure_cargo_nextest(ctx: &TaskContext, prefix: &'static str) -> Result<()> {
    if !command_available("cargo-nextest")? {
        warn("cargo-nextest not installed — installing");
        let mut install = ctx.command_in("cargo", &ctx.repo_root);
        install.args(["install", "--locked", "cargo-nextest"]);
        run_streaming(&mut install, prefix)?;
    }
    Ok(())
}

fn run_weaver_nextest_validation(ctx: &TaskContext, prefix: &'static str) -> Result<()> {
    prefixed_step(prefix, "Running cargo nextest run --workspace --locked");
    let mut nextest = ctx.command_in("cargo", &ctx.repo_root);
    nextest.args(["nextest", "run", "--workspace", "--locked"]);
    run_streaming(&mut nextest, prefix)?;
    prefixed_ok(prefix, "Rust tests passed");
    Ok(())
}

fn run_weaver_ci_clippy_validation(ctx: &TaskContext, prefix: &'static str) -> Result<()> {
    prefixed_step(prefix, "Running cargo xtask ci clippy --linux-only");
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

#[cfg(unix)]
fn configure_backend_process_group(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
    }
}

#[cfg(not(unix))]
fn configure_backend_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn install_backend_signal_forwarder(process_id: u32) -> Result<SignalForwarder> {
    let process_group = i32::try_from(process_id)
        .map_err(|_| anyhow!("process id overflow while installing signal forwarder"))?;
    SIGNAL_FORWARD_PROCESS_GROUP.store(process_group, Ordering::Relaxed);

    let action = forward_backend_sigaction();
    let mut previous_sigint = unsafe { std::mem::zeroed() };
    let mut previous_sigterm = unsafe { std::mem::zeroed() };

    unsafe {
        if libc::sigaction(libc::SIGINT, &action, &mut previous_sigint) != 0 {
            return Err(io::Error::last_os_error()).context("failed to install SIGINT forwarder");
        }
        if libc::sigaction(libc::SIGTERM, &action, &mut previous_sigterm) != 0 {
            let _ = libc::sigaction(libc::SIGINT, &previous_sigint, std::ptr::null_mut());
            return Err(io::Error::last_os_error()).context("failed to install SIGTERM forwarder");
        }
    }

    Ok(SignalForwarder {
        previous_sigint,
        previous_sigterm,
    })
}

#[cfg(not(unix))]
fn install_backend_signal_forwarder(_process_id: u32) -> Result<SignalForwarder> {
    Ok(SignalForwarder)
}

fn terminate_backend(backend: &mut Child) {
    #[cfg(unix)]
    {
        let process_id = backend.id();
        let _ = signal_process_group(process_id, libc::SIGINT);
        if wait_for_child_exit(backend, BACKEND_SHUTDOWN_GRACE_PERIOD) {
            return;
        }
        let _ = signal_process_group(process_id, libc::SIGKILL);
        let _ = backend.wait();
    }

    #[cfg(not(unix))]
    {
        let _ = backend.kill();
        let _ = backend.wait();
    }
}

#[cfg(unix)]
fn wait_for_child_exit(backend: &mut Child, timeout: StdDuration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        match backend.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(StdDuration::from_millis(100));
            }
            Ok(None) => return false,
            Err(_) => return false,
        }
    }
}

#[cfg(unix)]
fn signal_process_group(process_id: u32, signal: i32) -> io::Result<()> {
    let process_group = i32::try_from(process_id)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "process id overflow"))?;
    let result = unsafe { libc::kill(-process_group, signal) };
    if result == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    if matches!(error.raw_os_error(), Some(libc::ESRCH)) {
        return Ok(());
    }
    Err(error)
}

#[cfg(unix)]
extern "C" fn forward_backend_signal(signal: i32) {
    let process_group = SIGNAL_FORWARD_PROCESS_GROUP.load(Ordering::Relaxed);
    if process_group > 0 {
        unsafe {
            let _ = libc::kill(-process_group, signal);
        }
    }
}

#[cfg(unix)]
fn forward_backend_sigaction() -> libc::sigaction {
    let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
    unsafe {
        libc::sigemptyset(&mut action.sa_mask);
    }
    action.sa_flags = 0;
    action.sa_sigaction = forward_backend_signal as *const () as usize;
    action
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
    let key = fs::read_to_string(path)?
        .replace(['\r', '\n'], "")
        .trim()
        .to_string();
    Ok((!key.is_empty()).then_some(key))
}

fn ensure_state_encryption_key(key_path: &Path, db_path: &Path) -> Result<String> {
    if let Ok(env_key) = std::env::var("WEAVER_ENCRYPTION_KEY") {
        let env_key = env_key.trim().to_string();
        if !env_key.is_empty() {
            return Ok(env_key);
        }
    }

    if let Some(key) = load_state_encryption_key(key_path)? {
        return Ok(key);
    }

    if db_path.exists() {
        bail!(
            "dev state database exists at {} but no encryption key was found at {}; set \
             WEAVER_ENCRYPTION_KEY to the original key or rerun with `cargo xtask serve --clean` \
             to rebuild the dev database",
            db_path.display(),
            key_path.display()
        );
    }

    let mut key_bytes = [0u8; 32];
    getrandom::fill(&mut key_bytes).context("failed to generate dev encryption key")?;
    let key = BASE64_STANDARD.encode(key_bytes);
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(key_path, format!("{key}\n"))
        .with_context(|| format!("failed to write {}", key_path.display()))?;
    Ok(key)
}

fn reset_serve_database(db_path: &Path) -> Result<()> {
    let cleanup_targets = [
        db_path.to_path_buf(),
        PathBuf::from(format!("{}-wal", db_path.display())),
        PathBuf::from(format!("{}-shm", db_path.display())),
    ];

    step(format!(
        "Removing xtask serve database files under {}",
        db_path.display()
    ));
    for path in cleanup_targets {
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
    }
    ok("xtask serve database reset");
    Ok(())
}

fn ensure_frontend_dependencies(ctx: &TaskContext, web_dir: &Path) -> Result<()> {
    step("Syncing frontend dependencies for Vite dev server");
    let mut install = ctx.command_in("npm", web_dir);
    install.args(["install", "--no-fund", "--no-audit"]);
    run_status(&mut install).with_context(|| {
        format!(
            "failed to install frontend dependencies in {}",
            web_dir.display()
        )
    })?;
    ok("Frontend dependencies are up to date");
    Ok(())
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
    backend_binary: &Path,
    state_dir: &Path,
    backend_port: u16,
    backend_log: &Path,
    encryption_key: Option<&str>,
) -> Result<()> {
    if state_dir.join("weaver.db").exists() {
        return Ok(());
    }

    step(format!(
        "Bootstrapping dev state in {}",
        state_dir.display()
    ));
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(backend_log)?;
    let log_err = log.try_clone()?;
    let mut child = ctx.command(backend_binary);
    child
        .env("RUST_LOG", "warn")
        .args([
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

fn run_serve(ctx: &TaskContext, args: ServeArgs) -> Result<()> {
    require_command("npm")?;
    require_command("sqlite3")?;
    let web_dir = ctx.path("apps/weaver-web");
    ensure_frontend_dependencies(ctx, &web_dir)?;
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
    let backend_binary = ctx.path("target/debug/weaver");
    let backend_url = format!("http://127.0.0.1:{backend_port}");
    let frontend_url = format!("http://127.0.0.1:{frontend_port}");
    let vite_use_polling =
        std::env::var("WEAVER_VITE_USE_POLLING").unwrap_or_else(|_| "true".to_string());
    let vite_poll_interval =
        std::env::var("WEAVER_VITE_POLL_INTERVAL_MS").unwrap_or_else(|_| "250".to_string());
    let diagnostics_enabled =
        std::env::var("WEAVER_ENABLE_DIAGNOSTICS").unwrap_or_else(|_| "0".to_string());
    let local_rustflags = host_local_rustflags();

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

    let db_path = state_dir.join("weaver.db");
    if args.clean {
        reset_serve_database(&db_path)?;
    }

    let encryption_key = ensure_state_encryption_key(&state_key_file, &db_path)?;
    step("Building Weaver backend");
    println!("   Rust build: cargo build --locked -p weaver");
    let mut build = ctx.command_in("cargo", &ctx.repo_root);
    build
        .env("WEAVER_ENABLE_DIAGNOSTICS", &diagnostics_enabled)
        .env("RUSTFLAGS", &local_rustflags);
    build.args(["build", "--locked", "-p", "weaver"]);
    run_checked(&mut build)?;

    bootstrap_state_dir(
        ctx,
        &backend_binary,
        &state_dir,
        backend_port,
        &backend_log,
        Some(encryption_key.as_str()),
    )?;
    seed_runtime_dirs(&db_path, &data_dir)?;

    step(format!(
        "Starting Weaver backend from {} on :{backend_port}",
        backend_binary.display()
    ));
    println!("   Vite dev server: {frontend_url}");
    println!("   Vite file watch: polling={vite_use_polling} interval_ms={vite_poll_interval}");
    println!("   State: {}", state_dir.display());
    println!("   Data: {}", data_dir.display());
    println!("   Diagnostics enabled: {diagnostics_enabled}");
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&backend_log)?;
    let log_err = log.try_clone()?;
    let mut backend = ctx.command(&backend_binary);
    configure_backend_process_group(&mut backend);
    backend
        .env("RUST_LOG", &rust_log)
        .args([
            "--config",
            &state_dir.display().to_string(),
            "serve",
            "--port",
            &backend_port.to_string(),
        ])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    backend.env("WEAVER_ENCRYPTION_KEY", &encryption_key);
    let mut backend = backend.spawn()?;
    let backend_pid = backend.id();
    let backend_signal_forwarder = install_backend_signal_forwarder(backend_pid)?;
    if let Err(error) = wait_for_backend(backend_pid, backend_port, &backend_log) {
        drop(backend_signal_forwarder);
        terminate_backend(&mut backend);
        if !keep_data {
            drop(temp_dir);
        }
        return Err(error);
    }

    println!("==> Weaver backend ready");
    println!("    Backend:  {backend_url}");
    println!("    Frontend: {frontend_url}");
    println!("    State:    {}", state_dir.display());
    println!("    Data:     {}", data_dir.display());
    println!("    Log:      tail -f {}", backend_log.display());
    println!();
    println!("==> Starting Vite dev server with live updates...");

    let mut vite = ctx.command_in("npm", &web_dir);
    vite.env("WEAVER_DEV_PROXY_TARGET", &backend_url)
        .env("WEAVER_ENABLE_DIAGNOSTICS", &diagnostics_enabled)
        .env("WEAVER_VITE_USE_POLLING", &vite_use_polling)
        .env("WEAVER_VITE_POLL_INTERVAL_MS", &vite_poll_interval)
        .args([
            "run",
            "dev",
            "--",
            "--host",
            "0.0.0.0",
            "--port",
            &frontend_port.to_string(),
        ]);
    let result = run_status(&mut vite);

    drop(backend_signal_forwarder);
    terminate_backend(&mut backend);
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
            .env("RUSTFLAGS", host_local_rustflags())
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

    fn sample_winget_artifacts() -> Vec<WingetArtifact> {
        vec![
            WingetArtifact {
                architecture: "x64",
                installer_url: format!(
                    "https://github.com/scryer-media/weaver/releases/download/weaver-v0.6.6/{WINGET_WINDOWS_X64_ASSET}"
                ),
                installer_sha256: "A".repeat(64),
            },
            WingetArtifact {
                architecture: "arm64",
                installer_url: format!(
                    "https://github.com/scryer-media/weaver/releases/download/weaver-v0.6.6/{WINGET_WINDOWS_ARM64_ASSET}"
                ),
                installer_sha256: "B".repeat(64),
            },
        ]
    }

    #[test]
    fn winget_installer_manifest_uses_weaver_portable_zip_contract() {
        let version = Version::parse("0.6.6").unwrap();
        let manifest =
            winget_installer_manifest(&version, "2026-06-24", &sample_winget_artifacts());

        assert!(manifest.contains("PackageIdentifier: ScryerMedia.Weaver"));
        assert!(manifest.contains("PackageVersion: 0.6.6"));
        assert!(manifest.contains("InstallerType: zip"));
        assert!(manifest.contains("NestedInstallerType: portable"));
        assert!(manifest.contains("RelativeFilePath: weaver.exe"));
        assert!(manifest.contains("  PortableCommandAlias: weaver"));
        assert!(manifest.contains("Architecture: x64"));
        assert!(manifest.contains("Architecture: arm64"));
        assert!(manifest.contains(WINGET_WINDOWS_X64_ASSET));
        assert!(manifest.contains(WINGET_WINDOWS_ARM64_ASSET));
        assert!(manifest.contains("ReleaseDate: 2026-06-24"));
    }

    #[test]
    fn winget_locale_manifest_matches_weaver_identity() {
        let version = Version::parse("0.6.6").unwrap();
        let manifest = winget_locale_manifest(&version);

        assert!(manifest.contains("PackageIdentifier: ScryerMedia.Weaver"));
        assert!(manifest.contains("Publisher: Scryer Media"));
        assert!(manifest.contains("PackageName: Weaver"));
        assert!(manifest.contains("License: GPL-3.0-or-later with UnRAR restriction"));
        assert!(manifest.contains("Moniker: weaver-usenet"));
        assert!(manifest.contains(
            "ReleaseNotesUrl: https://github.com/scryer-media/weaver/releases/tag/weaver-v0.6.6"
        ));
    }

    #[test]
    fn winget_manifest_writer_uses_package_version_directory() {
        let output_dir = tempfile::tempdir().unwrap();
        let version = Version::parse("0.6.6").unwrap();

        let manifest_dir = write_winget_manifests(
            output_dir.path(),
            &version,
            "2026-06-24",
            &sample_winget_artifacts(),
        )
        .unwrap();

        assert_eq!(
            manifest_dir.strip_prefix(output_dir.path()).unwrap(),
            Path::new("ScryerMedia.Weaver").join("0.6.6")
        );
        assert!(
            manifest_dir
                .join("ScryerMedia.Weaver.installer.yaml")
                .is_file()
        );
        assert!(manifest_dir.join("ScryerMedia.Weaver.yaml").is_file());
        assert!(
            manifest_dir
                .join("ScryerMedia.Weaver.locale.en-US.yaml")
                .is_file()
        );
    }

    #[test]
    fn winget_version_and_repository_validation_are_strict() {
        assert_eq!(
            normalize_winget_version("weaver-v0.6.6").unwrap(),
            Version::parse("0.6.6").unwrap()
        );
        assert_eq!(
            normalize_github_repository("/scryer-media/weaver/").unwrap(),
            "scryer-media/weaver"
        );
        assert!(normalize_github_repository("https://github.com/scryer-media/weaver").is_err());
        assert!(validate_winget_release_date("2026/06/24").is_err());
    }

    fn sample_release_dry_run_cache() -> ReleaseDryRunCache {
        ReleaseDryRunCache {
            success: true,
            created_at: "2026-05-02T00:00:00Z".to_string(),
            git_commit: "abc123".to_string(),
            validated_tree: Some("tree123".to_string()),
            branch: "main".to_string(),
            worktree_clean_at_start: true,
            release_args: "bump:patch".to_string(),
            latest_tag_seen: Some("weaver-v0.2.7".to_string()),
            next_version: "0.2.8".to_string(),
            tag_name: "weaver-v0.2.8".to_string(),
            validated_steps: vec![
                "release_prep_validation".to_string(),
                "rust_validation".to_string(),
            ],
            failure_message: None,
        }
    }

    fn sample_release_dry_run_expectations<'a>() -> ReleaseDryRunExpectations<'a> {
        ReleaseDryRunExpectations {
            validated_tree: "tree123",
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
    fn release_dry_run_cache_rejects_prepared_tree_mismatch() {
        let mut cache = sample_release_dry_run_cache();
        cache.validated_tree = Some("tree456".to_string());
        let reason =
            release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
        assert_eq!(
            reason.as_deref(),
            Some("prepared tracked tree changed since dry run")
        );
    }

    #[test]
    fn release_dry_run_cache_accepts_commit_mismatch_when_tree_matches() {
        let mut cache = sample_release_dry_run_cache();
        cache.git_commit = "def456".to_string();
        let reason =
            release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
        assert!(reason.is_none());
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

    #[test]
    fn linux_clippy_defaults_follow_host_architecture() {
        assert_eq!(
            linux_clippy_defaults("aarch64-apple-darwin"),
            ("aarch64-unknown-linux-musl", "linux/arm64")
        );
        assert_eq!(
            linux_clippy_defaults("aarch64-unknown-linux-gnu"),
            ("aarch64-unknown-linux-musl", "linux/arm64")
        );
        assert_eq!(
            linux_clippy_defaults("x86_64-apple-darwin"),
            ("x86_64-unknown-linux-musl", "linux/amd64")
        );
    }

    #[test]
    fn cargo_target_env_key_matches_cargo_target_config_names() {
        assert_eq!(
            cargo_target_env_key("aarch64-unknown-linux-musl", "RUSTFLAGS"),
            "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS"
        );
        assert_eq!(
            cargo_target_env_key("x86_64-unknown-linux-musl", "LINKER"),
            "CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER"
        );
    }

    #[test]
    fn ci_rustflags_cover_supported_release_lanes() {
        for (target, lane, expected) in [
            (
                "aarch64-apple-darwin",
                ReleaseLane::Portable,
                "--cfg aes_armv8 --cfg chacha20_force_neon",
            ),
            (
                "aarch64-apple-darwin",
                ReleaseLane::AppleM1,
                "-C target-cpu=apple-m1 --cfg aes_armv8 --cfg chacha20_force_neon",
            ),
            (
                "aarch64-unknown-linux-musl",
                ReleaseLane::Portable,
                "--cfg aes_armv8 --cfg chacha20_force_neon",
            ),
            (
                "aarch64-unknown-linux-musl",
                ReleaseLane::CortexA76,
                "-C target-cpu=cortex-a76 --cfg aes_armv8 --cfg chacha20_force_neon",
            ),
            (
                "aarch64-pc-windows-msvc",
                ReleaseLane::Portable,
                "--cfg aes_armv8 -C target-feature=+crt-static -C linker=rust-lld",
            ),
            (
                "x86_64-apple-darwin",
                ReleaseLane::Haswell,
                "-C target-cpu=haswell",
            ),
            ("x86_64-unknown-linux-musl", ReleaseLane::Portable, ""),
            (
                "x86_64-unknown-linux-musl",
                ReleaseLane::Haswell,
                "-C target-cpu=haswell",
            ),
            (
                "x86_64-pc-windows-msvc",
                ReleaseLane::Portable,
                "-C target-feature=+crt-static -C linker=rust-lld",
            ),
        ] {
            assert_eq!(ci_rustflags_for_target(target, lane).unwrap(), expected);
        }
    }

    #[test]
    fn ci_windows_release_flags_are_static_crt_and_portable_cpu() {
        for target in ["x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc"] {
            let flags = ci_rustflags_for_target(target, ReleaseLane::Portable).unwrap();
            assert!(flags.contains("-C target-feature=+crt-static"));
            assert!(!flags.contains("target-cpu="));
        }
    }

    #[test]
    fn ci_rustflags_reject_unsupported_lanes() {
        let error = ci_rustflags_for_target("aarch64-unknown-linux-musl", ReleaseLane::AppleM1)
            .unwrap_err();
        assert!(
            error.to_string().contains(
                "unsupported lane apple-m1 for rustflags target aarch64-unknown-linux-musl"
            )
        );

        let error =
            ci_rustflags_for_target("x86_64-apple-darwin", ReleaseLane::Portable).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported lane portable for rustflags target x86_64-apple-darwin")
        );
    }

    #[test]
    fn local_rustflags_enable_arm_crypto_backends() {
        assert_eq!(
            local_rustflags_for_host("aarch64", "macos"),
            "-C target-cpu=native --cfg aes_armv8 --cfg chacha20_force_neon"
        );
        assert_eq!(
            local_rustflags_for_host("aarch64", "linux"),
            "-C target-cpu=native --cfg aes_armv8 --cfg chacha20_force_neon"
        );
        assert_eq!(
            local_rustflags_for_host("aarch64", "windows"),
            "-C target-cpu=native --cfg aes_armv8"
        );
    }

    #[test]
    fn local_rustflags_leave_x86_native_only() {
        assert_eq!(
            local_rustflags_for_host("x86_64", "linux"),
            "-C target-cpu=native"
        );
    }

    #[test]
    fn verify_crypto_target_accepts_supported_release_targets() {
        for (target, lane) in [
            ("x86_64-apple-darwin", ReleaseLane::Haswell),
            ("aarch64-apple-darwin", ReleaseLane::Portable),
            ("aarch64-apple-darwin", ReleaseLane::AppleM1),
            ("x86_64-unknown-linux-musl", ReleaseLane::Portable),
            ("x86_64-unknown-linux-musl", ReleaseLane::Haswell),
            ("aarch64-unknown-linux-musl", ReleaseLane::Portable),
            ("aarch64-unknown-linux-musl", ReleaseLane::CortexA76),
            ("x86_64-pc-windows-msvc", ReleaseLane::Portable),
            ("aarch64-pc-windows-msvc", ReleaseLane::Portable),
        ] {
            verify_crypto_target(target, lane).unwrap();
        }
    }

    #[test]
    fn verify_crypto_target_rejects_unsupported_targets() {
        let error =
            verify_crypto_target("x86_64-unknown-linux-gnu", ReleaseLane::Portable).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported native crypto target: x86_64-unknown-linux-gnu")
        );
    }

    #[test]
    fn verify_crypto_target_rejects_unsupported_lanes() {
        let error =
            verify_crypto_target("x86_64-pc-windows-msvc", ReleaseLane::Haswell).unwrap_err();
        assert!(
            error.to_string().contains(
                "unsupported lane haswell for native crypto target x86_64-pc-windows-msvc"
            )
        );

        let error = verify_crypto_target("x86_64-apple-darwin", ReleaseLane::Portable).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported lane portable for native crypto target x86_64-apple-darwin")
        );
    }

    #[test]
    fn release_hygiene_flags_local_absolute_paths() {
        let violations = scan_release_hygiene_content(
            Path::new("server/crates/weaver-server-core/src/pipeline/tests/rar_extraction.rs"),
            concat!(
                "const DEFAULT: &str = \"",
                "/",
                "Users/example/dev/supporting-codebases/par2cmdline-turbo/par2",
                "\";"
            ),
        );

        assert_eq!(
            violations,
            vec![
                concat!(
                    "server/crates/weaver-server-core/src/pipeline/tests/rar_extraction.rs:1: local absolute path reference: const DEFAULT: &str = \"",
                    "/",
                    "Users/example/dev/supporting-codebases/par2cmdline-turbo/par2",
                    "\";"
                )
                    .to_string()
            ]
        );
    }

    #[test]
    fn release_hygiene_flags_sibling_e2e_paths() {
        let violations = scan_release_hygiene_content(
            Path::new("server/crates/weaver-server-core/src/pipeline/tests/rar_extraction.rs"),
            concat!(
                "let fixture = manifest_dir.join(\"..",
                "/../..",
                "/e2e/testdata\").join(name);"
            ),
        );

        assert_eq!(
            violations,
            vec![
                concat!(
                    "server/crates/weaver-server-core/src/pipeline/tests/rar_extraction.rs:1: sibling e2e repo reference: let fixture = manifest_dir.join(\"..",
                    "/../..",
                    "/e2e/testdata\").join(name);"
                )
                    .to_string()
            ]
        );
    }

    #[test]
    fn release_hygiene_allows_repo_local_paths() {
        let violations = scan_release_hygiene_content(
            Path::new("server/crates/weaver-server-core/src/pipeline/tests/rar_extraction.rs"),
            "let fixture = manifest_dir.join(\"tests/fixtures\").join(name);",
        );

        assert!(violations.is_empty());
    }
}
