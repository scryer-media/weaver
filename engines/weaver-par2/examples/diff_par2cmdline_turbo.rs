#[path = "../tests/support/benchmark_support.rs"]
mod benchmark_support;

use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;

use benchmark_support::{
    DEFAULT_TURBO_BINARY, TurboMode, differential_scenarios, format_shell_command,
    select_scenarios, stage_scenario, turbo_args,
};
use weaver_par2::{Par2RepairOutcome, Par2Repairer, Par2RepairerOptions};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Weaver,
    Turbo,
    Both,
}

impl Engine {
    fn includes_weaver(self) -> bool {
        matches!(self, Self::Weaver | Self::Both)
    }

    fn includes_turbo(self) -> bool {
        matches!(self, Self::Turbo | Self::Both)
    }
}

#[derive(Debug)]
struct Config {
    execute: bool,
    list_only: bool,
    engine: Engine,
    mode: TurboMode,
    scenario_filters: Vec<String>,
    turbo_binary: PathBuf,
    turbo_threads: Option<usize>,
    turbo_memory_mib: Option<usize>,
    scan_skip_data: bool,
    scan_skip_leeway: Option<u64>,
    quiet: bool,
}

fn usage() -> &'static str {
    "\
Usage:
  cargo run -p weaver-par2 --example diff_par2cmdline_turbo -- [options]

Options:
  --list                     List available scenarios and exit
  --execute                  Run the selected engines instead of dry-run planning
  --engine <weaver|turbo|both>
  --mode <verify|repair>
  --scenario <name-fragment> Repeatable scenario filter
  --turbo-binary <path>      Defaults to par2 on PATH
  --turbo-threads <n>        Passed to par2cmdline-turbo as -t<n>
  --turbo-memory-mib <n>     Passed to par2cmdline-turbo as -m<n>
  --scan-skip-data           Use oracle-style block skip scanning (-N in par2cmdline-turbo)
  --scan-skip-leeway <bytes> Use this skip leeway with --scan-skip-data (-S<n>)
  --no-quiet                 Omit -q when invoking par2cmdline-turbo
  --help
"
}

fn main() {
    let config = parse_config();
    let scenarios = select_scenarios(differential_scenarios(), &config.scenario_filters);

    if config.list_only {
        for scenario in &scenarios {
            println!("{}  {}", scenario.name, scenario.fixture_dir.display());
        }
        return;
    }

    println!("weaver-par2 differential scaffold");
    println!("  execute: {}", config.execute);
    println!("  engine:  {:?}", config.engine);
    println!("  mode:    {:?}", config.mode);
    println!("  turbo:   {}", config.turbo_binary.display());
    println!();

    for scenario in scenarios {
        let preview = stage_scenario(&scenario);
        println!("scenario: {}", preview.name);
        println!("  fixture: {}", scenario.fixture_dir.display());
        println!("  payloads: {}", preview.payload_paths.len());
        println!("  slice: {} KiB", preview.slice_size / 1024);

        if config.engine.includes_weaver() {
            if config.execute {
                let staged = stage_scenario(&scenario);
                let started = Instant::now();
                let outcome = run_weaver(
                    &staged,
                    config.mode,
                    config.scan_skip_data,
                    config.scan_skip_leeway,
                )
                .expect("weaver run");
                println!(
                    "  weaver: {:?} in {:.3}s (missing={} available={} reconstructed={} copied={})",
                    outcome.status,
                    started.elapsed().as_secs_f64(),
                    outcome.missing_blocks,
                    outcome.available_blocks,
                    outcome.bytes_reconstructed,
                    outcome.bytes_copied
                );
            } else {
                println!(
                    "  weaver: dry-run via Par2Repairer(repair={})",
                    matches!(config.mode, TurboMode::Repair)
                );
            }
        }

        if config.engine.includes_turbo() {
            if config.execute {
                let staged = stage_scenario(&scenario);
                let args = turbo_args(
                    &staged,
                    config.mode,
                    config.turbo_memory_mib,
                    config.turbo_threads,
                    config.quiet,
                    config.scan_skip_data,
                    config.scan_skip_leeway,
                );
                let started = Instant::now();
                let output = Command::new(&config.turbo_binary)
                    .args(&args)
                    .current_dir(staged.temp.path())
                    .output()
                    .expect("run par2cmdline-turbo");
                println!(
                    "  turbo: exit={} in {:.3}s",
                    output.status,
                    started.elapsed().as_secs_f64()
                );
                let stdout = String::from_utf8_lossy(&output.stdout);
                if !stdout.trim().is_empty() {
                    println!("  turbo stdout:\n{}", indent(stdout.trim_end()));
                }
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !stderr.trim().is_empty() {
                    println!("  turbo stderr:\n{}", indent(stderr.trim_end()));
                }
            } else {
                let args = turbo_args(
                    &preview,
                    config.mode,
                    config.turbo_memory_mib,
                    config.turbo_threads,
                    config.quiet,
                    config.scan_skip_data,
                    config.scan_skip_leeway,
                );
                let preview =
                    format_shell_command(&config.turbo_binary, &args, preview.temp.path());
                println!("  turbo: {preview}");
            }
        }

        println!();
    }
}

fn parse_config() -> Config {
    let mut config = Config {
        execute: false,
        list_only: false,
        engine: Engine::Both,
        mode: TurboMode::Repair,
        scenario_filters: Vec::new(),
        turbo_binary: std::env::var_os("PAR2_TURBO_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_TURBO_BINARY)),
        turbo_threads: std::env::var("PAR2_TURBO_THREADS")
            .ok()
            .and_then(|value| value.parse().ok()),
        turbo_memory_mib: std::env::var("PAR2_TURBO_MEMORY_MIB")
            .ok()
            .and_then(|value| value.parse().ok()),
        scan_skip_data: false,
        scan_skip_leeway: None,
        quiet: true,
    };

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--list" => config.list_only = true,
            "--execute" => config.execute = true,
            "--engine" => {
                let value = args.next().unwrap_or_else(|| {
                    eprintln!("missing value for --engine");
                    eprintln!("{}", usage());
                    std::process::exit(2);
                });
                config.engine = parse_engine(&value);
            }
            "--mode" => {
                let value = args.next().unwrap_or_else(|| {
                    eprintln!("missing value for --mode");
                    eprintln!("{}", usage());
                    std::process::exit(2);
                });
                config.mode = parse_mode(&value);
            }
            "--scenario" => {
                let value = args.next().unwrap_or_else(|| {
                    eprintln!("missing value for --scenario");
                    eprintln!("{}", usage());
                    std::process::exit(2);
                });
                config.scenario_filters.push(value);
            }
            "--turbo-binary" => {
                let value = args.next().unwrap_or_else(|| {
                    eprintln!("missing value for --turbo-binary");
                    eprintln!("{}", usage());
                    std::process::exit(2);
                });
                config.turbo_binary = PathBuf::from(value);
            }
            "--turbo-threads" => {
                let value = args.next().unwrap_or_else(|| {
                    eprintln!("missing value for --turbo-threads");
                    eprintln!("{}", usage());
                    std::process::exit(2);
                });
                config.turbo_threads = Some(value.parse().expect("valid thread count"));
            }
            "--turbo-memory-mib" => {
                let value = args.next().unwrap_or_else(|| {
                    eprintln!("missing value for --turbo-memory-mib");
                    eprintln!("{}", usage());
                    std::process::exit(2);
                });
                config.turbo_memory_mib = Some(value.parse().expect("valid memory size"));
            }
            "--scan-skip-data" => config.scan_skip_data = true,
            "--scan-skip-leeway" => {
                let value = args.next().unwrap_or_else(|| {
                    eprintln!("missing value for --scan-skip-leeway");
                    eprintln!("{}", usage());
                    std::process::exit(2);
                });
                config.scan_skip_data = true;
                config.scan_skip_leeway = Some(value.parse().expect("valid skip leeway"));
            }
            "--no-quiet" => config.quiet = false,
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown option: {other}");
                eprintln!("{}", usage());
                std::process::exit(2);
            }
        }
    }

    config
}

fn parse_engine(value: &str) -> Engine {
    match value {
        "weaver" => Engine::Weaver,
        "turbo" => Engine::Turbo,
        "both" => Engine::Both,
        _ => {
            eprintln!("invalid --engine value: {value}");
            eprintln!("{}", usage());
            std::process::exit(2);
        }
    }
}

fn parse_mode(value: &str) -> TurboMode {
    match value {
        "verify" => TurboMode::Verify,
        "repair" => TurboMode::Repair,
        _ => {
            eprintln!("invalid --mode value: {value}");
            eprintln!("{}", usage());
            std::process::exit(2);
        }
    }
}

fn run_weaver(
    staged: &benchmark_support::StagedScenario,
    mode: TurboMode,
    scan_skip_data: bool,
    scan_skip_leeway: Option<u64>,
) -> weaver_par2::Result<Par2RepairOutcome> {
    let mut options = Par2RepairerOptions::new(
        staged.temp.path().to_path_buf(),
        vec![staged.main_par2.clone()],
    );
    options.recovery_paths = staged.recovery_par2.clone();
    options.repair = matches!(mode, TurboMode::Repair);
    options.scan_skip_data = scan_skip_data;
    if let Some(scan_skip_leeway) = scan_skip_leeway {
        options.scan_skip_leeway = scan_skip_leeway;
    }
    Par2Repairer::new(options).verify_or_repair()
}

fn indent(text: &str) -> String {
    text.lines()
        .map(|line| format!("    {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}
