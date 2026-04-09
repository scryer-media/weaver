use crate::TaskContext;
use anyhow::{Context, Result, bail};
use chrono::Local;
use std::thread;
use std::time::Duration;

pub(crate) fn run(ctx: &TaskContext) -> Result<()> {
    crate::require_command("pgrep")?;
    crate::require_command("ps")?;

    let pid = find_weaver_pid(ctx)?;
    println!("Monitoring weaver PID={pid}");
    println!("---");

    loop {
        let rss_kb = current_rss_kb(ctx, &pid)?;
        let rss_mb = rss_kb / 1024;
        let memory_pressure = system_memory_pressure(ctx)
            .unwrap_or_else(|_| "memory_pressure unavailable".to_string());
        let time = Local::now().format("%H:%M:%S");

        println!("[{time}] RSS={rss_mb}MB | {memory_pressure}");
        if rss_mb > 1024 {
            println!("MEMORY WARNING: {rss_mb}MB");
        }

        thread::sleep(Duration::from_secs(5));
    }
}

fn find_weaver_pid(ctx: &TaskContext) -> Result<String> {
    let mut command = ctx.command("pgrep");
    command.args(["-f", "target/debug/weaver"]);
    let output = crate::run_capture(&mut command)?;
    let pid = output
        .lines()
        .find(|line| !line.trim().is_empty())
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    if pid.is_empty() {
        bail!("weaver not running");
    }
    Ok(pid)
}

fn current_rss_kb(ctx: &TaskContext, pid: &str) -> Result<u64> {
    let mut command = ctx.command("ps");
    command.args(["-o", "rss=", "-p", pid]);
    let output = crate::run_capture(&mut command)?;
    let rss = output.trim();
    if rss.is_empty() {
        bail!("weaver process gone");
    }
    rss.parse::<u64>()
        .with_context(|| format!("failed to parse RSS value '{rss}'"))
}

fn system_memory_pressure(ctx: &TaskContext) -> Result<String> {
    if !crate::command_available("memory_pressure")? {
        bail!("memory_pressure unavailable");
    }
    let mut command = ctx.command("memory_pressure");
    command.arg("-Q");
    let output = crate::run_capture(&mut command)?;
    Ok(output
        .lines()
        .find(|line| line.contains("System-wide"))
        .map(str::trim)
        .unwrap_or("memory_pressure output unavailable")
        .to_string())
}
