use crate::{ProfileLocalArgs, TaskContext};
use anyhow::{Context, Result, bail};
use chrono::Local;
use std::fs;
use std::process::Stdio;

pub(crate) fn run(ctx: &TaskContext, args: ProfileLocalArgs) -> Result<()> {
    crate::require_command("lsof")?;
    crate::require_command("sample")?;
    crate::require_command("spindump")?;

    let duration = args
        .duration_seconds
        .unwrap_or_else(|| "30".to_string())
        .parse::<u64>()
        .context("duration must be an integer number of seconds")?;
    let interval_ms = args
        .interval_millis
        .unwrap_or_else(|| "5".to_string())
        .parse::<u64>()
        .context("interval must be an integer number of milliseconds")?;

    let pid = listening_pid(ctx, "9090")?;
    let stamp = Local::now().format("%Y%m%d-%H%M%S").to_string();
    let out_dir = ctx.path(&format!("tmp/profiles/{stamp}"));
    fs::create_dir_all(&out_dir)?;

    let sample_out = out_dir.join("weaver.sample.txt");
    let spindump_out = out_dir.join("weaver.spindump.txt");
    let meta_out = out_dir.join("meta.txt");

    let mut ps = ctx.command("ps");
    ps.args(["-p", &pid, "-o", "pid=", "-o", "command="]);
    let ps_line = crate::run_capture(&mut ps)?;
    fs::write(
        &meta_out,
        format!(
            "pid={pid}\nduration_seconds={duration}\nsample_interval_ms={interval_ms}\nstarted_at={}\n{}",
            Local::now().to_rfc3339(),
            ps_line
        ),
    )?;

    println!("Profiling PID {pid} for {duration}s ({interval_ms}ms sample interval)");
    println!("Output dir: {}", out_dir.display());

    let mut sample = ctx.command("sample");
    sample
        .args([
            &pid,
            &duration.to_string(),
            &interval_ms.to_string(),
            "-mayDie",
            "-fullPaths",
            "-file",
        ])
        .arg(&sample_out)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut sample_child = sample.spawn()?;

    let mut spindump = ctx.command("spindump");
    spindump
        .args([&pid, &duration.to_string(), "10", "-o"])
        .arg(&spindump_out)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut spindump_child = spindump.spawn()?;

    let _ = sample_child.wait()?;
    let spindump_status = spindump_child.wait()?;
    let spindump_available = spindump_out.is_file();
    let mut meta_append = fs::read_to_string(&meta_out)?;
    meta_append.push_str(&format!(
        "spindump_status={}\nspindump_available={}\n",
        spindump_status.code().unwrap_or(-1),
        if spindump_available { 1 } else { 0 }
    ));
    fs::write(&meta_out, meta_append)?;

    println!("Done.");
    println!("sample:   {}", sample_out.display());
    if spindump_available {
        println!("spindump: {}", spindump_out.display());
    } else {
        println!("spindump: unavailable");
    }
    println!("meta:     {}", meta_out.display());
    Ok(())
}

fn listening_pid(ctx: &TaskContext, port: &str) -> Result<String> {
    let mut command = ctx.command("lsof");
    command.args(["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-t"]);
    let output = crate::run_capture(&mut command)?;
    let pid = output
        .lines()
        .find(|line| !line.trim().is_empty())
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    if pid.is_empty() {
        bail!("no process found listening on port {port}");
    }
    Ok(pid)
}
