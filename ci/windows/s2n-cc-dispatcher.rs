use std::env;
use std::ffi::{OsStr, OsString};
use std::process::{Command, ExitCode};

fn contains_s2n(value: &OsStr) -> bool {
    value.to_string_lossy().contains("s2n-tls-sys")
}

fn is_s2n_invocation(args: &[OsString]) -> bool {
    env::var_os("CARGO_PKG_NAME")
        .as_deref()
        .is_some_and(contains_s2n)
        || env::var_os("CARGO_CRATE_NAME")
            .as_deref()
            .is_some_and(|name| name.to_string_lossy().contains("s2n_tls_sys"))
        || env::var_os("CARGO_MANIFEST_DIR")
            .as_deref()
            .is_some_and(contains_s2n)
        || args.iter().any(|arg| contains_s2n(arg.as_os_str()))
}

fn tool_env(name: &str, fallback: &str) -> OsString {
    env::var_os(name).unwrap_or_else(|| OsString::from(fallback))
}

fn main() -> ExitCode {
    let args: Vec<OsString> = env::args_os().skip(1).collect();
    let use_clang_for_s2n = is_s2n_invocation(&args);
    let tool = if use_clang_for_s2n {
        tool_env("WEAVER_REAL_CLANG", "clang")
    } else {
        tool_env("WEAVER_REAL_CLANG_CL", "clang-cl")
    };

    let mut command = Command::new(tool);

    if use_clang_for_s2n {
        if let Some(include_dir) = env::var_os("WEAVER_S2N_COMPAT_INCLUDE") {
            let mut include_arg = OsString::from("-I");
            include_arg.push(include_dir);
            command.arg(include_arg);
        }
        command.args(args);
    } else {
        command.args(args.into_iter().filter(|arg| arg != "--"));
    }

    match command.status() {
        Ok(status) => ExitCode::from(status.code().unwrap_or(1) as u8),
        Err(error) => {
            eprintln!("failed to run compiler dispatcher target: {error}");
            ExitCode::from(1)
        }
    }
}
