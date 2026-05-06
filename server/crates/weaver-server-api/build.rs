use std::env;

fn env_flag_enabled(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                default
            } else {
                !matches!(
                    normalized.as_str(),
                    "0" | "false" | "off" | "no" | "disabled"
                )
            }
        }
        Err(_) => default,
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=WEAVER_ENABLE_DIAGNOSTICS");
    println!("cargo:rustc-check-cfg=cfg(weaver_diagnostics)");

    if env_flag_enabled("WEAVER_ENABLE_DIAGNOSTICS", true) {
        println!("cargo:rustc-cfg=weaver_diagnostics");
    }
}
