use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;

fn main() {
    for (index, arg) in env::args().enumerate() {
        println!("argv{index}={arg}");
    }
    println!(
        "probe_env={}",
        env::var("WEAVER_LAUNCHER_PROBE_ENV").unwrap_or_else(|_| "<missing>".to_string())
    );
    println!(
        "probe_tz={}",
        env::var("TZ").unwrap_or_else(|_| "<missing>".to_string())
    );

    if let Ok(path) = env::var("WEAVER_LAUNCHER_PROBE_WRITE_PATH") {
        fs::write(&path, b"probe").expect("payload probe should write the requested file");
        let mode = fs::metadata(&path)
            .expect("payload probe should stat the requested file")
            .permissions()
            .mode()
            & 0o777;
        println!("probe_file_mode={mode:o}");
    }
}
