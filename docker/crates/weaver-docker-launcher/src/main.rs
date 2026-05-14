fn main() {
    if let Err(error) = weaver_docker_launcher::run_from_env() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
