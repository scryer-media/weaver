fn main() {
    let sdl = weaver_server_api::export_schema_sdl();
    print!("{sdl}");
    if !sdl.ends_with('\n') {
        println!();
    }
}
