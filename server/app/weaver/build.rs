use std::env;

fn main() {
    compile_windows_resources();
}

fn compile_windows_resources() {
    println!("cargo:rerun-if-changed=resources/windows/weaver.rc");
    println!("cargo:rerun-if-changed=resources/windows/weaver.exe.manifest");

    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }

    embed_resource::compile("resources/windows/weaver.rc", embed_resource::NONE)
        .manifest_required()
        .unwrap_or_else(|error| panic!("failed to embed Windows application manifest: {error}"));
}
