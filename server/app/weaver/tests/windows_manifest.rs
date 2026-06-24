#[test]
fn windows_manifest_opts_into_long_paths() {
    let manifest = include_str!("../resources/windows/weaver.exe.manifest");

    assert!(
        manifest.contains("http://schemas.microsoft.com/SMI/2016/WindowsSettings"),
        "manifest must declare the Windows settings namespace"
    );
    assert!(
        manifest.contains("<ws2:longPathAware") && manifest.contains(">true</ws2:longPathAware>"),
        "manifest must opt weaver.exe into long path awareness"
    );
}
