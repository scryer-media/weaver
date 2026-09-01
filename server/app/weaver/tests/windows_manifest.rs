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

/// The desktop wrapper hosts WebView2, which renders at the monitor's own
/// scale factor. Without this declaration Windows stretches the window's
/// bitmap the moment it moves to a differently scaled display, and the whole
/// UI goes soft.
#[test]
fn windows_manifest_opts_into_per_monitor_dpi_awareness() {
    let manifest = include_str!("../resources/windows/weaver.exe.manifest");

    assert!(
        manifest.contains("<dpiAwareness") && manifest.contains(">PerMonitorV2</dpiAwareness>"),
        "manifest must declare PerMonitorV2 DPI awareness"
    );
}
