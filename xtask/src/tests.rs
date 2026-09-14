use super::*;

fn parse_serve_args(args: &[&str]) -> ServeArgs {
    let cli =
        Cli::try_parse_from(std::iter::once("cargo xtask").chain(args.iter().copied())).unwrap();
    let Commands::Serve(args) = cli.command else {
        panic!("expected serve command");
    };
    args
}

#[test]
fn serve_build_profiles_are_explicit_and_mutually_exclusive() {
    assert_eq!(parse_serve_args(&["serve"]).cargo_profile(), None);
    assert_eq!(
        parse_serve_args(&["serve", "--release"]).cargo_profile(),
        Some("e2e")
    );
    assert_eq!(
        parse_serve_args(&["serve", "--production-build"]).cargo_profile(),
        Some("release")
    );
    assert!(
        Cli::try_parse_from(["cargo xtask", "serve", "--release", "--production-build"]).is_err()
    );
}

#[test]
fn local_agent_key_strips_only_trailing_line_endings() {
    assert_eq!(
        strip_trailing_line_endings("  key with spaces  \r\n".to_string()),
        "  key with spaces  "
    );
    assert_eq!(strip_trailing_line_endings("\r\n".to_string()), "");
}

#[test]
fn local_agent_key_generation_uses_weaver_key_shape() {
    let key = generate_local_agent_api_key().unwrap();
    assert!(key.starts_with("wvr_"));
    assert_eq!(key.len(), 36);
}

/// Provisioning shells out to the `sqlite3` CLI, which not every host has.
fn sqlite3_cli_available() -> bool {
    Command::new("sqlite3")
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[test]
fn local_agent_key_provisioning_is_admin_and_rotates_the_previous_dev_key() {
    if !sqlite3_cli_available() {
        eprintln!("skipping: the sqlite3 CLI is not installed on this host");
        return;
    }
    let state = tempfile::tempdir().unwrap();
    let db_path = state.path().join("weaver.db");
    let mut schema = Command::new("sqlite3");
    schema.arg(&db_path).arg(
        "CREATE TABLE api_keys (\
            id INTEGER PRIMARY KEY AUTOINCREMENT, \
            name TEXT NOT NULL, \
            key_hash BLOB NOT NULL UNIQUE, \
            scope TEXT NOT NULL, \
            created_at INTEGER NOT NULL, \
            last_used_at INTEGER\
        );",
    );
    run_checked(&mut schema).unwrap();

    provision_local_agent_api_key(&db_path, "wvr_local-agent-one").unwrap();
    provision_local_agent_api_key(&db_path, "wvr_local-agent-two").unwrap();

    let mut query = Command::new("sqlite3");
    query
        .arg("-noheader")
        .arg(&db_path)
        .arg("SELECT name || '|' || scope || '|' || length(key_hash) FROM api_keys;");
    assert_eq!(
        run_capture(&mut query).unwrap().trim(),
        "xtask-local-agent|admin|32"
    );
}

#[test]
fn graphql_baseline_bootstrap_applies_only_before_the_baseline_release() {
    let baseline = graphql_api_baseline_version();

    // No release has shipped api/graphql/schema.graphql yet.
    assert!(allow_missing_previous_graphql_schema(None));
    let before_baseline = Version::new(baseline.major, baseline.minor, baseline.patch - 1);
    assert!(allow_missing_previous_graphql_schema(Some(&format!(
        "weaver-v{before_baseline}"
    ))));

    // The baseline release and everything after it must ship the artifact;
    // a missing one is a regression, not a fresh baseline.
    assert!(!allow_missing_previous_graphql_schema(Some(&format!(
        "weaver-v{baseline}"
    ))));
    let after_baseline = Version::new(baseline.major, baseline.minor + 1, 0);
    assert!(!allow_missing_previous_graphql_schema(Some(&format!(
        "weaver-v{after_baseline}"
    ))));
}

#[test]
fn graphql_schema_breaks_require_a_minor_or_major_bump() {
    let patch = Version::parse("0.7.7").unwrap();
    assert!(!schema_breaks_allowed_for_bump(
        Some("weaver-v0.7.6"),
        &patch
    ));

    let minor = Version::parse("0.8.0").unwrap();
    assert!(schema_breaks_allowed_for_bump(
        Some("weaver-v0.7.6"),
        &minor
    ));

    let major = Version::parse("1.0.0").unwrap();
    assert!(schema_breaks_allowed_for_bump(
        Some("weaver-v0.7.6"),
        &major
    ));

    // Without a previous tag there is nothing to compare, so breaks are
    // never waved through on that path.
    assert!(!schema_breaks_allowed_for_bump(None, &major));
    assert!(!schema_breaks_allowed_for_bump(Some("not-a-tag"), &major));
}

fn sample_winget_artifacts() -> Vec<WingetArtifact> {
    vec![
        WingetArtifact {
            architecture: "x64",
            installer_url: format!(
                "https://github.com/scryer-media/weaver/releases/download/weaver-v0.6.6/{WINGET_WINDOWS_X64_ASSET}"
            ),
            installer_sha256: "A".repeat(64),
            product_code: "{694CA1CE-CB74-486A-BB1A-005D1D2051A2}".to_string(),
        },
        WingetArtifact {
            architecture: "arm64",
            installer_url: format!(
                "https://github.com/scryer-media/weaver/releases/download/weaver-v0.6.6/{WINGET_WINDOWS_ARM64_ASSET}"
            ),
            installer_sha256: "B".repeat(64),
            product_code: "{AD8E9924-5148-4052-9A91-E4B7B47C9CD7}".to_string(),
        },
    ]
}

#[test]
fn winget_installer_manifest_uses_weaver_msi_contract() {
    let version = Version::parse("0.6.6").unwrap();
    let manifest = winget_installer_manifest(&version, "2026-06-24", &sample_winget_artifacts());

    assert!(manifest.contains("PackageIdentifier: ScryerMedia.Weaver"));
    assert!(manifest.contains("PackageVersion: 0.6.6"));
    assert!(manifest.contains("InstallerType: msi"));
    assert!(manifest.contains("UpgradeBehavior: install"));
    assert!(manifest.contains("ProductCode: '{694CA1CE-CB74-486A-BB1A-005D1D2051A2}'"));
    assert!(manifest.contains("ProductCode: '{AD8E9924-5148-4052-9A91-E4B7B47C9CD7}'"));
    assert!(manifest.contains("Architecture: x64"));
    assert!(manifest.contains("Architecture: arm64"));
    assert!(manifest.contains(WINGET_WINDOWS_X64_ASSET));
    assert!(manifest.contains(WINGET_WINDOWS_ARM64_ASSET));
    assert!(manifest.contains("ReleaseDate: 2026-06-24"));
}

#[test]
fn winget_locale_manifest_matches_weaver_identity() {
    let version = Version::parse("0.6.6").unwrap();
    let manifest = winget_locale_manifest(&version);

    assert!(manifest.contains("PackageIdentifier: ScryerMedia.Weaver"));
    assert!(manifest.contains("Publisher: Scryer Media"));
    assert!(manifest.contains("PackageName: Weaver"));
    assert!(manifest.contains("License: GPL-3.0-or-later with UnRAR restriction"));
    assert!(manifest.contains("Moniker: weaver-usenet"));
    assert!(manifest.contains(
        "ReleaseNotesUrl: https://github.com/scryer-media/weaver/releases/tag/weaver-v0.6.6"
    ));
}

#[test]
fn winget_manifest_writer_uses_package_version_directory() {
    let output_dir = tempfile::tempdir().unwrap();
    let version = Version::parse("0.6.6").unwrap();

    let manifest_dir = write_winget_manifests(
        output_dir.path(),
        &version,
        "2026-06-24",
        &sample_winget_artifacts(),
    )
    .unwrap();

    assert_eq!(
        manifest_dir.strip_prefix(output_dir.path()).unwrap(),
        Path::new("ScryerMedia.Weaver").join("0.6.6")
    );
    assert!(
        manifest_dir
            .join("ScryerMedia.Weaver.installer.yaml")
            .is_file()
    );
    assert!(manifest_dir.join("ScryerMedia.Weaver.yaml").is_file());
    assert!(
        manifest_dir
            .join("ScryerMedia.Weaver.locale.en-US.yaml")
            .is_file()
    );
    for manifest in [
        "ScryerMedia.Weaver.yaml",
        "ScryerMedia.Weaver.installer.yaml",
        "ScryerMedia.Weaver.locale.en-US.yaml",
    ] {
        let content = fs::read_to_string(manifest_dir.join(manifest)).unwrap();
        assert!(content.contains("$schema=https://aka.ms/winget-manifest."));
        assert!(content.contains(".1.10.0.schema.json"));
    }
}

#[test]
fn winget_version_and_repository_validation_are_strict() {
    assert_eq!(
        normalize_winget_version("weaver-v0.6.6").unwrap(),
        Version::parse("0.6.6").unwrap()
    );
    assert_eq!(
        normalize_github_repository("/scryer-media/weaver/").unwrap(),
        "scryer-media/weaver"
    );
    assert!(normalize_github_repository("https://github.com/scryer-media/weaver").is_err());
    assert!(validate_winget_release_date("2026/06/24").is_err());
}

fn sample_release_dry_run_cache() -> ReleaseDryRunCache {
    ReleaseDryRunCache {
        success: true,
        created_at: "2026-05-02T00:00:00Z".to_string(),
        git_commit: "abc123".to_string(),
        validated_tree: Some("tree123".to_string()),
        branch: "main".to_string(),
        worktree_clean_at_start: true,
        release_args: "bump:patch".to_string(),
        latest_tag_seen: Some("weaver-v0.2.7".to_string()),
        next_version: "0.2.8".to_string(),
        tag_name: "weaver-v0.2.8".to_string(),
        validated_steps: vec![
            "release_prep_validation".to_string(),
            "rust_validation".to_string(),
        ],
        failure_message: None,
    }
}

fn sample_release_dry_run_expectations<'a>() -> ReleaseDryRunExpectations<'a> {
    ReleaseDryRunExpectations {
        validated_tree: "tree123",
        release_args: "bump:patch",
        latest_tag_seen: Some("weaver-v0.2.7"),
        next_version: "0.2.8",
        tag_name: "weaver-v0.2.8",
    }
}

#[test]
fn release_args_signature_uses_bump_mode_when_version_not_explicit() {
    assert_eq!(
        release_args_signature(None, VersionBump::Minor),
        "bump:minor"
    );
}

#[test]
fn release_args_signature_uses_explicit_version_when_present() {
    let version = Version::parse("1.2.3").unwrap();
    assert_eq!(
        release_args_signature(Some(&version), VersionBump::Patch),
        "version:1.2.3"
    );
}

#[test]
fn release_dry_run_cache_round_trips_through_json() {
    let cache = sample_release_dry_run_cache();
    let json = serde_json::to_string(&cache).unwrap();
    let decoded: ReleaseDryRunCache = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, cache);
}

#[test]
fn release_dry_run_cache_rejects_unsuccessful_prior_run() {
    let mut cache = sample_release_dry_run_cache();
    cache.success = false;
    let reason =
        release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
    assert_eq!(
        reason.as_deref(),
        Some("previous dry run did not complete successfully")
    );
}

#[test]
fn release_dry_run_cache_rejects_dirty_start() {
    let mut cache = sample_release_dry_run_cache();
    cache.worktree_clean_at_start = false;
    let reason =
        release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
    assert_eq!(
        reason.as_deref(),
        Some("dry run started from a dirty worktree")
    );
}

#[test]
fn release_dry_run_cache_rejects_prepared_tree_mismatch() {
    let mut cache = sample_release_dry_run_cache();
    cache.validated_tree = Some("tree456".to_string());
    let reason =
        release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
    assert_eq!(
        reason.as_deref(),
        Some("prepared tracked tree changed since dry run")
    );
}

#[test]
fn release_dry_run_cache_accepts_commit_mismatch_when_tree_matches() {
    let mut cache = sample_release_dry_run_cache();
    cache.git_commit = "def456".to_string();
    let reason =
        release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
    assert!(reason.is_none());
}

#[test]
fn release_dry_run_cache_rejects_args_mismatch() {
    let mut cache = sample_release_dry_run_cache();
    cache.release_args = "bump:minor".to_string();
    let reason =
        release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
    assert_eq!(
        reason.as_deref(),
        Some("release arguments changed since dry run")
    );
}

#[test]
fn release_dry_run_cache_rejects_latest_tag_mismatch() {
    let mut cache = sample_release_dry_run_cache();
    cache.latest_tag_seen = Some("weaver-v0.2.6".to_string());
    let reason =
        release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
    assert_eq!(
        reason.as_deref(),
        Some("latest release tag changed since dry run")
    );
}

#[test]
fn release_dry_run_cache_rejects_next_tag_mismatch() {
    let mut cache = sample_release_dry_run_cache();
    cache.tag_name = "weaver-v0.2.9".to_string();
    let reason =
        release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
    assert_eq!(
        reason.as_deref(),
        Some("computed release tag changed since dry run")
    );
}

#[test]
fn release_dry_run_cache_accepts_matching_inputs() {
    let cache = sample_release_dry_run_cache();
    let reason =
        release_dry_run_cache_rejection_reason(&cache, &sample_release_dry_run_expectations());
    assert!(reason.is_none());
}

#[test]
fn linux_clippy_defaults_follow_host_architecture() {
    assert_eq!(
        linux_clippy_defaults("aarch64-apple-darwin"),
        ("aarch64-unknown-linux-musl", "linux/arm64")
    );
    assert_eq!(
        linux_clippy_defaults("aarch64-unknown-linux-gnu"),
        ("aarch64-unknown-linux-musl", "linux/arm64")
    );
    assert_eq!(
        linux_clippy_defaults("x86_64-apple-darwin"),
        ("x86_64-unknown-linux-musl", "linux/amd64")
    );
}

#[test]
fn cargo_target_env_key_matches_cargo_target_config_names() {
    assert_eq!(
        cargo_target_env_key("aarch64-unknown-linux-musl", "RUSTFLAGS"),
        "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS"
    );
    assert_eq!(
        cargo_target_env_key("x86_64-unknown-linux-musl", "LINKER"),
        "CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER"
    );
}

#[test]
fn ci_rustflags_cover_supported_release_lanes() {
    for (target, lane, expected) in [
        (
            "aarch64-apple-darwin",
            ReleaseLane::Portable,
            "--cfg aes_armv8 --cfg chacha20_force_neon",
        ),
        (
            "aarch64-apple-darwin",
            ReleaseLane::AppleM1,
            "-C target-cpu=apple-m1 --cfg aes_armv8 --cfg chacha20_force_neon",
        ),
        (
            "aarch64-unknown-linux-musl",
            ReleaseLane::Portable,
            "--cfg aes_armv8 --cfg chacha20_force_neon",
        ),
        (
            "aarch64-unknown-linux-musl",
            ReleaseLane::CortexA76,
            "-C target-cpu=cortex-a76 --cfg aes_armv8 --cfg chacha20_force_neon",
        ),
        (
            "aarch64-pc-windows-msvc",
            ReleaseLane::Portable,
            "--cfg aes_armv8 -C target-feature=+crt-static -C linker=rust-lld",
        ),
        (
            "x86_64-apple-darwin",
            ReleaseLane::Haswell,
            "-C target-cpu=haswell",
        ),
        ("x86_64-unknown-linux-musl", ReleaseLane::Portable, ""),
        (
            "x86_64-unknown-linux-musl",
            ReleaseLane::Haswell,
            "-C target-cpu=haswell",
        ),
        (
            "x86_64-pc-windows-msvc",
            ReleaseLane::Portable,
            "-C target-feature=+crt-static -C linker=rust-lld",
        ),
    ] {
        assert_eq!(ci_rustflags_for_target(target, lane).unwrap(), expected);
    }
}

#[test]
fn ci_windows_release_flags_are_static_crt_and_portable_cpu() {
    for target in ["x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc"] {
        let flags = ci_rustflags_for_target(target, ReleaseLane::Portable).unwrap();
        assert!(flags.contains("-C target-feature=+crt-static"));
        assert!(!flags.contains("target-cpu="));
    }
}

#[test]
fn ci_rustflags_reject_unsupported_lanes() {
    let error =
        ci_rustflags_for_target("aarch64-unknown-linux-musl", ReleaseLane::AppleM1).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported lane apple-m1 for rustflags target aarch64-unknown-linux-musl")
    );

    let error = ci_rustflags_for_target("x86_64-apple-darwin", ReleaseLane::Portable).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported lane portable for rustflags target x86_64-apple-darwin")
    );
}

#[test]
fn local_rustflags_enable_arm_crypto_backends() {
    assert_eq!(
        local_rustflags_for_host("aarch64", "macos"),
        "-C target-cpu=native --cfg aes_armv8 --cfg chacha20_force_neon"
    );
    assert_eq!(
        local_rustflags_for_host("aarch64", "linux"),
        "-C target-cpu=native --cfg aes_armv8 --cfg chacha20_force_neon"
    );
    assert_eq!(
        local_rustflags_for_host("aarch64", "windows"),
        "-C target-cpu=native --cfg aes_armv8"
    );
}

#[test]
fn local_rustflags_leave_x86_native_only() {
    assert_eq!(
        local_rustflags_for_host("x86_64", "linux"),
        "-C target-cpu=native"
    );
}

#[test]
fn verify_crypto_target_accepts_supported_release_targets() {
    for (target, lane) in [
        ("x86_64-apple-darwin", ReleaseLane::Haswell),
        ("aarch64-apple-darwin", ReleaseLane::Portable),
        ("aarch64-apple-darwin", ReleaseLane::AppleM1),
        ("x86_64-unknown-linux-musl", ReleaseLane::Portable),
        ("x86_64-unknown-linux-musl", ReleaseLane::Haswell),
        ("aarch64-unknown-linux-musl", ReleaseLane::Portable),
        ("aarch64-unknown-linux-musl", ReleaseLane::CortexA76),
        ("x86_64-pc-windows-msvc", ReleaseLane::Portable),
        ("aarch64-pc-windows-msvc", ReleaseLane::Portable),
    ] {
        verify_crypto_target(target, lane).unwrap();
    }
}

#[test]
fn verify_crypto_target_rejects_unsupported_targets() {
    let error =
        verify_crypto_target("x86_64-unknown-linux-gnu", ReleaseLane::Portable).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported native crypto target: x86_64-unknown-linux-gnu")
    );
}

#[test]
fn verify_crypto_target_rejects_unsupported_lanes() {
    let error = verify_crypto_target("x86_64-pc-windows-msvc", ReleaseLane::Haswell).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported lane haswell for native crypto target x86_64-pc-windows-msvc")
    );

    let error = verify_crypto_target("x86_64-apple-darwin", ReleaseLane::Portable).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported lane portable for native crypto target x86_64-apple-darwin")
    );
}

#[test]
fn release_hygiene_flags_local_absolute_paths() {
    let violations = scan_release_hygiene_content(
        Path::new("server/crates/weaver-server-core/src/pipeline/tests/rar_extraction.rs"),
        concat!(
            "const DEFAULT: &str = \"",
            "/",
            "Users/example/dev/supporting-codebases/par2cmdline-turbo/par2",
            "\";"
        ),
    );

    assert_eq!(
        violations,
        vec![
            concat!(
                "server/crates/weaver-server-core/src/pipeline/tests/rar_extraction.rs:1: local absolute path reference: const DEFAULT: &str = \"",
                "/",
                "Users/example/dev/supporting-codebases/par2cmdline-turbo/par2",
                "\";"
            )
                .to_string()
        ]
    );
}

#[test]
fn release_hygiene_flags_sibling_e2e_paths() {
    let violations = scan_release_hygiene_content(
        Path::new("server/crates/weaver-server-core/src/pipeline/tests/rar_extraction.rs"),
        concat!(
            "let fixture = manifest_dir.join(\"..",
            "/../..",
            "/e2e/testdata\").join(name);"
        ),
    );

    assert_eq!(
        violations,
        vec![
            concat!(
                "server/crates/weaver-server-core/src/pipeline/tests/rar_extraction.rs:1: sibling e2e repo reference: let fixture = manifest_dir.join(\"..",
                "/../..",
                "/e2e/testdata\").join(name);"
            )
                .to_string()
        ]
    );
}

#[test]
fn release_hygiene_allows_repo_local_paths() {
    let violations = scan_release_hygiene_content(
        Path::new("server/crates/weaver-server-core/src/pipeline/tests/rar_extraction.rs"),
        "let fixture = manifest_dir.join(\"tests/fixtures\").join(name);",
    );

    assert!(violations.is_empty());
}
