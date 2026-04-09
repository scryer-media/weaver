use crate::TaskContext;
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

pub(crate) fn run(ctx: &TaskContext, args: Vec<String>) -> Result<()> {
    crate::require_command("curl")?;
    crate::require_command("tar")?;
    crate::require_command("unzip")?;

    let options = parse_args(args)?;
    if !options.kit_dir.is_dir() {
        bail!("kit directory not found: {}", options.kit_dir.display());
    }

    let work_dir = TempDir::new()?;
    let weaver_tmp = work_dir.path().join("weaver");
    let turbo_tmp = work_dir.path().join("turbo");
    fs::create_dir_all(&weaver_tmp)?;
    fs::create_dir_all(&turbo_tmp)?;

    let weaver_release = download_release_asset(
        ctx,
        "scryer-media/weaver",
        AssetSelector::Exact("weaver-linux-x86_64.tar.gz"),
        &weaver_tmp.join("weaver-linux-x86_64.tar.gz"),
    )?;
    let turbo_release = download_release_asset(
        ctx,
        "animetosho/par2cmdline-turbo",
        AssetSelector::TurboLinuxAmd64Zip,
        &turbo_tmp.join("par2cmdline-turbo-linux-amd64.zip"),
    )?;

    let weaver_bin_dir = options.kit_dir.join("bin/weaver-linux-x86_64");
    let turbo_bin_dir = options.kit_dir.join("bin/par2cmdline-turbo-linux-amd64");
    fs::create_dir_all(&weaver_bin_dir)?;
    fs::create_dir_all(&turbo_bin_dir)?;
    let weaver_binary = weaver_bin_dir.join("weaver");
    let turbo_binary = turbo_bin_dir.join("par2");
    let _ = fs::remove_file(&weaver_binary);
    let _ = fs::remove_file(&turbo_binary);

    let weaver_archive = weaver_tmp.join("weaver-linux-x86_64.tar.gz");
    let turbo_archive = turbo_tmp.join("par2cmdline-turbo-linux-amd64.zip");

    let mut untar = ctx.command("tar");
    untar
        .args(["-xzf"])
        .arg(&weaver_archive)
        .args(["-C"])
        .arg(&weaver_tmp);
    crate::run_checked(&mut untar)?;
    let unpacked_weaver = weaver_tmp.join("weaver");
    if !unpacked_weaver.is_file() {
        bail!("weaver release asset did not contain a top-level 'weaver' binary");
    }
    fs::copy(&unpacked_weaver, &weaver_binary)?;
    make_executable(&weaver_binary)?;

    let mut unzip = ctx.command("unzip");
    unzip
        .args(["-q", "-o"])
        .arg(&turbo_archive)
        .args(["-d"])
        .arg(turbo_tmp.join("unpacked"));
    crate::run_checked(&mut unzip)?;
    let unpacked_turbo = turbo_tmp.join("unpacked/par2");
    if !unpacked_turbo.is_file() {
        bail!("turbo release asset did not contain a top-level 'par2' binary");
    }
    fs::copy(&unpacked_turbo, &turbo_binary)?;
    make_executable(&turbo_binary)?;

    let meta_dir = options.kit_dir.join("meta");
    fs::create_dir_all(&meta_dir)?;
    let meta_path = meta_dir.join("tool-downloads.env");
    fs::write(
        &meta_path,
        format!(
            "weaver_release_repo=scryer-media/weaver\nweaver_release_tag={}\nweaver_asset_name={}\nweaver_asset_digest={}\nweaver_asset_sha256={}\nweaver_binary_sha256={}\nturbo_release_repo=animetosho/par2cmdline-turbo\nturbo_release_tag={}\nturbo_asset_name={}\nturbo_asset_digest={}\nturbo_asset_sha256={}\nturbo_binary_sha256={}\n",
            weaver_release.tag_name,
            weaver_release.asset_name,
            weaver_release.asset_digest.unwrap_or_default(),
            sha256_file(&weaver_archive)?,
            sha256_file(&weaver_binary)?,
            turbo_release.tag_name,
            turbo_release.asset_name,
            turbo_release.asset_digest.unwrap_or_default(),
            sha256_file(&turbo_archive)?,
            sha256_file(&turbo_binary)?,
        ),
    )?;

    if let Some(output_tar) = &options.output_tar {
        let _ = fs::remove_file(output_tar);
        let parent = options
            .kit_dir
            .parent()
            .with_context(|| format!("missing parent for {}", options.kit_dir.display()))?;
        let basename = options
            .kit_dir
            .file_name()
            .and_then(|name| name.to_str())
            .with_context(|| format!("invalid kit dir name {}", options.kit_dir.display()))?;
        let mut tar = ctx.command("tar");
        tar.current_dir(parent)
            .args(["-czf"])
            .arg(output_tar)
            .arg(basename);
        crate::run_checked(&mut tar)?;
    }

    println!("kit_dir={}", options.kit_dir.display());
    println!("weaver_binary={}", weaver_binary.display());
    println!("turbo_binary={}", turbo_binary.display());
    if let Some(output_tar) = options.output_tar {
        println!("output_tar={}", output_tar.display());
    }
    Ok(())
}

struct Options {
    kit_dir: PathBuf,
    output_tar: Option<PathBuf>,
}

fn parse_args(args: Vec<String>) -> Result<Options> {
    let mut kit_dir = None::<PathBuf>;
    let mut output_tar = None::<PathBuf>;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--kit-dir" => kit_dir = Some(PathBuf::from(require_value(&mut iter, "--kit-dir")?)),
            "--output-tar" => {
                output_tar = Some(PathBuf::from(require_value(&mut iter, "--output-tar")?));
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            _ => bail!("unknown argument: {arg}"),
        }
    }
    Ok(Options {
        kit_dir: kit_dir.context("--kit-dir is required")?,
        output_tar,
    })
}

fn print_help() {
    println!(
        "Populate a PAR2 x86 kit with the latest public x86_64 Linux binaries for Weaver and par2cmdline-turbo.\n\nUsage:\n  cargo xtask par2 finalize-kit --kit-dir /path/to/kit [--output-tar /path/to/archive.tar.gz]"
    );
}

fn require_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .with_context(|| format!("{flag} requires a value"))
}

#[derive(Deserialize)]
struct ReleaseResponse {
    tag_name: String,
    assets: Vec<ReleaseAsset>,
}

#[derive(Clone, Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
    digest: Option<String>,
}

enum AssetSelector<'a> {
    Exact(&'a str),
    TurboLinuxAmd64Zip,
}

struct DownloadedRelease {
    tag_name: String,
    asset_name: String,
    asset_digest: Option<String>,
}

fn download_release_asset(
    ctx: &TaskContext,
    repo: &str,
    selector: AssetSelector<'_>,
    out_file: &Path,
) -> Result<DownloadedRelease> {
    let release_json = download_release_json(ctx, repo)?;
    let asset = select_asset(&release_json.assets, selector)?.clone();

    let mut curl = ctx.command("curl");
    curl.args(["-fL", &asset.browser_download_url, "-o"])
        .arg(out_file);
    crate::run_checked(&mut curl)?;

    Ok(DownloadedRelease {
        tag_name: release_json.tag_name,
        asset_name: asset.name,
        asset_digest: asset.digest,
    })
}

fn download_release_json(ctx: &TaskContext, repo: &str) -> Result<ReleaseResponse> {
    let url = format!("https://api.github.com/repos/{repo}/releases/latest");
    let mut curl = ctx.command("curl");
    curl.args([
        "-fsSL",
        "-H",
        "Accept: application/vnd.github+json",
        "-H",
        "User-Agent: weaver-xtask",
        &url,
    ]);
    let body = crate::run_capture(&mut curl)?;
    serde_json::from_str(&body).with_context(|| format!("failed to parse release JSON for {repo}"))
}

fn select_asset<'a>(
    assets: &'a [ReleaseAsset],
    selector: AssetSelector<'_>,
) -> Result<&'a ReleaseAsset> {
    match selector {
        AssetSelector::Exact(name) => assets
            .iter()
            .find(|asset| asset.name == name)
            .with_context(|| format!("missing asset named {name}")),
        AssetSelector::TurboLinuxAmd64Zip => assets
            .iter()
            .find(|asset| {
                asset.name.starts_with("par2cmdline-turbo-")
                    && asset.name.ends_with("-linux-amd64.zip")
            })
            .context("missing linux amd64 turbo asset"),
    }
}

fn make_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)?;
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path)?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{digest:x}"))
}
