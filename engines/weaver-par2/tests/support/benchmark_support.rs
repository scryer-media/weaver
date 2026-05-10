use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use weaver_par2::Par2FileSet;

pub const DEFAULT_TURBO_BINARY: &str =
    "par2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurboMode {
    Verify,
    Repair,
}

#[derive(Debug, Clone)]
pub enum Mutation {
    HeavyDamage {
        damage_count: usize,
        bytes_per_site: usize,
    },
    RemoveMiddlePayload,
    CorruptMiddlePayload {
        corrupt_len: usize,
        min_offset: u64,
    },
}

#[derive(Debug, Clone)]
pub struct Scenario {
    pub name: &'static str,
    pub fixture_dir: PathBuf,
    pub payload_prefix: &'static str,
    pub payload_extension: &'static str,
    pub par2_prefix: &'static str,
    pub mutation: Mutation,
}

#[derive(Debug)]
pub struct StagedScenario {
    pub name: String,
    pub temp: TempDir,
    pub main_par2: PathBuf,
    pub recovery_par2: Vec<PathBuf>,
    pub payload_paths: Vec<PathBuf>,
    pub slice_size: u64,
}

pub fn crate_bench_scenarios() -> Vec<Scenario> {
    let fixtures = manifest_dir().join("tests/fixtures");
    vec![
        Scenario {
            name: "rar5_heavy_damage_28",
            fixture_dir: fixtures.join("rar5_heavy_damage"),
            payload_prefix: "fixture_rar5_heavy_damage",
            payload_extension: "rar",
            par2_prefix: "fixture_rar5_heavy_damage_repair",
            mutation: Mutation::HeavyDamage {
                damage_count: 28,
                bytes_per_site: 1024,
            },
        },
        Scenario {
            name: "rar5_heavy_damage_250",
            fixture_dir: fixtures.join("rar5_heavy_damage"),
            payload_prefix: "fixture_rar5_heavy_damage",
            payload_extension: "rar",
            par2_prefix: "fixture_rar5_heavy_damage_repair",
            mutation: Mutation::HeavyDamage {
                damage_count: 250,
                bytes_per_site: 1024,
            },
        },
        Scenario {
            name: "rar5_lz_plain_missing_middle",
            fixture_dir: fixtures.join("rar5_lz_plain"),
            payload_prefix: "fixture_rar5_lz_plain",
            payload_extension: "rar",
            par2_prefix: "fixture_rar5_lz_plain_repair",
            mutation: Mutation::RemoveMiddlePayload,
        },
        Scenario {
            name: "rar4_store_enc_corrupt_middle",
            fixture_dir: fixtures.join("rar4_store_enc"),
            payload_prefix: "fixture_rar4_store_enc",
            payload_extension: "rar",
            par2_prefix: "fixture_rar4_store_enc_repair",
            mutation: Mutation::CorruptMiddlePayload {
                corrupt_len: 32 * 1024,
                min_offset: 8192,
            },
        },
    ]
}

pub fn differential_scenarios() -> Vec<Scenario> {
    crate_bench_scenarios()
}

pub fn select_scenarios(scenarios: Vec<Scenario>, filters: &[String]) -> Vec<Scenario> {
    if filters.is_empty() {
        return scenarios;
    }

    let lowered: Vec<String> = filters
        .iter()
        .map(|filter| filter.to_ascii_lowercase())
        .collect();
    let selected: Vec<Scenario> = scenarios
        .into_iter()
        .filter(|scenario| {
            let name = scenario.name.to_ascii_lowercase();
            lowered.iter().any(|filter| name.contains(filter))
        })
        .collect();

    assert!(
        !selected.is_empty(),
        "no benchmark scenarios matched filters: {filters:?}"
    );
    selected
}

pub fn stage_scenario(scenario: &Scenario) -> StagedScenario {
    let temp = TempDir::new().expect("tempdir");
    copy_dir_contents(&scenario.fixture_dir, temp.path());

    let mut par2_paths = collect_paths(temp.path(), scenario.par2_prefix, "par2");
    assert!(
        !par2_paths.is_empty(),
        "fixture {:?} did not contain par2 files for prefix {}",
        scenario.fixture_dir,
        scenario.par2_prefix
    );
    par2_paths.sort();
    let slice_size = Par2FileSet::from_paths(&par2_paths)
        .expect("fixture par2 load")
        .slice_size;

    let payload_paths = collect_paths(
        temp.path(),
        scenario.payload_prefix,
        scenario.payload_extension,
    );
    apply_mutation(&scenario.mutation, &payload_paths, slice_size);

    let payload_paths = collect_paths(
        temp.path(),
        scenario.payload_prefix,
        scenario.payload_extension,
    );
    let (main_par2, recovery_par2) = split_par2_paths(par2_paths);

    StagedScenario {
        name: scenario.name.to_owned(),
        temp,
        main_par2,
        recovery_par2,
        payload_paths,
        slice_size,
    }
}

pub fn turbo_args(
    staged: &StagedScenario,
    mode: TurboMode,
    memory_mib: Option<usize>,
    threads: Option<usize>,
    quiet: bool,
) -> Vec<String> {
    let mut args = Vec::new();
    args.push(match mode {
        TurboMode::Verify => "verify".to_owned(),
        TurboMode::Repair => "repair".to_owned(),
    });
    if quiet {
        args.push("-q".to_owned());
    }
    if let Some(memory_mib) = memory_mib {
        args.push(format!("-m{memory_mib}"));
    }
    if let Some(threads) = threads {
        args.push(format!("-t{threads}"));
    }
    args.push(
        staged
            .main_par2
            .file_name()
            .and_then(OsStr::to_str)
            .expect("main par2 filename")
            .to_owned(),
    );
    args
}

pub fn format_shell_command(binary: &Path, args: &[String], cwd: &Path) -> String {
    let binary = shell_quote(binary.as_os_str().to_string_lossy().as_ref());
    let cwd = shell_quote(cwd.display().to_string().as_str());
    let args = args
        .iter()
        .map(|arg| shell_quote(arg))
        .collect::<Vec<_>>()
        .join(" ");
    format!("(cd {cwd} && {binary} {args})")
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn copy_dir_contents(src: &Path, dst: &Path) {
    for entry in fs::read_dir(src).expect("read_dir") {
        let entry = entry.expect("dir entry");
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            fs::create_dir_all(&dst_path).expect("create_dir_all");
            copy_dir_contents(&src_path, &dst_path);
        } else {
            fs::copy(&src_path, &dst_path).expect("copy fixture file");
        }
    }
}

fn collect_paths(dir: &Path, prefix: &str, extension: &str) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .expect("read_dir")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension() == Some(OsStr::new(extension))
                && path
                    .file_name()
                    .and_then(OsStr::to_str)
                    .is_some_and(|name| name.starts_with(prefix))
        })
        .collect();
    paths.sort();
    paths
}

fn apply_mutation(mutation: &Mutation, payload_paths: &[PathBuf], slice_size: u64) {
    match mutation {
        Mutation::HeavyDamage {
            damage_count,
            bytes_per_site,
        } => {
            let payload = payload_paths
                .first()
                .expect("heavy damage fixture must contain one payload");
            let payload_size = fs::metadata(payload).expect("payload metadata").len();
            let total_slices = payload_size.div_ceil(slice_size) as usize;
            let stride = total_slices / (damage_count + 1);
            let mut file = OpenOptions::new()
                .write(true)
                .open(payload)
                .expect("open payload for heavy damage");
            let damage_bytes = vec![0xA5; *bytes_per_site];
            for index in 0..*damage_count {
                let offset = (stride * (index + 1)) as u64 * slice_size + 100;
                file.seek(SeekFrom::Start(offset)).expect("seek payload");
                file.write_all(&damage_bytes).expect("write damage bytes");
            }
        }
        Mutation::RemoveMiddlePayload => {
            let victim = payload_paths
                .get(payload_paths.len() / 2)
                .expect("missing middle payload victim");
            fs::remove_file(victim).expect("remove middle payload");
        }
        Mutation::CorruptMiddlePayload {
            corrupt_len,
            min_offset,
        } => {
            let victim = payload_paths
                .get(payload_paths.len() / 2)
                .expect("missing middle payload victim");
            let file_len = fs::metadata(victim).expect("payload metadata").len();
            let offset = (file_len / 3).max(*min_offset);
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(victim)
                .expect("open payload for corruption");
            file.seek(SeekFrom::Start(offset)).expect("seek payload");
            file.write_all(&vec![0xA5; *corrupt_len])
                .expect("write corruption bytes");
            file.sync_data().expect("sync corruption bytes");
        }
    }
}

fn split_par2_paths(par2_paths: Vec<PathBuf>) -> (PathBuf, Vec<PathBuf>) {
    let mut main_par2 = None;
    let mut recovery_par2 = Vec::new();

    for path in par2_paths {
        let name = path
            .file_name()
            .and_then(OsStr::to_str)
            .expect("par2 filename");
        if name.contains(".vol") {
            recovery_par2.push(path);
        } else if main_par2.replace(path).is_some() {
            panic!("fixture contained multiple main par2 files");
        }
    }

    (
        main_par2.expect("fixture must contain a main par2 file"),
        recovery_par2,
    )
}

fn shell_quote(raw: &str) -> String {
    if raw
        .bytes()
        .all(|byte| matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'/' | b'.' | b'_' | b'-' | b':' | b'+'))
    {
        raw.to_owned()
    } else {
        format!("'{}'", raw.replace('\'', "'\"'\"'"))
    }
}
