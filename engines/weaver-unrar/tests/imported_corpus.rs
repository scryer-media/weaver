use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::process::Command;

use crc32fast::hash as crc32;
use tempfile::TempDir;
use weaver_unrar::{ArchiveFormat, ExtractOptions, FileHash, RarArchive, ReadSeek, UnixOwnerInfo};

const LIBARCHIVE_PASSWORD: &str = "password";
const GENERATED_FIXTURE_PASSWORD: &str = "testpass123";

fn fixture(dir: &str, name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(dir)
        .join(name)
}

fn open_single(dir: &str, filename: &str) -> RarArchive {
    let data = std::fs::read(fixture(dir, filename)).unwrap();
    RarArchive::open(Cursor::new(data)).unwrap()
}

fn open_single_with_password(dir: &str, filename: &str, password: &str) -> RarArchive {
    let data = std::fs::read(fixture(dir, filename)).unwrap();
    RarArchive::open_with_password(Cursor::new(data), password).unwrap()
}

fn open_multi(dir: &str, filenames: &[&str]) -> RarArchive {
    let readers: Vec<Box<dyn ReadSeek>> = filenames
        .iter()
        .map(|name| {
            let data = std::fs::read(fixture(dir, name)).unwrap();
            Box::new(Cursor::new(data)) as Box<dyn ReadSeek>
        })
        .collect();
    RarArchive::open_volumes(readers).unwrap()
}

fn extract_bytes(archive: &mut RarArchive, index: usize) -> Vec<u8> {
    archive
        .extract_member(index, &ExtractOptions::default(), None)
        .unwrap()
        .into_bytes()
        .unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum FsSnapshotEntry {
    Directory {
        name: String,
        mode: Option<u32>,
        mtime_ns: Option<i128>,
    },
    File {
        name: String,
        len: u64,
        crc32: u32,
        mode: Option<u32>,
        mtime_ns: Option<i128>,
    },
    HardlinkGroup {
        names: Vec<String>,
    },
    Symlink {
        name: String,
        target: String,
        mtime_ns: Option<i128>,
    },
}

fn oracle_required() -> bool {
    std::env::var("WEAVER_REQUIRE_UNRAR_ORACLE").is_ok_and(|value| value == "1" || value == "true")
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn part_prefix_and_number(path: &Path) -> Option<(String, u32)> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_suffix(".rar")?;
    let (prefix, digits) = stem.rsplit_once(".part")?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((prefix.to_string(), digits.parse().ok()?))
}

fn collect_oracle_fixture_groups() -> Vec<(String, Vec<PathBuf>, Option<&'static str>)> {
    let root = fixture_root();
    let mut singles = Vec::new();
    let mut parts = BTreeMap::<(PathBuf, String), Vec<(u32, PathBuf)>>::new();

    for dir in ["rar4", "rar5"] {
        let dir_path = root.join(dir);
        let Ok(entries) = std::fs::read_dir(&dir_path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("rar") {
                continue;
            }
            if let Some((prefix, part)) = part_prefix_and_number(&path) {
                parts
                    .entry((dir_path.clone(), prefix))
                    .or_default()
                    .push((part, path));
            } else {
                singles.push(path);
            }
        }
    }

    let mut groups = Vec::new();
    for path in singles {
        let label = path.strip_prefix(&root).unwrap().display().to_string();
        groups.push((label, vec![path], None));
    }
    for ((dir, prefix), mut paths) in parts {
        paths.sort_by_key(|(part, _)| *part);
        let password = prefix
            .contains("_enc")
            .then_some(GENERATED_FIXTURE_PASSWORD);
        let label = format!("{}/{}", dir.strip_prefix(&root).unwrap().display(), prefix);
        groups.push((
            label,
            paths.into_iter().map(|(_, path)| path).collect(),
            password,
        ));
    }
    groups.sort_by(|a, b| a.0.cmp(&b.0));
    groups
}

fn normalized_output_path(name: &str) -> PathBuf {
    name.replace('\\', "/")
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .fold(PathBuf::new(), |mut path, part| {
            path.push(if part == ".." { "_" } else { part });
            path
        })
}

fn unlock_file_for_read(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            let mut permissions = metadata.permissions();
            permissions.set_mode(permissions.mode() | 0o600);
            let _ = std::fs::set_permissions(path, permissions);
        }
    }

    #[cfg(not(unix))]
    {
        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            let mut permissions = metadata.permissions();
            permissions.set_readonly(false);
            let _ = std::fs::set_permissions(path, permissions);
        }
    }
}

fn system_time_ns(time: std::time::SystemTime) -> Option<i128> {
    match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => Some(
            i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos()),
        ),
        Err(err) => {
            let duration = err.duration();
            Some(
                -(i128::from(duration.as_secs()) * 1_000_000_000
                    + i128::from(duration.subsec_nanos())),
            )
        }
    }
}

fn snapshot_filesystem(
    root: &Path,
    compare_mtime_paths: &BTreeSet<String>,
) -> Vec<FsSnapshotEntry> {
    #[cfg(unix)]
    fn hardlink_key(metadata: &std::fs::Metadata) -> Option<(u64, u64)> {
        use std::os::unix::fs::MetadataExt;

        (metadata.nlink() > 1).then_some((metadata.dev(), metadata.ino()))
    }

    #[cfg(not(unix))]
    fn hardlink_key(_metadata: &std::fs::Metadata) -> Option<(u64, u64)> {
        None
    }

    #[cfg(unix)]
    fn mode_bits(metadata: &std::fs::Metadata) -> Option<u32> {
        use std::os::unix::fs::MetadataExt;

        Some(metadata.mode() & 0o7777)
    }

    #[cfg(not(unix))]
    fn mode_bits(_metadata: &std::fs::Metadata) -> Option<u32> {
        None
    }

    fn mtime_ns(
        name: &str,
        metadata: &std::fs::Metadata,
        compare_paths: &BTreeSet<String>,
    ) -> Option<i128> {
        compare_paths
            .contains(name)
            .then(|| metadata.modified().ok().and_then(system_time_ns))
            .flatten()
    }

    fn walk(
        root: &Path,
        dir: &Path,
        out: &mut Vec<FsSnapshotEntry>,
        hardlinks: &mut BTreeMap<(u64, u64), Vec<String>>,
        compare_mtime_paths: &BTreeSet<String>,
    ) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(metadata) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            let Ok(rel) = path.strip_prefix(root) else {
                continue;
            };
            let name = rel.to_string_lossy().replace('\\', "/");
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                let target = std::fs::read_link(&path)
                    .map(|target| target.to_string_lossy().replace('\\', "/"))
                    .unwrap_or_default();
                out.push(FsSnapshotEntry::Symlink {
                    mtime_ns: mtime_ns(&name, &metadata, compare_mtime_paths),
                    name,
                    target,
                });
            } else if file_type.is_dir() {
                out.push(FsSnapshotEntry::Directory {
                    mtime_ns: mtime_ns(&name, &metadata, compare_mtime_paths),
                    name,
                    mode: mode_bits(&metadata),
                });
                walk(root, &path, out, hardlinks, compare_mtime_paths);
            } else if file_type.is_file() {
                unlock_file_for_read(&path);
                let bytes = std::fs::read(&path)
                    .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
                let mtime_ns = mtime_ns(&name, &metadata, compare_mtime_paths);
                if let Some(key) = hardlink_key(&metadata) {
                    hardlinks.entry(key).or_default().push(name.clone());
                }
                out.push(FsSnapshotEntry::File {
                    name,
                    len: bytes.len() as u64,
                    crc32: crc32(&bytes),
                    mode: mode_bits(&metadata),
                    mtime_ns,
                });
            }
        }
    }

    let mut entries = Vec::new();
    let mut hardlinks = BTreeMap::new();
    walk(
        root,
        root,
        &mut entries,
        &mut hardlinks,
        compare_mtime_paths,
    );
    for mut names in hardlinks.into_values() {
        if names.len() > 1 {
            names.sort();
            entries.push(FsSnapshotEntry::HardlinkGroup { names });
        }
    }
    entries.sort();
    entries
}

fn run_unrar_extract(
    unrar_bin: &str,
    paths: &[PathBuf],
    password: Option<&str>,
    out_dir: &Path,
) -> std::process::ExitStatus {
    let destination = format!("{}{}", out_dir.display(), std::path::MAIN_SEPARATOR);
    let mut command = Command::new(unrar_bin);
    command
        .arg("x")
        .arg("-idq")
        .arg("-inul")
        .arg("-o+")
        .arg(
            password
                .map(|pwd| format!("-p{pwd}"))
                .unwrap_or_else(|| "-p-".to_string()),
        )
        .arg(&paths[0])
        .arg(destination);
    command
        .status()
        .unwrap_or_else(|err| panic!("failed to run {unrar_bin}: {err}"))
}

fn run_unrar_bare_list(
    unrar_bin: &str,
    paths: &[PathBuf],
    password: Option<&str>,
) -> std::process::Output {
    let mut command = Command::new(unrar_bin);
    command
        .arg("lb")
        .arg(
            password
                .map(|pwd| format!("-p{pwd}"))
                .unwrap_or_else(|| "-p-".to_string()),
        )
        .arg(&paths[0]);
    command
        .output()
        .unwrap_or_else(|err| panic!("failed to run {unrar_bin}: {err}"))
}

fn run_unrar_technical_list(
    unrar_bin: &str,
    paths: &[PathBuf],
    password: Option<&str>,
) -> std::process::Output {
    let mut command = Command::new(unrar_bin);
    command
        .arg("lt")
        .arg(
            password
                .map(|pwd| format!("-p{pwd}"))
                .unwrap_or_else(|| "-p-".to_string()),
        )
        .arg(&paths[0]);
    command
        .output()
        .unwrap_or_else(|err| panic!("failed to run {unrar_bin}: {err}"))
}

fn decode_unrar_bare_list_line(line: &str) -> String {
    let Some(marker_pos) = line.find('\u{fffe}') else {
        return line.to_string();
    };

    let mut decoded = String::with_capacity(line.len());
    decoded.push_str(&line[..marker_pos]);
    let mut raw = Vec::new();
    for ch in line[marker_pos + '\u{fffe}'.len_utf8()..].chars() {
        let codepoint = ch as u32;
        if (0xe000..=0xe0ff).contains(&codepoint) {
            raw.push((codepoint - 0xe000) as u8);
        } else {
            if !raw.is_empty() {
                decoded.push_str(&String::from_utf8_lossy(&raw));
                raw.clear();
            }
            decoded.push(ch);
        }
    }
    if !raw.is_empty() {
        decoded.push_str(&String::from_utf8_lossy(&raw));
    }
    decoded
}

fn parse_unrar_technical_entries(stdout: &[u8]) -> Vec<TechnicalEntry> {
    let text = String::from_utf8_lossy(stdout);
    let mut entries = Vec::new();
    let mut current: Option<TechnicalEntry> = None;

    for line in text.lines() {
        let trimmed = line.trim_start();
        if let Some(name) = trimmed.strip_prefix("Name: ") {
            if let Some(entry) = current.take() {
                entries.push(entry);
            }
            current = Some(TechnicalEntry {
                name: decode_unrar_bare_list_line(name),
                kind: String::new(),
                target: None,
                size: None,
                packed_size: None,
                crc32: None,
                host_os: None,
                version: None,
            });
        } else if let Some(kind) = trimmed.strip_prefix("Type: ") {
            if let Some(entry) = current.as_mut() {
                entry.kind = kind.to_string();
            }
        } else if let Some(target) = trimmed.strip_prefix("Target: ")
            && let Some(entry) = current.as_mut()
        {
            entry.target = Some(decode_unrar_bare_list_line(target));
        } else if let Some(size) = trimmed.strip_prefix("Size: ") {
            if let Some(entry) = current.as_mut() {
                entry.size = size.parse().ok();
            }
        } else if let Some(size) = trimmed.strip_prefix("Packed size: ") {
            if let Some(entry) = current.as_mut() {
                entry.packed_size = size.parse().ok();
            }
        } else if let Some(crc) = trimmed.strip_prefix("CRC32: ") {
            if let Some(entry) = current.as_mut() {
                entry.crc32 = u32::from_str_radix(crc, 16).ok();
            }
        } else if let Some(host_os) = trimmed.strip_prefix("Host OS: ") {
            if let Some(entry) = current.as_mut() {
                entry.host_os = Some(host_os.to_string());
            }
        } else if let Some(version) = trimmed.strip_prefix("File version: ")
            && let Some(entry) = current.as_mut()
        {
            entry.version = version.parse().ok();
        }
    }

    if let Some(entry) = current {
        entries.push(entry);
    }

    entries
}

fn open_oracle_archive(paths: &[PathBuf], password: Option<&str>) -> RarArchive {
    let readers: Vec<Box<dyn ReadSeek>> = paths
        .iter()
        .map(|path| {
            Box::new(
                std::fs::File::open(path)
                    .unwrap_or_else(|err| panic!("failed to open {}: {err}", path.display())),
            ) as Box<dyn ReadSeek>
        })
        .collect();
    let mut archive = RarArchive::open_volumes(readers).unwrap_or_else(|err| {
        panic!(
            "failed to open volumes [{}]: {err}",
            paths
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    });
    if let Some(password) = password {
        archive.set_password(password);
    }
    archive
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TechnicalEntry {
    name: String,
    kind: String,
    target: Option<String>,
    size: Option<u64>,
    packed_size: Option<u64>,
    crc32: Option<u32>,
    host_os: Option<String>,
    version: Option<u64>,
}

fn member_listing_name(member: &weaver_unrar::MemberInfo) -> String {
    if let Some(version) = member.version {
        let suffix = format!(";{version}");
        if member.name.ends_with(&suffix) {
            member.name.clone()
        } else {
            format!("{}{suffix}", member.name)
        }
    } else {
        member.name.clone()
    }
}

fn member_listing_kind(member: &weaver_unrar::MemberInfo) -> &'static str {
    if member.is_hardlink {
        "Hard link"
    } else if member.is_file_copy {
        "File reference"
    } else if member.is_symlink {
        match member.host_os {
            weaver_unrar::HostOs::Windows => "Windows symbolic link",
            _ => "Unix symbolic link",
        }
    } else if member.is_directory {
        "Directory"
    } else {
        "File"
    }
}

fn member_listing_host_os(member: &weaver_unrar::MemberInfo) -> Option<String> {
    match member.host_os {
        weaver_unrar::HostOs::Windows => Some("Windows".to_string()),
        weaver_unrar::HostOs::Unix => Some("Unix".to_string()),
        weaver_unrar::HostOs::Darwin => match member.compression.format {
            ArchiveFormat::Rar4 | ArchiveFormat::Rar14 => Some("Mac OS".to_string()),
            ArchiveFormat::Rar5 => Some("Unix".to_string()),
        },
        weaver_unrar::HostOs::Unknown(_) => None,
    }
}

fn try_weaver_bare_list_names(
    paths: &[PathBuf],
    password: Option<&str>,
) -> weaver_unrar::RarResult<Vec<String>> {
    let readers: Vec<Box<dyn ReadSeek>> = paths
        .iter()
        .map(|path| {
            Ok(
                Box::new(std::fs::File::open(path).map_err(weaver_unrar::RarError::Io)?)
                    as Box<dyn ReadSeek>,
            )
        })
        .collect::<weaver_unrar::RarResult<_>>()?;
    let mut archive = RarArchive::open_volumes(readers)?;
    if let Some(password) = password {
        archive.set_password(password);
    }
    Ok(archive
        .metadata()
        .members
        .into_iter()
        .map(|member| member_listing_name(&member))
        .collect())
}

fn try_weaver_technical_entries(
    paths: &[PathBuf],
    password: Option<&str>,
) -> weaver_unrar::RarResult<Vec<TechnicalEntry>> {
    let readers: Vec<Box<dyn ReadSeek>> = paths
        .iter()
        .map(|path| {
            Ok(
                Box::new(std::fs::File::open(path).map_err(weaver_unrar::RarError::Io)?)
                    as Box<dyn ReadSeek>,
            )
        })
        .collect::<weaver_unrar::RarResult<_>>()?;
    let mut archive = RarArchive::open_volumes(readers)?;
    if let Some(password) = password {
        archive.set_password(password);
    }
    Ok(archive
        .metadata()
        .members
        .into_iter()
        .map(|member| TechnicalEntry {
            name: member_listing_name(&member),
            kind: member_listing_kind(&member).to_string(),
            target: member.link_target.as_ref().map(|target| {
                normalized_output_path(target)
                    .to_string_lossy()
                    .replace('\\', "/")
            }),
            size: (!member.is_directory).then_some(member.unpacked_size.unwrap_or(0)),
            packed_size: (!member.is_directory).then_some(member.compressed_size),
            crc32: member
                .crc32
                .or_else(|| (member.is_directory || member.is_hardlink).then_some(0)),
            host_os: member_listing_host_os(&member),
            version: member.version,
        })
        .collect())
}

fn archived_mtime_paths(paths: &[PathBuf], password: Option<&str>) -> BTreeSet<String> {
    let members = open_oracle_archive(paths, password).metadata().members;
    let output_members: Vec<_> = members
        .into_iter()
        .map(|member| {
            let name = normalized_output_path(&member.name)
                .to_string_lossy()
                .replace('\\', "/");
            (
                name,
                member.is_directory,
                member.is_symlink,
                member.is_hardlink,
                member.is_file_copy,
                member.version,
                member.mtime,
            )
        })
        .filter(|(_, _, _, _, _, version, _)| version.is_none())
        .collect();

    output_members
        .iter()
        .filter_map(
            |(name, is_directory, is_symlink, is_hardlink, is_file_copy, _, mtime)| {
                if mtime.is_none() {
                    return None;
                }
                if *is_symlink || *is_hardlink || *is_file_copy {
                    return None;
                }
                if *is_directory
                    && output_members
                        .iter()
                        .any(|(other, _, _, _, _, _, _)| other.starts_with(&format!("{name}/")))
                {
                    return None;
                }
                Some(name.clone())
            },
        )
        .collect()
}

fn extract_weaver_to_filesystem(paths: &[PathBuf], password: Option<&str>, out_dir: &Path) {
    let mut archive = open_oracle_archive(paths, password);
    let options = ExtractOptions {
        verify: true,
        password: password.map(str::to_owned),
        restore_owners: false,
    };
    let members = archive.metadata().members.clone();
    for (index, member) in members.iter().enumerate() {
        if member.version.is_some() {
            continue;
        }
        let out_path = out_dir.join(normalized_output_path(&member.name));
        archive
            .extract_member_to_file(index, &options, None, &out_path)
            .unwrap_or_else(|err| panic!("failed to extract {}: {err}", member.name));
    }
}

#[test]
fn imported_archive_flags_are_surfaced() {
    let rar4_locked = open_single("rar4", "ssokolow_rar3_locked.rar");
    let rar4_locked_metadata = rar4_locked.metadata();
    assert_eq!(rar4_locked_metadata.format, ArchiveFormat::Rar4);
    assert!(rar4_locked_metadata.is_locked);
    assert!(rar4_locked.is_locked());
    assert_eq!(rar4_locked.member_names(), vec!["testfile.txt"]);

    let rar5_locked = open_single("rar5", "ssokolow_rar5_locked.rar");
    let rar5_locked_metadata = rar5_locked.metadata();
    assert_eq!(rar5_locked_metadata.format, ArchiveFormat::Rar5);
    assert!(rar5_locked_metadata.is_locked);
    assert!(rar5_locked.is_locked());
    assert_eq!(rar5_locked.member_names(), vec!["testfile.txt"]);

    let rar4_recovery = open_single("rar4", "rar4_recovery.rar");
    assert!(rar4_recovery.metadata().has_recovery_record);
    assert!(rar4_recovery.has_recovery_record());

    let rar5_recovery = open_single("rar5", "rar5_recovery.rar");
    assert!(rar5_recovery.metadata().has_recovery_record);
    assert!(rar5_recovery.has_recovery_record());

    let rarity_check = open_single("rar4", "ssokolow_rar3_authenticity_verification.rar");
    assert_eq!(rarity_check.member_names(), vec!["testfile.txt"]);
}

#[test]
fn imported_sfx_fixtures_open_and_scan() {
    let ssokolow_sfx = [
        ("rar4", "ssokolow_rar3_dos_sfx.exe"),
        ("rar4", "ssokolow_rar3_wincon_sfx.exe"),
        ("rar4", "ssokolow_rar3_wingui_sfx.exe"),
        ("rar5", "ssokolow_rar5_linux_sfx.bin"),
        ("rar5", "ssokolow_rar5_wincon_sfx.exe"),
        ("rar5", "ssokolow_rar5_wingui_sfx.exe"),
    ];

    for (dir, filename) in ssokolow_sfx {
        let archive = open_single(dir, filename);
        assert_eq!(archive.member_names(), vec!["testfile.txt", "acknow.txt"]);
    }

    let mut rar4_sfx = open_single("rar4", "test_read_format_rar_sfx.exe");
    assert_eq!(
        rar4_sfx.member_names(),
        vec![
            "test.txt",
            "testshortcut.lnk",
            "testdir\\test.txt",
            "testdir",
            "testemptydir",
        ]
    );
    assert_eq!(extract_bytes(&mut rar4_sfx, 0), b"test text file\r\n");
    assert_eq!(extract_bytes(&mut rar4_sfx, 2), b"test text file\r\n");

    let mut rar5_sfx = open_single("rar5", "test_read_format_rar5_sfx.exe");
    assert_eq!(rar5_sfx.member_names(), vec!["test.txt.txt"]);
    assert_eq!(extract_bytes(&mut rar5_sfx, 0), b"123");
}

#[test]
fn imported_encrypted_filename_fixtures_extract_with_password() {
    let cases = [
        ("rar4", "test_read_format_rar4_encrypted_filenames.rar"),
        ("rar4", "test_read_format_rar4_solid_encrypted.rar"),
        (
            "rar4",
            "test_read_format_rar4_solid_encrypted_filenames.rar",
        ),
        ("rar5", "test_read_format_rar5_encrypted_filenames.rar"),
        (
            "rar5",
            "test_read_format_rar5_solid_encrypted_filenames.rar",
        ),
    ];
    let expected_names = ["a.txt", "b.txt", "c.txt", "d.txt"];

    for (dir, filename) in cases {
        let mut archive = open_single_with_password(dir, filename, LIBARCHIVE_PASSWORD);
        let metadata = archive.metadata();
        assert_eq!(
            metadata
                .members
                .iter()
                .map(|member| member.name.as_str())
                .collect::<Vec<_>>(),
            expected_names
        );
        assert!(metadata.members.iter().all(|member| member.is_encrypted));

        for (index, expected_name) in expected_names.iter().enumerate() {
            let expected = format!("This is from {expected_name}");
            assert_eq!(extract_bytes(&mut archive, index), expected.as_bytes());
        }
    }
}

#[test]
fn imported_rar5_metadata_and_hash_fixtures_are_exercised() {
    let mut blake2 = open_single("rar5", "test_read_format_rar5_blake2.rar");
    let blake2_metadata = blake2.metadata();
    assert!(matches!(
        blake2_metadata.members[0].hash,
        Some(FileHash::Blake2sp(_))
    ));
    let blake2_bytes = extract_bytes(&mut blake2, 0);
    assert_eq!(blake2_bytes.len(), 814);
    assert_eq!(crc32(&blake2_bytes), 0x7E5E_C49E);

    let hardlink = open_single("rar5", "test_read_format_rar5_hardlink.rar");
    let hardlink_metadata = hardlink.metadata();
    assert_eq!(hardlink_metadata.members[0].name, "file.txt");
    assert_eq!(hardlink_metadata.members[1].name, "hardlink.txt");
    assert!(hardlink_metadata.members[1].is_hardlink);
    assert_eq!(
        hardlink_metadata.members[1].link_target.as_deref(),
        Some("file.txt")
    );

    let owner = open_single("rar5", "test_read_format_rar5_owner.rar");
    let owner_metadata = owner.metadata();
    assert_eq!(
        owner_metadata.members[0].owner,
        Some(UnixOwnerInfo {
            user_name: Some("root".to_string()),
            group_name: Some("wheel".to_string()),
            user_name_raw: Some(b"root".to_vec()),
            group_name_raw: Some(b"wheel".to_vec()),
            uid: None,
            gid: None,
        })
    );
    assert_eq!(
        owner_metadata.members[1].owner,
        Some(UnixOwnerInfo {
            user_name: Some("nobody".to_string()),
            group_name: Some("nogroup".to_string()),
            user_name_raw: Some(b"nobody".to_vec()),
            group_name_raw: Some(b"nogroup".to_vec()),
            uid: None,
            gid: None,
        })
    );
    assert_eq!(
        owner_metadata.members[2].owner,
        Some(UnixOwnerInfo {
            user_name: None,
            group_name: None,
            user_name_raw: None,
            group_name_raw: None,
            uid: Some(9999),
            gid: Some(8888),
        })
    );

    let fileattr = open_single("rar5", "test_read_format_rar5_fileattr.rar");
    let fileattr_metadata = fileattr.metadata();
    let expected_attrs = [
        ("readonly.txt", true, false, false, false),
        ("hidden.txt", false, true, false, false),
        ("system.txt", false, false, true, false),
        ("ro_hidden.txt", true, true, false, false),
        ("dir_readonly", true, false, false, true),
        ("dir_hidden", false, true, false, true),
        ("dir_system", false, false, true, true),
        ("dir_rohidden", true, true, false, true),
    ];
    for (index, (name, readonly, hidden, system, is_dir)) in expected_attrs.into_iter().enumerate()
    {
        let member = &fileattr_metadata.members[index];
        assert_eq!(member.name, name);
        assert_eq!(member.attributes.is_readonly(), readonly);
        assert_eq!(member.attributes.is_hidden(), hidden);
        assert_eq!(member.attributes.is_system(), system);
        assert_eq!(member.is_directory, is_dir);
    }

    let win32 = open_single("rar5", "test_read_format_rar5_win32.rar");
    let win32_metadata = win32.metadata();
    assert!(win32_metadata.members[0].is_directory);
    assert_eq!(win32_metadata.members[3].name, "test2.bin");
    assert!(win32_metadata.members[3].attributes.is_readonly());

    let extra_field_version = open_single("rar5", "test_read_format_rar5_extra_field_version.rar");
    assert_eq!(
        extra_field_version.member_names(),
        vec!["bin/2to3;1", "bin/2to3"]
    );
}

#[test]
fn imported_filter_and_multifile_fixtures_extract() {
    let mut rar4_multifile = open_single("rar4", "rar4_multifile_lz.rar");
    let originals = [
        (
            "hello.txt",
            std::fs::read(fixture("originals", "hello.txt")).unwrap(),
        ),
        (
            "second.txt",
            std::fs::read(fixture("originals", "second.txt")).unwrap(),
        ),
        (
            "zeros_64k.bin",
            std::fs::read(fixture("originals", "zeros_64k.bin")).unwrap(),
        ),
    ];
    for (index, (name, expected)) in originals.into_iter().enumerate() {
        assert_eq!(rar4_multifile.member_names()[index], name);
        assert_eq!(extract_bytes(&mut rar4_multifile, index), expected);
    }

    let stored_manyfiles = open_single("rar5", "test_read_format_rar5_stored_manyfiles.rar");
    assert_eq!(
        stored_manyfiles.member_names(),
        vec!["make_uue.tcl", "cebula.txt", "test.bin"]
    );
    let mut stored_manyfiles = stored_manyfiles;
    let cebula = extract_bytes(&mut stored_manyfiles, 1);
    assert!(cebula.starts_with(b"Cebula"));

    let mut multiarchive = open_multi(
        "rar5",
        &[
            "test_read_format_rar5_multiarchive_solid.part01.rar",
            "test_read_format_rar5_multiarchive_solid.part02.rar",
            "test_read_format_rar5_multiarchive_solid.part03.rar",
            "test_read_format_rar5_multiarchive_solid.part04.rar",
        ],
    );
    assert_eq!(
        multiarchive.member_names(),
        vec![
            "cebula.txt",
            "test.bin",
            "test1.bin",
            "test2.bin",
            "test3.bin",
            "test4.bin",
            "test5.bin",
            "test6.bin",
            "elf-Linux-ARMv7-ls",
        ]
    );
    let first_crc = multiarchive.member_info(0).unwrap().crc32.unwrap();
    let first = extract_bytes(&mut multiarchive, 0);
    assert_eq!(crc32(&first), first_crc);

    let windows = open_single("rar4", "test_read_format_rar_windows.rar");
    assert!(windows.member_names().contains(&"testdir\\test.txt"));
    assert!(windows.member_names().contains(&"testemptydir"));

    let unicode = open_single("rar4", "test_read_format_rar_unicode.rar");
    assert_eq!(unicode.metadata().members.len(), 6);
    assert!(
        unicode
            .metadata()
            .members
            .iter()
            .any(|member| member.name == "abcdefghijklmnopqrsテスト.txt")
    );
}

#[test]
fn imported_filter_and_arm_gap_fixtures_extract() {
    let passing = [
        ("rar4", "test_read_format_rar_filter.rar"),
        ("rar4", "test_read_format_rar_multi_lzss_blocks.rar"),
        ("rar5", "test_read_format_rar5_arm.rar"),
    ];

    for (dir, filename) in passing {
        let outcome = catch_unwind(AssertUnwindSafe(|| corpus_outcome(dir, filename)))
            .unwrap_or_else(|_| panic!("fixture panicked during decode: {dir}/{filename}"));
        assert_eq!(
            outcome,
            CorpusOutcome::Completed,
            "expected successful extract for {dir}/{filename}"
        );
    }
}

#[test]
fn imported_rar5_bad_tables_matches_unrar_partial_zero_recovery() {
    let mut archive = open_single("rar5", "test_read_format_rar5_bad_tables.rar");
    let bytes = extract_bytes(&mut archive, 0);

    assert_eq!(bytes, vec![0; 34]);
}

#[test]
fn imported_bare_listing_matches_unrar_when_available() {
    let Ok(unrar_bin) = std::env::var("UNRAR_BIN") else {
        if oracle_required() {
            panic!(
                "WEAVER_REQUIRE_UNRAR_ORACLE is set but UNRAR_BIN is missing for imported fixture bare-list oracle"
            );
        }
        eprintln!(
            "skipping imported fixture bare-list oracle; set UNRAR_BIN to compare against local unrar"
        );
        return;
    };

    let mut compared = 0usize;
    for (label, paths, password) in collect_oracle_fixture_groups() {
        let output = run_unrar_bare_list(&unrar_bin, &paths, password);
        if !output.status.success() {
            continue;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let unrar_names: Vec<String> = stdout
            .lines()
            .filter(|line| !line.is_empty())
            .map(decode_unrar_bare_list_line)
            .collect();
        let weaver_names = match try_weaver_bare_list_names(&paths, password) {
            Ok(names) => names,
            Err(err) if unrar_names.is_empty() => {
                eprintln!("skipping empty corrupt bare-list group {label}: {err}");
                continue;
            }
            Err(err) => panic!("Weaver could not list {label} but UnRAR listed entries: {err}"),
        };
        assert_eq!(
            weaver_names, unrar_names,
            "bare listing mismatch for {label}"
        );
        compared += 1;
    }

    assert!(
        compared > 0,
        "UNRAR_BIN={unrar_bin} did not accept any imported fixture groups"
    );
}

#[test]
fn imported_technical_listing_types_match_unrar_when_available() {
    let Ok(unrar_bin) = std::env::var("UNRAR_BIN") else {
        if oracle_required() {
            panic!(
                "WEAVER_REQUIRE_UNRAR_ORACLE is set but UNRAR_BIN is missing for imported fixture technical-list oracle"
            );
        }
        eprintln!(
            "skipping imported fixture technical-list oracle; set UNRAR_BIN to compare against local unrar"
        );
        return;
    };

    let mut compared = 0usize;
    for (label, paths, password) in collect_oracle_fixture_groups() {
        let output = run_unrar_technical_list(&unrar_bin, &paths, password);
        if !output.status.success() {
            continue;
        }
        let unrar_entries = parse_unrar_technical_entries(&output.stdout);
        let weaver_entries = match try_weaver_technical_entries(&paths, password) {
            Ok(entries) => entries,
            Err(err) if unrar_entries.is_empty() => {
                eprintln!("skipping empty corrupt technical-list group {label}: {err}");
                continue;
            }
            Err(err) => {
                panic!("Weaver could not technically list {label} but UnRAR listed entries: {err}")
            }
        };

        assert_eq!(
            weaver_entries.len(),
            unrar_entries.len(),
            "technical listing entry count mismatch for {label}"
        );
        for (index, (weaver, unrar)) in weaver_entries.iter().zip(unrar_entries.iter()).enumerate()
        {
            assert_eq!(
                &weaver.name, &unrar.name,
                "technical listing name mismatch for {label} entry {index}"
            );
            assert_eq!(
                &weaver.kind, &unrar.kind,
                "technical listing type mismatch for {label} entry {index}"
            );
            if let Some(target) = &weaver.target {
                assert_eq!(
                    Some(target),
                    unrar.target.as_ref(),
                    "technical listing target mismatch for {label} entry {index}"
                );
            }
            assert_eq!(
                weaver.size, unrar.size,
                "technical listing size mismatch for {label} entry {index}"
            );
            let _ = (&weaver.packed_size, &unrar.packed_size);
            if let Some(crc32) = unrar.crc32 {
                assert_eq!(
                    weaver.crc32,
                    Some(crc32),
                    "technical listing CRC32 mismatch for {label} entry {index}"
                );
            }
            if let Some(host_os) = &weaver.host_os {
                assert_eq!(
                    Some(host_os),
                    unrar.host_os.as_ref(),
                    "technical listing host-OS mismatch for {label} entry {index}"
                );
            }
            assert_eq!(
                weaver.version, unrar.version,
                "technical listing file-version mismatch for {label} entry {index}"
            );
        }
        compared += 1;
    }

    assert!(
        compared > 0,
        "UNRAR_BIN={unrar_bin} did not accept any imported fixture groups"
    );
}

#[test]
fn imported_filesystem_outputs_match_unrar_when_available() {
    let Ok(unrar_bin) = std::env::var("UNRAR_BIN") else {
        if oracle_required() {
            panic!(
                "WEAVER_REQUIRE_UNRAR_ORACLE is set but UNRAR_BIN is missing for imported fixture filesystem oracle"
            );
        }
        eprintln!(
            "skipping imported fixture filesystem oracle; set UNRAR_BIN to compare against local unrar"
        );
        return;
    };

    let mut compared = 0usize;
    for (label, paths, password) in collect_oracle_fixture_groups() {
        let unrar_out = TempDir::new().unwrap();
        let status = run_unrar_extract(&unrar_bin, &paths, password, unrar_out.path());
        if !status.success() {
            continue;
        }

        let weaver_out = TempDir::new().unwrap();
        extract_weaver_to_filesystem(&paths, password, weaver_out.path());

        let compare_mtime_paths = archived_mtime_paths(&paths, password);
        let unrar_snapshot = snapshot_filesystem(unrar_out.path(), &compare_mtime_paths);
        let weaver_snapshot = snapshot_filesystem(weaver_out.path(), &compare_mtime_paths);
        assert_eq!(
            weaver_snapshot, unrar_snapshot,
            "filesystem extraction mismatch for {label}"
        );
        compared += 1;
    }

    assert!(
        compared > 0,
        "UNRAR_BIN={unrar_bin} did not accept any imported fixture groups"
    );
}

#[cfg(feature = "slow-tests")]
#[test]
fn imported_ppmd_transition_fixture_extracts_successfully() {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        corpus_outcome("rar4", "test_read_format_rar_ppmd_lzss_conversion.rar")
    }))
    .unwrap_or_else(|_| {
        panic!("fixture panicked during decode: rar4/test_read_format_rar_ppmd_lzss_conversion.rar")
    });
    assert_eq!(
        outcome,
        CorpusOutcome::Completed,
        "expected successful extract for rar4/test_read_format_rar_ppmd_lzss_conversion.rar"
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CorpusOutcome {
    RejectedOnOpen,
    RejectedOnExtract,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedOutcome {
    Failure,
    Completed,
}

fn corpus_outcome(dir: &str, filename: &str) -> CorpusOutcome {
    let data = std::fs::read(fixture(dir, filename)).unwrap();
    let mut archive = match RarArchive::open(Cursor::new(data)) {
        Ok(archive) => archive,
        Err(_) => return CorpusOutcome::RejectedOnOpen,
    };

    let member_count = archive.metadata().members.len();
    for index in 0..member_count {
        if archive
            .extract_member(index, &ExtractOptions::default(), None)
            .is_err()
        {
            return CorpusOutcome::RejectedOnExtract;
        }
    }

    CorpusOutcome::Completed
}

#[test]
fn malformed_imported_corpus_is_rejected_or_handled_without_panicking() {
    let cases = [
        (
            "rar4",
            "test_read_format_rar_invalid1.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_noeof.rar",
            ExpectedOutcome::Completed,
        ),
        (
            "rar4",
            "test_read_format_rar_overflow.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_ppmd_use_after_free.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_ppmd_use_after_free2.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_newsub_huge.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_endarc_huge.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_symlink_huge.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_bad_tables.rar",
            ExpectedOutcome::Completed,
        ),
        (
            "rar5",
            "test_read_format_rar5_bad_window_sz_in_mltarc_file.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_block_size_is_too_small.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_data_ready_pointer_leak.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_decode_number_out_of_bounds_read.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_distance_overflow.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_invalid_dict_reference.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_invalid_hash_valid_htime_exfld.rar",
            ExpectedOutcome::Completed,
        ),
        (
            "rar5",
            "test_read_format_rar5_leftshift1.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_leftshift2.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_loop_bug.rar",
            ExpectedOutcome::Completed,
        ),
        (
            "rar5",
            "test_read_format_rar5_only_crypt_exfld.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_readtables_overflow.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_truncated_huff.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_unsupported_exfld.rar",
            ExpectedOutcome::Completed,
        ),
        (
            "rar5",
            "test_read_format_rar5_window_buf_and_size_desync.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_arm_filter_on_window_boundary.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_different_window_size.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_different_solid_window_size.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_nonempty_dir_stream.rar",
            ExpectedOutcome::Failure,
        ),
    ];

    for (dir, filename, expected) in cases {
        let outcome = catch_unwind(AssertUnwindSafe(|| corpus_outcome(dir, filename)))
            .unwrap_or_else(|_| panic!("fixture panicked during decode: {dir}/{filename}"));

        match expected {
            ExpectedOutcome::Failure => {
                assert!(
                    matches!(
                        outcome,
                        CorpusOutcome::RejectedOnOpen | CorpusOutcome::RejectedOnExtract
                    ),
                    "expected failure for {dir}/{filename}, got {outcome:?}"
                );
            }
            ExpectedOutcome::Completed => {
                assert_eq!(
                    outcome,
                    CorpusOutcome::Completed,
                    "expected graceful completion for {dir}/{filename}"
                );
            }
        }
    }
}
