use anyhow::{Result, anyhow};
use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Arch {
    Amd64,
    Arm64,
    Unknown,
}

impl Arch {
    pub fn from_machine(machine: &str) -> Self {
        match machine.trim().to_ascii_lowercase().as_str() {
            "x86_64" | "amd64" => Self::Amd64,
            "aarch64" | "arm64" => Self::Arm64,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Lane {
    Portable,
    Haswell,
    CortexA76,
}

impl Lane {
    pub fn executable_name(self) -> &'static str {
        match self {
            Self::Portable => "weaver-portable",
            Self::Haswell => "weaver-haswell",
            Self::CortexA76 => "weaver-cortex-a76",
        }
    }

    pub fn payload_file_name(self) -> &'static str {
        match self {
            Self::Portable => "weaver-portable.zst",
            Self::Haswell => "weaver-haswell.zst",
            Self::CortexA76 => "weaver-cortex-a76.zst",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadCandidate {
    pub lane: Lane,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrivilegeMode {
    Direct,
    DropPrivileges { uid: u32, gid: u32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchPlan {
    pub arch: Arch,
    pub selected_lane: Lane,
    pub payload_candidates: Vec<PayloadCandidate>,
    pub privilege_mode: PrivilegeMode,
}

pub const WEAVER_X86_64_REQUIRED_FEATURES: &[&str] = &[
    "avx",
    "avx2",
    "bmi1",
    "bmi2",
    "f16c",
    "fma",
    "lzcnt",
    "movbe",
    "pclmulqdq",
    "popcnt",
    "rdrand",
    "sse3",
    "sse4.1",
    "sse4.2",
    "ssse3",
    "xsave",
    "xsaveopt",
];

pub const WEAVER_ARM64_REQUIRED_FEATURES: &[&str] = &[
    "aes", "crc32", "dotprod", "fp16", "lse", "neon", "rdm", "sha2",
];

pub fn normalize_x86_feature(token: &str) -> Option<&'static str> {
    match token.trim().to_ascii_lowercase().as_str() {
        "avx" | "avx1.0" => Some("avx"),
        "avx2" | "avx2.0" => Some("avx2"),
        "bmi1" => Some("bmi1"),
        "bmi2" => Some("bmi2"),
        "f16c" => Some("f16c"),
        "fma" => Some("fma"),
        "abm" | "lzcnt" => Some("lzcnt"),
        "movbe" => Some("movbe"),
        "pclmul" | "pclmulqdq" => Some("pclmulqdq"),
        "popcnt" => Some("popcnt"),
        "rdrand" => Some("rdrand"),
        "sse3" => Some("sse3"),
        "sse4_1" | "sse4.1" => Some("sse4.1"),
        "sse4_2" | "sse4.2" => Some("sse4.2"),
        "ssse3" => Some("ssse3"),
        "osxsave" | "xsave" => Some("xsave"),
        "xsaveopt" => Some("xsaveopt"),
        _ => None,
    }
}

pub fn normalize_arm64_feature(token: &str) -> Option<&'static str> {
    match token.trim().to_ascii_lowercase().as_str() {
        "aes" => Some("aes"),
        "crc" | "crc32" => Some("crc32"),
        "asimd" | "neon" => Some("neon"),
        "fphp" | "asimdhp" | "fp16" => Some("fp16"),
        "atomics" | "lse" => Some("lse"),
        "asimdrdm" | "rdm" => Some("rdm"),
        "asimddp" | "dotprod" => Some("dotprod"),
        "sha2" => Some("sha2"),
        _ => None,
    }
}

pub fn normalized_features_from_cpuinfo_text(cpuinfo: &str, arch: Arch) -> BTreeSet<String> {
    let mut features = BTreeSet::new();
    for line in cpuinfo.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        if !key.eq_ignore_ascii_case("flags") && !key.eq_ignore_ascii_case("features") {
            continue;
        }
        for token in value.split_whitespace() {
            let normalized = match arch {
                Arch::Amd64 => normalize_x86_feature(token),
                Arch::Arm64 => normalize_arm64_feature(token),
                Arch::Unknown => None,
            };
            if let Some(feature) = normalized {
                features.insert(feature.to_string());
            }
        }
    }
    features
}

pub fn select_lane_from_features(arch: Arch, features: &BTreeSet<String>) -> Lane {
    match arch {
        Arch::Amd64 => {
            if WEAVER_X86_64_REQUIRED_FEATURES
                .iter()
                .all(|required| features.contains(*required))
            {
                Lane::Haswell
            } else {
                Lane::Portable
            }
        }
        Arch::Arm64 => {
            if WEAVER_ARM64_REQUIRED_FEATURES
                .iter()
                .all(|required| features.contains(*required))
            {
                Lane::CortexA76
            } else {
                Lane::Portable
            }
        }
        Arch::Unknown => Lane::Portable,
    }
}

pub fn select_lane_from_cpuinfo(arch: Arch, cpuinfo: io::Result<String>) -> Lane {
    match cpuinfo {
        Ok(contents) => {
            let features = normalized_features_from_cpuinfo_text(&contents, arch);
            if features.is_empty() {
                Lane::Portable
            } else {
                select_lane_from_features(arch, &features)
            }
        }
        Err(_) => Lane::Portable,
    }
}

pub fn build_launch_plan(
    payload_root: &Path,
    arch: Arch,
    cpuinfo: io::Result<String>,
    current_uid: u32,
    puid: u32,
    pgid: u32,
) -> LaunchPlan {
    let selected_lane = select_lane_from_cpuinfo(arch, cpuinfo);
    let mut payload_candidates = vec![PayloadCandidate {
        lane: selected_lane,
        path: payload_root.join(selected_lane.payload_file_name()),
    }];
    if selected_lane != Lane::Portable {
        payload_candidates.push(PayloadCandidate {
            lane: Lane::Portable,
            path: payload_root.join(Lane::Portable.payload_file_name()),
        });
    }

    let privilege_mode = if current_uid == 0 {
        PrivilegeMode::DropPrivileges {
            uid: puid,
            gid: pgid,
        }
    } else {
        PrivilegeMode::Direct
    };

    LaunchPlan {
        arch,
        selected_lane,
        payload_candidates,
        privilege_mode,
    }
}

pub fn attempt_payloads<T, F>(payload_candidates: &[PayloadCandidate], mut attempt: F) -> Result<T>
where
    F: FnMut(&PayloadCandidate) -> Result<T>,
{
    let mut last_error = None;
    for candidate in payload_candidates {
        match attempt(candidate) {
            Ok(value) => return Ok(value),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no payload candidates were provided")))
}

#[cfg(target_os = "linux")]
pub fn run_from_env() -> Result<()> {
    linux::run_from_env()
}

#[cfg(not(target_os = "linux"))]
pub fn run_from_env() -> Result<()> {
    Err(anyhow!("weaver-docker-launcher is only supported on Linux"))
}

#[cfg(target_os = "linux")]
pub fn launch_compressed_payload(
    payload_path: &Path,
    argv: &[std::ffi::OsString],
    env_pairs: &[(std::ffi::OsString, std::ffi::OsString)],
) -> Result<()> {
    linux::launch_compressed_payload(payload_path, argv, env_pairs)
}

#[cfg(not(target_os = "linux"))]
pub fn launch_compressed_payload(
    _payload_path: &Path,
    _argv: &[std::ffi::OsString],
    _env_pairs: &[(std::ffi::OsString, std::ffi::OsString)],
) -> Result<()> {
    Err(anyhow!(
        "compressed payload launching is only supported on Linux"
    ))
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{Arch, LaunchPlan, PayloadCandidate, PrivilegeMode, build_launch_plan};
    use anyhow::{Context, Result, anyhow, bail};
    use std::env;
    use std::ffi::{CStr, CString, OsStr, OsString};
    use std::fs::{self, File};
    use std::io::{self, Seek, SeekFrom, Write};
    use std::mem::MaybeUninit;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    const CONFIG_DIR: &str = "/config";
    const PAYLOAD_ROOT: &str = "/opt/weaver/payloads";

    pub(super) fn run_from_env() -> Result<()> {
        let arch = detect_arch()?;
        let current_uid = unsafe { libc::geteuid() } as u32;
        configure_umask()?;
        let (puid, pgid) = resolve_requested_owner_ids(current_uid)?;
        let args: Vec<OsString> = env::args_os().skip(1).collect();
        let mut env_pairs: Vec<(OsString, OsString)> = env::vars_os().collect();
        translate_log_env_alias(&mut env_pairs);
        let plan = build_launch_plan(
            Path::new(PAYLOAD_ROOT),
            arch,
            fs::read_to_string("/proc/cpuinfo"),
            current_uid,
            puid,
            pgid,
        );

        if let PrivilegeMode::DropPrivileges { uid, gid } = plan.privilege_mode {
            fs::create_dir_all(CONFIG_DIR)
                .with_context(|| format!("failed to create {CONFIG_DIR}"))?;
            chown_recursive(Path::new(CONFIG_DIR), uid, gid)?;
            print_banner(uid, gid);
            drop_privileges(uid, gid)?;
        }

        launch_from_plan(&plan, &args, &env_pairs)
    }

    fn parse_env_u32(key: &str, default: u32) -> Result<u32> {
        match env::var(key) {
            Ok(value) => {
                let value = value.trim();
                if value.is_empty() {
                    return Ok(default);
                }
                value
                    .parse::<u32>()
                    .with_context(|| format!("{key} must be an unsigned integer"))
            }
            Err(env::VarError::NotPresent) => Ok(default),
            Err(error) => Err(anyhow!("{key} is not valid UTF-8: {error}")),
        }
    }

    fn resolve_requested_owner_ids(current_uid: u32) -> Result<(u32, u32)> {
        if current_uid == 0 {
            Ok((parse_env_u32("PUID", 1000)?, parse_env_u32("PGID", 1000)?))
        } else {
            Ok((1000, 1000))
        }
    }

    fn configure_umask() -> Result<()> {
        match env::var("UMASK") {
            Ok(value) => {
                let value = value.trim();
                if value.is_empty() {
                    return Ok(());
                }
                let mask = parse_umask_value(value)?;
                unsafe {
                    libc::umask(mask);
                }
                Ok(())
            }
            Err(env::VarError::NotPresent) => Ok(()),
            Err(error) => Err(anyhow!("UMASK is not valid UTF-8: {error}")),
        }
    }

    fn translate_log_env_alias(env_pairs: &mut Vec<(OsString, OsString)>) {
        if env_pairs.iter().any(|(key, _)| key == "RUST_LOG") {
            return;
        }

        let log_value = env_pairs
            .iter()
            .rev()
            .find(|(key, value)| key == "LOG" && !value.is_empty())
            .map(|(_, value)| value.clone());
        if let Some(log_value) = log_value {
            env_pairs.push((OsString::from("RUST_LOG"), log_value));
        }
    }

    pub(super) fn parse_umask_value(value: &str) -> Result<libc::mode_t> {
        let value = value.trim();
        if value.len() != 3 && value.len() != 4 {
            bail!("UMASK must be a 3- or 4-digit octal value");
        }
        if !value.bytes().all(|byte| (b'0'..=b'7').contains(&byte)) {
            bail!("UMASK must contain only octal digits");
        }

        let parsed = u32::from_str_radix(value, 8)
            .with_context(|| format!("failed to parse UMASK value {value}"))?;
        if parsed > 0o777 {
            bail!("UMASK must be between 000 and 777");
        }
        Ok(parsed as libc::mode_t)
    }

    fn launch_from_plan(
        plan: &LaunchPlan,
        args: &[OsString],
        env_pairs: &[(OsString, OsString)],
    ) -> Result<()> {
        let mut last_error = None;
        for (index, candidate) in plan.payload_candidates.iter().enumerate() {
            let is_last = index + 1 == plan.payload_candidates.len();
            let exec_argv = exec_argv(candidate, args);
            match launch_compressed_payload(&candidate.path, &exec_argv, env_pairs) {
                Ok(()) => unreachable!("fexecve returned successfully"),
                Err(error) if !is_last => {
                    eprintln!(
                        "launcher: failed to start {} from {}: {error:#}; falling back to portable",
                        candidate.lane.executable_name(),
                        candidate.path.display(),
                    );
                    last_error = Some(error);
                }
                Err(error) => {
                    return Err(last_error.unwrap_or(error));
                }
            }
        }

        bail!("no payload candidates were available")
    }

    fn exec_argv(candidate: &PayloadCandidate, args: &[OsString]) -> Vec<OsString> {
        let mut exec_argv = Vec::with_capacity(args.len() + 1);
        exec_argv.push(OsString::from(candidate.lane.executable_name()));
        exec_argv.extend(args.iter().cloned());
        exec_argv
    }

    pub(super) fn launch_compressed_payload(
        payload_path: &Path,
        argv: &[OsString],
        env_pairs: &[(OsString, OsString)],
    ) -> Result<()> {
        let payload = File::open(payload_path)
            .with_context(|| format!("failed to open {}", payload_path.display()))?;
        let mut decoder = zstd::Decoder::new(payload).with_context(|| {
            format!(
                "failed to create zstd decoder for {}",
                payload_path.display()
            )
        })?;
        let memfd = create_memfd(
            argv.first()
                .map(OsString::as_os_str)
                .unwrap_or_else(|| OsStr::new("weaver")),
        )?;
        let mut memfd_file = File::from(memfd);

        io::copy(&mut decoder, &mut memfd_file)
            .with_context(|| format!("failed to decompress {}", payload_path.display()))?;
        memfd_file
            .flush()
            .with_context(|| format!("failed to flush {}", payload_path.display()))?;
        memfd_file
            .seek(SeekFrom::Start(0))
            .with_context(|| format!("failed to rewind {}", payload_path.display()))?;

        let memfd_raw = memfd_file.as_raw_fd();
        let chmod_result = unsafe { libc::fchmod(memfd_raw, 0o755) };
        if chmod_result != 0 {
            return Err(io::Error::last_os_error()).context("failed to mark memfd executable");
        }

        fexecve(memfd_raw, argv, env_pairs)
    }

    fn create_memfd(name: &OsStr) -> Result<OwnedFd> {
        const MFD_EXEC: libc::c_uint = 0x0010;

        let name = CString::new(name.as_bytes())
            .with_context(|| format!("memfd name contains NUL bytes: {:?}", name))?;
        let fallback_flags = libc::MFD_CLOEXEC;
        let preferred_flags = libc::MFD_CLOEXEC | MFD_EXEC;

        for flags in [preferred_flags, fallback_flags] {
            let fd = unsafe { libc::memfd_create(name.as_ptr(), flags) };
            if fd >= 0 {
                return Ok(unsafe { OwnedFd::from_raw_fd(fd) });
            }

            let error = io::Error::last_os_error();
            let is_compatibility_fallback =
                flags == preferred_flags && should_retry_without_mfd_exec(&error);
            if !is_compatibility_fallback {
                return Err(error).context("memfd_create failed");
            }
        }

        bail!("memfd_create did not produce a usable executable file descriptor")
    }

    fn fexecve(fd: i32, argv: &[OsString], env_pairs: &[(OsString, OsString)]) -> Result<()> {
        let argv_cstrings = os_strings_to_cstrings(argv).context("argv contains NUL bytes")?;
        let env_cstrings =
            env_pairs_to_cstrings(env_pairs).context("environment contains NUL bytes")?;
        let mut argv_ptrs: Vec<*const libc::c_char> =
            argv_cstrings.iter().map(|value| value.as_ptr()).collect();
        argv_ptrs.push(std::ptr::null());
        let mut env_ptrs: Vec<*const libc::c_char> =
            env_cstrings.iter().map(|value| value.as_ptr()).collect();
        env_ptrs.push(std::ptr::null());

        let result = unsafe { libc::fexecve(fd, argv_ptrs.as_ptr(), env_ptrs.as_ptr()) };
        if result == 0 {
            unreachable!("fexecve returned success unexpectedly");
        }

        let error = io::Error::last_os_error();
        if should_try_procfd_exec_fallback(&error) {
            let fd_path =
                CString::new(format!("/proc/self/fd/{fd}")).context("fd path contained NUL")?;
            let fallback_result =
                unsafe { libc::execve(fd_path.as_ptr(), argv_ptrs.as_ptr(), env_ptrs.as_ptr()) };
            if fallback_result == 0 {
                unreachable!("execve via /proc/self/fd returned success unexpectedly");
            }
            return Err(io::Error::last_os_error())
                .context("execve via /proc/self/fd failed after fexecve fallback");
        }

        Err(error).context("fexecve failed")
    }

    fn should_retry_without_mfd_exec(error: &io::Error) -> bool {
        matches!(
            error.raw_os_error(),
            Some(libc::EINVAL)
                | Some(libc::ENOSYS)
                | Some(libc::EOPNOTSUPP)
                | Some(libc::EPERM)
                | Some(libc::EACCES)
        )
    }

    fn should_try_procfd_exec_fallback(error: &io::Error) -> bool {
        matches!(
            error.raw_os_error(),
            Some(libc::ENOENT) | Some(libc::ENOSYS) | Some(libc::EPERM) | Some(libc::EACCES)
        )
    }

    fn os_strings_to_cstrings(values: &[OsString]) -> Result<Vec<CString>> {
        values
            .iter()
            .map(|value| CString::new(value.as_os_str().as_bytes()).map_err(anyhow::Error::from))
            .collect()
    }

    fn env_pairs_to_cstrings(values: &[(OsString, OsString)]) -> Result<Vec<CString>> {
        values
            .iter()
            .map(|(key, value)| {
                let mut bytes = Vec::with_capacity(
                    key.as_os_str().as_bytes().len() + value.as_os_str().as_bytes().len() + 1,
                );
                bytes.extend_from_slice(key.as_os_str().as_bytes());
                bytes.push(b'=');
                bytes.extend_from_slice(value.as_os_str().as_bytes());
                CString::new(bytes).map_err(anyhow::Error::from)
            })
            .collect()
    }

    fn detect_arch() -> Result<Arch> {
        let mut utsname = MaybeUninit::<libc::utsname>::zeroed();
        let result = unsafe { libc::uname(utsname.as_mut_ptr()) };
        if result != 0 {
            return Err(io::Error::last_os_error()).context("uname failed");
        }
        let utsname = unsafe { utsname.assume_init() };
        let machine = unsafe { CStr::from_ptr(utsname.machine.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        Ok(Arch::from_machine(&machine))
    }

    fn chown_recursive(path: &Path, uid: u32, gid: u32) -> Result<()> {
        lchown(path, uid, gid)?;
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("failed to stat {}", path.display()))?;
        if metadata.file_type().is_dir() {
            for entry in fs::read_dir(path)
                .with_context(|| format!("failed to read directory {}", path.display()))?
            {
                let entry =
                    entry.with_context(|| format!("failed to enumerate {}", path.display()))?;
                chown_recursive(&entry.path(), uid, gid)?;
            }
        }
        Ok(())
    }

    fn lchown(path: &Path, uid: u32, gid: u32) -> Result<()> {
        let path = CString::new(path.as_os_str().as_bytes())
            .with_context(|| format!("path contains NUL bytes: {}", path.display()))?;
        let result = unsafe { libc::lchown(path.as_ptr(), uid, gid) };
        if result != 0 {
            return Err(io::Error::last_os_error())
                .with_context(|| format!("failed to chown {}", path.to_string_lossy()));
        }
        Ok(())
    }

    fn drop_privileges(uid: u32, gid: u32) -> Result<()> {
        let groups = [gid as libc::gid_t];
        let setgroups_result = unsafe { libc::setgroups(groups.len(), groups.as_ptr()) };
        if setgroups_result != 0 {
            return Err(io::Error::last_os_error()).context("setgroups failed");
        }

        let setgid_result = unsafe { libc::setgid(gid as libc::gid_t) };
        if setgid_result != 0 {
            return Err(io::Error::last_os_error()).context("setgid failed");
        }

        let setuid_result = unsafe { libc::setuid(uid as libc::uid_t) };
        if setuid_result != 0 {
            return Err(io::Error::last_os_error()).context("setuid failed");
        }

        Ok(())
    }

    fn print_banner(uid: u32, gid: u32) {
        println!(
            "\n───────────────────────────────────\n  weaver\n  User UID:  {uid}\n  User GID:  {gid}\n  Config:    /config\n───────────────────────────────────\n"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Result, anyhow};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn amd64_selects_haswell_when_all_features_are_present() {
        let cpuinfo = "processor : 0\nflags     : fpu sse3 ssse3 sse4_1 sse4_2 avx avx2 bmi1 bmi2 f16c fma lzcnt movbe pclmulqdq popcnt rdrand xsave xsaveopt\n";
        assert_eq!(
            Lane::Haswell,
            select_lane_from_cpuinfo(Arch::Amd64, Ok(cpuinfo.to_string()))
        );
    }

    #[test]
    fn amd64_treats_abm_as_lzcnt() {
        let cpuinfo = "processor : 0\nflags     : fpu sse3 ssse3 sse4_1 sse4_2 avx avx2 bmi1 bmi2 f16c fma abm movbe pclmulqdq popcnt rdrand xsave xsaveopt\n";
        assert_eq!(
            Lane::Haswell,
            select_lane_from_cpuinfo(Arch::Amd64, Ok(cpuinfo.to_string()))
        );
    }

    #[test]
    fn amd64_falls_back_to_portable_when_features_are_missing() {
        let cpuinfo = "processor : 0\nflags     : fpu sse3 ssse3 sse4_1 sse4_2 avx bmi1 bmi2 f16c fma lzcnt movbe pclmulqdq popcnt rdrand xsave xsaveopt\n";
        assert_eq!(
            Lane::Portable,
            select_lane_from_cpuinfo(Arch::Amd64, Ok(cpuinfo.to_string()))
        );
    }

    #[test]
    fn arm64_selects_cortex_a76_when_all_features_are_present() {
        let cpuinfo =
            "processor : 0\nFeatures  : fp asimd aes sha2 crc32 atomics fphp asimdrdm asimddp\n";
        assert_eq!(
            Lane::CortexA76,
            select_lane_from_cpuinfo(Arch::Arm64, Ok(cpuinfo.to_string()))
        );
    }

    #[test]
    fn arm64_falls_back_to_portable_when_features_are_missing() {
        let cpuinfo = "processor : 0\nFeatures  : fp asimd aes sha2 crc32 atomics fphp asimdrdm\n";
        assert_eq!(
            Lane::Portable,
            select_lane_from_cpuinfo(Arch::Arm64, Ok(cpuinfo.to_string()))
        );
    }

    #[test]
    fn cpuinfo_feature_keys_are_case_insensitive() {
        let cpuinfo =
            "processor : 0\nfeatures  : fp asimd aes sha2 crc32 atomics fphp asimdrdm asimddp\n";
        assert_eq!(
            Lane::CortexA76,
            select_lane_from_cpuinfo(Arch::Arm64, Ok(cpuinfo.to_string()))
        );
    }

    #[test]
    fn unreadable_cpuinfo_defaults_to_portable() {
        assert_eq!(
            Lane::Portable,
            select_lane_from_cpuinfo(
                Arch::Amd64,
                Err(io::Error::new(io::ErrorKind::NotFound, "missing")),
            )
        );
    }

    #[test]
    fn launch_plan_adds_portable_fallback_for_optimized_lanes() {
        let cpuinfo = "processor : 0\nflags     : fpu sse3 ssse3 sse4_1 sse4_2 avx avx2 bmi1 bmi2 f16c fma lzcnt movbe pclmulqdq popcnt rdrand xsave xsaveopt\n";
        let plan = build_launch_plan(
            Path::new("/payloads"),
            Arch::Amd64,
            Ok(cpuinfo.to_string()),
            0,
            1000,
            1000,
        );

        assert_eq!(Lane::Haswell, plan.selected_lane);
        assert_eq!(
            vec![
                PayloadCandidate {
                    lane: Lane::Haswell,
                    path: PathBuf::from("/payloads/weaver-haswell.zst"),
                },
                PayloadCandidate {
                    lane: Lane::Portable,
                    path: PathBuf::from("/payloads/weaver-portable.zst"),
                },
            ],
            plan.payload_candidates
        );
    }

    #[test]
    fn launch_plan_uses_direct_mode_for_non_root() {
        let plan = build_launch_plan(
            Path::new("/payloads"),
            Arch::Amd64,
            Err(io::Error::new(io::ErrorKind::NotFound, "missing")),
            1001,
            1000,
            1000,
        );

        assert_eq!(PrivilegeMode::Direct, plan.privilege_mode);
    }

    #[test]
    fn launch_plan_uses_drop_privileges_mode_for_root() {
        let plan = build_launch_plan(
            Path::new("/payloads"),
            Arch::Amd64,
            Err(io::Error::new(io::ErrorKind::NotFound, "missing")),
            0,
            1234,
            2345,
        );

        assert_eq!(
            PrivilegeMode::DropPrivileges {
                uid: 1234,
                gid: 2345
            },
            plan.privilege_mode
        );
    }

    #[test]
    fn attempt_payloads_falls_back_to_portable_when_optimized_attempt_fails() -> Result<()> {
        let candidates = vec![
            PayloadCandidate {
                lane: Lane::Haswell,
                path: PathBuf::from("/payloads/weaver-haswell.zst"),
            },
            PayloadCandidate {
                lane: Lane::Portable,
                path: PathBuf::from("/payloads/weaver-portable.zst"),
            },
        ];
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen_for_attempt = Rc::clone(&seen);

        let result = attempt_payloads(&candidates, move |candidate| {
            seen_for_attempt.borrow_mut().push(candidate.lane);
            if candidate.lane == Lane::Haswell {
                Err(anyhow!("optimized payload failed"))
            } else {
                Ok(candidate.lane)
            }
        })?;

        assert_eq!(Lane::Portable, result);
        assert_eq!(vec![Lane::Haswell, Lane::Portable], *seen.borrow());
        Ok(())
    }

    #[test]
    fn attempt_payloads_hard_fails_when_portable_is_the_only_candidate() {
        let candidates = vec![PayloadCandidate {
            lane: Lane::Portable,
            path: PathBuf::from("/payloads/weaver-portable.zst"),
        }];

        let error =
            attempt_payloads::<(), _>(&candidates, |_| Err(anyhow!("portable payload failed")))
                .expect_err("portable-only payload should hard fail");
        assert!(error.to_string().contains("portable payload failed"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_umask_accepts_standard_octal_values() -> Result<()> {
        assert_eq!(0o022, linux::parse_umask_value("022")? as u32);
        assert_eq!(0o002, linux::parse_umask_value("002")? as u32);
        assert_eq!(0o0022, linux::parse_umask_value("0022")? as u32);
        assert_eq!(0o022, linux::parse_umask_value(" 022 ")? as u32);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_umask_rejects_invalid_values() {
        for invalid in ["22", "00022", "08", "abc", "0x22", "1777"] {
            let error = linux::parse_umask_value(invalid).expect_err("invalid UMASK should fail");
            assert!(error.to_string().contains("UMASK"));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resolve_requested_owner_ids_ignores_env_when_non_root() -> Result<()> {
        unsafe {
            std::env::set_var("PUID", "not-a-number");
            std::env::set_var("PGID", "still-not-a-number");
        }

        let ids = linux::resolve_requested_owner_ids(1000)?;

        unsafe {
            std::env::remove_var("PUID");
            std::env::remove_var("PGID");
        }

        assert_eq!((1000, 1000), ids);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resolve_requested_owner_ids_honors_blank_defaults_for_root() -> Result<()> {
        unsafe {
            std::env::set_var("PUID", "  ");
            std::env::set_var("PGID", "");
        }

        let ids = linux::resolve_requested_owner_ids(0)?;

        unsafe {
            std::env::remove_var("PUID");
            std::env::remove_var("PGID");
        }

        assert_eq!((1000, 1000), ids);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn translate_log_env_alias_sets_rust_log_when_missing() {
        let mut env_pairs = vec![(OsString::from("LOG"), OsString::from("weaver=debug,info"))];

        linux::translate_log_env_alias(&mut env_pairs);

        assert!(
            env_pairs
                .iter()
                .any(|(key, value)| key == "RUST_LOG" && value == "weaver=debug,info")
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn translate_log_env_alias_preserves_existing_rust_log() {
        let mut env_pairs = vec![
            (OsString::from("LOG"), OsString::from("info")),
            (OsString::from("RUST_LOG"), OsString::from("warn")),
        ];

        linux::translate_log_env_alias(&mut env_pairs);

        let rust_log_values: Vec<_> = env_pairs
            .iter()
            .filter(|(key, _)| key == "RUST_LOG")
            .map(|(_, value)| value.clone())
            .collect();
        assert_eq!(vec![OsString::from("warn")], rust_log_values);
    }
}
