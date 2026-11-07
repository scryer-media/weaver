// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

static PREFERRED_CPUS: OnceLock<Vec<usize>> = OnceLock::new();
#[cfg(target_os = "linux")]
static ALLOWED_CPUS: OnceLock<Vec<usize>> = OnceLock::new();
static NEXT_CPU: AtomicUsize = AtomicUsize::new(0);
const TOKIO_WORKER_THREADS_ENV: &str = "WEAVER_TOKIO_WORKER_THREADS";

pub fn install_tokio_worker_affinity(builder: &mut tokio::runtime::Builder) {
    // Capture the process/container allowance before any runtime thread is pinned.
    #[cfg(target_os = "linux")]
    let _ = allowed_cpus();
    let cpus = preferred_cpus();
    if let Some(worker_threads) = tokio_worker_threads_override() {
        builder.worker_threads(worker_threads);
    } else if !cpus.is_empty() {
        builder.worker_threads(cpus.len());
    }

    if cpus.is_empty() {
        return;
    }

    let cpus_for_threads = cpus.to_vec();
    builder.on_thread_start(move || {
        pin_current_thread_round_robin(&cpus_for_threads);
    });
}

pub fn pin_current_thread_for_hot_download_path() {
    let cpus = preferred_cpus();
    if cpus.is_empty() {
        return;
    }
    pin_current_thread_round_robin(cpus);
}

// Blocking threads share the download workers' pinning. Post-processing must
// expose the original CPU allowance before a pool or decoder sizes or creates
// its workers: new threads inherit their parent's affinity, too.
pub(crate) struct PostprocessAffinity {
    #[cfg(target_os = "linux")]
    previous: Option<Vec<usize>>,
    // The mask must be restored on the thread that entered the scope.
    _thread: PhantomData<Rc<()>>,
}

impl PostprocessAffinity {
    pub(crate) fn enter() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::enter_with_cpus(ALLOWED_CPUS.get().map_or(&[], Vec::as_slice))
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self {
                _thread: PhantomData,
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn enter_with_cpus(cpus: &[usize]) -> Self {
        if cpus.is_empty() {
            return Self {
                previous: None,
                _thread: PhantomData,
            };
        }
        let previous = current_thread_cpus().filter(|_| set_current_thread_cpus(cpus));
        if previous.is_none() {
            tracing::warn!(
                error = %std::io::Error::last_os_error(),
                "could not restore the allowed CPU mask for post-processing"
            );
        }
        Self {
            previous,
            _thread: PhantomData,
        }
    }
}

impl Drop for PostprocessAffinity {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if let Some(previous) = &self.previous
            && !set_current_thread_cpus(previous)
        {
            tracing::warn!(
                error = %std::io::Error::last_os_error(),
                "could not restore creator thread CPU affinity after post-processing"
            );
        }
    }
}

#[cfg(target_os = "linux")]
fn allowed_cpus() -> &'static [usize] {
    ALLOWED_CPUS
        .get_or_init(|| {
            current_thread_cpus().unwrap_or_else(|| {
                tracing::warn!("could not capture the allowed CPU mask before runtime startup");
                Vec::new()
            })
        })
        .as_slice()
}

#[cfg(target_os = "linux")]
fn current_thread_cpus() -> Option<Vec<usize>> {
    // SAFETY: cpu_set_t accepts a zeroed value and the syscall writes within it.
    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    if unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&set), &mut set) } != 0 {
        return None;
    }
    Some(
        (0..libc::CPU_SETSIZE as usize)
            // SAFETY: every index is within the fixed CPU set.
            .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
            .collect(),
    )
}

#[cfg(target_os = "linux")]
fn set_current_thread_cpus(cpus: &[usize]) -> bool {
    if cpus.is_empty() || cpus.iter().any(|&cpu| cpu >= libc::CPU_SETSIZE as usize) {
        return false;
    }
    // SAFETY: cpu_set_t accepts a zeroed value; every CPU index was checked above.
    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    for &cpu in cpus {
        unsafe { libc::CPU_SET(cpu, &mut set) };
    }
    // SAFETY: the syscall reads a valid CPU set for this thread only.
    unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) == 0 }
}

fn preferred_cpus() -> &'static [usize] {
    PREFERRED_CPUS.get_or_init(detect_preferred_cpus).as_slice()
}

fn tokio_worker_threads_override() -> Option<usize> {
    match std::env::var(TOKIO_WORKER_THREADS_ENV) {
        Ok(value) if value.trim().is_empty() => None,
        Ok(value) => match value.parse::<usize>() {
            Ok(threads) if threads > 0 => Some(threads),
            _ => panic!("{TOKIO_WORKER_THREADS_ENV} must be a positive integer"),
        },
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("failed to read {TOKIO_WORKER_THREADS_ENV}: {error}"),
    }
}

fn pin_current_thread_round_robin(cpus: &[usize]) {
    if cpus.is_empty() {
        return;
    }

    let idx = NEXT_CPU.fetch_add(1, Ordering::Relaxed) % cpus.len();
    pin_current_thread_to_cpu(cpus[idx]);
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn detect_preferred_cpus() -> Vec<usize> {
    use std::collections::HashSet;
    use std::fs;
    use std::path::Path;

    #[derive(Clone)]
    struct CpuInfo {
        cpu: usize,
        package_id: String,
        core_id: String,
        max_freq_khz: u64,
    }

    fn read_trimmed(path: &Path) -> Option<String> {
        fs::read_to_string(path).ok().map(|s| s.trim().to_string())
    }

    fn read_freq(cpu_dir: &Path) -> Option<u64> {
        let cpufreq = cpu_dir.join("cpufreq");
        for name in ["cpuinfo_max_freq", "scaling_max_freq"] {
            if let Some(value) = read_trimmed(&cpufreq.join(name))
                && let Ok(freq) = value.parse::<u64>()
            {
                return Some(freq);
            }
        }
        None
    }

    let mut infos = Vec::new();
    let Ok(entries) = fs::read_dir("/sys/devices/system/cpu") else {
        return Vec::new();
    };

    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some(cpu_suffix) = name.strip_prefix("cpu") else {
            continue;
        };
        let Ok(cpu) = cpu_suffix.parse::<usize>() else {
            continue;
        };
        if !allowed_cpus().contains(&cpu) {
            continue;
        }

        let cpu_dir = entry.path();
        let Some(max_freq_khz) = read_freq(&cpu_dir) else {
            continue;
        };

        let topology = cpu_dir.join("topology");
        let package_id =
            read_trimmed(&topology.join("physical_package_id")).unwrap_or_else(|| "0".to_string());
        let core_id = read_trimmed(&topology.join("core_id")).unwrap_or_else(|| cpu.to_string());

        infos.push(CpuInfo {
            cpu,
            package_id,
            core_id,
            max_freq_khz,
        });
    }

    let Some(max_freq_khz) = infos.iter().map(|info| info.max_freq_khz).max() else {
        return Vec::new();
    };

    infos.sort_by_key(|info| info.cpu);
    let mut seen_cores = HashSet::new();
    let mut selected = Vec::new();

    for info in infos
        .into_iter()
        .filter(|info| info.max_freq_khz == max_freq_khz)
    {
        let core_key = format!("{}:{}", info.package_id, info.core_id);
        if !seen_cores.insert(core_key) {
            continue;
        }
        selected.push(info.cpu);
        if selected.len() >= 4 {
            break;
        }
    }

    selected
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn detect_preferred_cpus() -> Vec<usize> {
    Vec::new()
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn pin_current_thread_to_cpu(cpu: usize) {
    // SAFETY: `cpu_set_t` is a plain old data structure and zeroed is valid.
    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    // SAFETY: macros expect a valid pointer to cpu_set_t.
    unsafe {
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        let _ = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn pin_current_thread_to_cpu(_: usize) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn extraction_affinity_restores_runtime_allowance_and_child_inheritance() {
        std::thread::spawn(|| {
            let allowed = current_thread_cpus().expect("read owned thread affinity");
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            install_tokio_worker_affinity(&mut builder);
            assert_eq!(allowed_cpus(), allowed);
            assert!(preferred_cpus().iter().all(|cpu| allowed.contains(cpu)));
            let runtime = builder.worker_threads(1).build().unwrap();
            runtime.block_on(async move {
                tokio::task::spawn_blocking(move || {
                    let pinned = vec![allowed[0]];
                    assert!(set_current_thread_cpus(&pinned));
                    {
                        let _scope = PostprocessAffinity::enter();
                        assert_eq!(current_thread_cpus().unwrap(), allowed);
                        let inherited = std::thread::spawn(current_thread_cpus).join().unwrap();
                        assert_eq!(inherited.unwrap(), allowed);
                    }
                    assert_eq!(current_thread_cpus().unwrap(), pinned);
                })
                .await
                .unwrap();
            });
        })
        .join()
        .unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn extraction_affinity_obeys_restricted_allowance_and_restores_on_unwind() {
        std::thread::spawn(|| {
            let original = current_thread_cpus().expect("read owned thread affinity");
            let allowed: Vec<_> = original.iter().copied().take(2).collect();
            let pinned = vec![allowed[0]];
            assert!(set_current_thread_cpus(&pinned));
            let result = std::panic::catch_unwind(|| {
                let _scope = PostprocessAffinity::enter_with_cpus(&allowed);
                assert_eq!(current_thread_cpus().unwrap(), allowed);
                let inherited = std::thread::spawn(current_thread_cpus).join().unwrap();
                assert_eq!(inherited.unwrap(), allowed);
                panic!("exercise affinity restoration during unwind");
            });
            let error = result.expect_err("the controlled panic must unwind");
            assert_eq!(
                error.downcast_ref::<&str>(),
                Some(&"exercise affinity restoration during unwind")
            );
            assert_eq!(current_thread_cpus().unwrap(), pinned);
        })
        .join()
        .unwrap();
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn extraction_affinity_is_a_noop_off_linux() {
        let _scope = PostprocessAffinity::enter();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn postprocess_pool_workers_inherit_allowance_after_creation_and_replacement() {
        std::thread::spawn(|| {
            let allowed = current_thread_cpus().expect("read owned thread affinity");
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            install_tokio_worker_affinity(&mut builder);
            assert_eq!(allowed_cpus(), allowed);
            let pinned = vec![allowed[0]];
            assert!(set_current_thread_cpus(&pinned));
            for threads in [2, 3] {
                let pool = crate::runtime::postprocess_pool::build_postprocess_pool(threads);
                assert_eq!(current_thread_cpus().unwrap(), pinned);
                let masks = pool.broadcast(|_| current_thread_cpus().unwrap());
                assert_eq!(masks.len(), threads);
                for mask in masks {
                    assert_eq!(mask, allowed);
                }
            }
        })
        .join()
        .unwrap();
    }
}
