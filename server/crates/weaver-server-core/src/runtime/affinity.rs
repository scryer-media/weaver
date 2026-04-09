use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

static PREFERRED_CPUS: OnceLock<Vec<usize>> = OnceLock::new();
static NEXT_CPU: AtomicUsize = AtomicUsize::new(0);

pub fn install_tokio_worker_affinity(builder: &mut tokio::runtime::Builder) {
    let cpus = preferred_cpus();
    if cpus.is_empty() {
        return;
    }

    builder.worker_threads(cpus.len());

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

fn preferred_cpus() -> &'static [usize] {
    PREFERRED_CPUS.get_or_init(detect_preferred_cpus).as_slice()
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
