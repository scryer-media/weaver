use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

struct ConfiguredFailpoint {
    name: String,
    fired: AtomicBool,
}

struct ConfiguredDelay {
    name: String,
    millis: u64,
    fired: AtomicBool,
}

fn configured_failpoint() -> &'static Option<ConfiguredFailpoint> {
    static FAILPOINT: OnceLock<Option<ConfiguredFailpoint>> = OnceLock::new();
    FAILPOINT.get_or_init(|| {
        std::env::var("WEAVER_E2E_FAILPOINT")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(|name| ConfiguredFailpoint {
                name,
                fired: AtomicBool::new(false),
            })
    })
}

fn configured_delay() -> &'static Option<ConfiguredDelay> {
    static DELAY: OnceLock<Option<ConfiguredDelay>> = OnceLock::new();
    DELAY.get_or_init(|| {
        let raw = std::env::var("WEAVER_E2E_DELAY").ok()?;
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        let (name, millis) = raw.rsplit_once('=')?;
        let millis = millis.trim().parse::<u64>().ok()?;
        if millis == 0 {
            return None;
        }
        Some(ConfiguredDelay {
            name: name.trim().to_string(),
            millis,
            fired: AtomicBool::new(false),
        })
    })
}

pub fn maybe_delay(name: &str) {
    let Some(configured) = configured_delay().as_ref() else {
        return;
    };
    if configured.name != name {
        return;
    }
    if configured
        .fired
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    tracing::warn!(
        delay_ms = configured.millis,
        hook = name,
        "tripping e2e delay hook"
    );
    std::thread::sleep(std::time::Duration::from_millis(configured.millis));
}

pub fn maybe_trip(name: &str) {
    let Some(configured) = configured_failpoint().as_ref() else {
        return;
    };
    if configured.name != name {
        return;
    }
    if configured
        .fired
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    tracing::warn!(failpoint = name, "tripping e2e failpoint");
    std::process::abort();
}
