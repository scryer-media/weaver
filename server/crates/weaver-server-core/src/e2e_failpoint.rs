// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

pub const STATUS_ENTER_PAUSED: &str = "status.enter_paused";
pub const STATUS_ENTER_VERIFYING: &str = "status.enter_verifying";
pub const STATUS_ENTER_REPAIRING: &str = "status.enter_repairing";
pub const STATUS_ENTER_QUEUED_REPAIR: &str = "status.enter_queued_repair";
pub const STATUS_ENTER_QUEUED_EXTRACT: &str = "status.enter_queued_extract";

// After a recovery set's repair has ruled and before the job's delivery
// moves anything, off the pipeline's loop, so a held job stands still while the
// queue keeps moving.
pub const DIRECT_STORE_POST_REPAIR_VERIFY: &str = "direct_store.post_repair_verify";

struct ConfiguredFailpoint {
    name: String,
    fired: AtomicBool,
}

struct ConfiguredDelay {
    name: String,
    millis: u64,
    fired: AtomicBool,
}

struct ConfiguredShutdownHold {
    name: String,
    fired: AtomicBool,
}

// Whether the scheduler has been told to shut down, for a hook held until
// then. Set by [`release_shutdown_holds`], never cleared: a process that has
// been told to stop is not told to carry on.
static SHUTDOWN_SIGNALLED: std::sync::Mutex<bool> = std::sync::Mutex::new(false);
static SHUTDOWN_WAKE: std::sync::Condvar = std::sync::Condvar::new();

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

fn configured_shutdown_hold() -> &'static Option<ConfiguredShutdownHold> {
    static HOLD: OnceLock<Option<ConfiguredShutdownHold>> = OnceLock::new();
    HOLD.get_or_init(|| {
        std::env::var("WEAVER_E2E_HOLD")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(|name| ConfiguredShutdownHold {
                name,
                fired: AtomicBool::new(false),
            })
    })
}

// Releases every hook held by `WEAVER_E2E_HOLD`, and lets any hook reached
// afterwards through at once. Called when the scheduler is told to shut down.
pub fn release_shutdown_holds() {
    *SHUTDOWN_SIGNALLED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    SHUTDOWN_WAKE.notify_all();
}

// The first caller to reach `name`, when `WEAVER_E2E_HOLD` names it, parks
// there until the scheduler is told to shut down.
fn maybe_hold_until_shutdown(name: &str) {
    let Some(configured) = configured_shutdown_hold().as_ref() else {
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

    tracing::warn!(hook = name, "holding e2e delay hook until shutdown");
    let mut signalled = SHUTDOWN_SIGNALLED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while !*signalled {
        signalled = SHUTDOWN_WAKE
            .wait(signalled)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    drop(signalled);
    tracing::warn!(hook = name, "releasing e2e delay hook for shutdown");
}

// A named delay hook: sleeps once for `WEAVER_E2E_DELAY=<name>=<millis>`, or
// holds once for `WEAVER_E2E_HOLD=<name>` until the process is told to shut
// down.
pub fn maybe_delay(name: &str) {
    maybe_hold_until_shutdown(name);
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
