// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

pub const STATUS_ENTER_PAUSED: &str = "status.enter_paused";
pub const STATUS_ENTER_VERIFYING: &str = "status.enter_verifying";
pub const STATUS_ENTER_REPAIRING: &str = "status.enter_repairing";
pub const STATUS_ENTER_QUEUED_REPAIR: &str = "status.enter_queued_repair";
pub const STATUS_ENTER_QUEUED_EXTRACT: &str = "status.enter_queued_extract";

// Direct-store coverage barrier step boundaries. One name per
// step so a container crash test can budget exactly one run each and assert
// that floors are published only after the checkpoint is durable. The sync hook
// trips *after* a destination is synced, so it reproduces a mid-sync crash —
// one file durable, the next not — rather than a crash before any sync.
pub const DIRECT_STORE_BARRIER_DRAIN: &str = "direct_store.barrier.drain";
pub const DIRECT_STORE_BARRIER_SYNC: &str = "direct_store.barrier.sync";
pub const DIRECT_STORE_BARRIER_PERSIST: &str = "direct_store.barrier.persist";
pub const DIRECT_STORE_BARRIER_PUBLISH: &str = "direct_store.barrier.publish";

// A segment batch's disk write on the owner thread, before any byte lands.
pub const DISK_WRITE: &str = "download.disk_write";

// A PAR2 repair on its blocking worker, before the session reads anything.
pub const PAR2_REPAIR_RUN: &str = "repair.par2_run";

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

// A test holds a named point for every caller whose path lies under one
// root, until it releases it. Keyed by root so a hold reaches only the test
// that installed it. Compiled only into tests.
#[cfg(test)]
pub(crate) struct Hold {
    name: &'static str,
    root: std::path::PathBuf,
    released: std::sync::Mutex<bool>,
    wake: std::sync::Condvar,
    arrived: tokio::sync::Notify,
}

#[cfg(test)]
static HOLDS: std::sync::Mutex<Vec<std::sync::Arc<Hold>>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
impl Hold {
    pub(crate) fn install(name: &'static str, root: &std::path::Path) -> std::sync::Arc<Self> {
        let hold = std::sync::Arc::new(Self {
            name,
            root: root.to_path_buf(),
            released: std::sync::Mutex::new(false),
            wake: std::sync::Condvar::new(),
            arrived: tokio::sync::Notify::new(),
        });
        HOLDS
            .lock()
            .expect("hold registry poisoned")
            .push(std::sync::Arc::clone(&hold));
        hold
    }

    // Resolves once a caller has reached the point and is parked there.
    pub(crate) async fn arrived(&self) {
        self.arrived.notified().await;
    }

    pub(crate) fn release(self: &std::sync::Arc<Self>) {
        HOLDS
            .lock()
            .expect("hold registry poisoned")
            .retain(|hold| !std::sync::Arc::ptr_eq(hold, self));
        *self.released.lock().expect("hold poisoned") = true;
        self.wake.notify_all();
    }
}

// Parks the caller while a test holds `name` for a root above `path`.
pub fn maybe_hold(name: &str, path: &std::path::Path) {
    #[cfg(test)]
    {
        let hold = HOLDS
            .lock()
            .expect("hold registry poisoned")
            .iter()
            .find(|hold| hold.name == name && path.starts_with(&hold.root))
            .cloned();
        if let Some(hold) = hold {
            hold.arrived.notify_one();
            let mut released = hold.released.lock().expect("hold poisoned");
            while !*released {
                released = hold.wake.wait(released).expect("hold poisoned");
            }
        }
    }
    #[cfg(not(test))]
    let _ = (name, path);
}
