// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The same-filesystem tests that compare the disk owner's answer with the
// staging choice, which lives in the pipeline.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;
use crate::runtime::StartupDiskGate;

// A year: long enough that no test ever sees an interval refresh, so
// every probe a test counts is one it asked for.
const NEVER: Duration = Duration::from_secs(365 * 24 * 60 * 60);

fn counting_probe(calls: Arc<Mutex<Vec<PathBuf>>>) -> SharedProbeFn {
    Arc::new(move |path: &Path| {
        calls.lock().unwrap().push(path.to_path_buf());
        if path.ends_with("unmounted") {
            return Err(DiskProbeError::Io(io::Error::from(io::ErrorKind::NotFound)));
        }
        Ok(DiskSpace {
            total_bytes: 1000,
            available_bytes: path.as_os_str().len() as u64,
        })
    })
}

// Devices for the same-filesystem tests, on directories no other test
// names.
fn device_layout(tag: &str) -> ([PathBuf; 3], PathBuf, PathBuf) {
    let base = PathBuf::from(format!("/device-layout-{tag}"));
    let data = base.join("data");
    let intermediate = base.join("intermediate");
    let complete = base.join("complete");
    let movies = base.join("movies");
    let beside = base.join("beside");
    weaver_server_disk::runtime::fs::test_devices::assign(&data, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&intermediate, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete, 2);
    weaver_server_disk::runtime::fs::test_devices::assign(&movies, 3);
    weaver_server_disk::runtime::fs::test_devices::assign(&beside, 1);
    ([data, intermediate, complete], movies, beside)
}

fn staging_choice(one_device: bool, root: &Path) -> Option<PathBuf> {
    crate::jobs::working_dir::staging_dir_on(
        one_device,
        root,
        weaver_server_base::jobs::ids::JobId(7),
    )
}

// The staging choice from the owner's devices is the one made by
// reading both devices on the spot: over one device, two devices, a
// category folder, and a destination under the complete root that is
// no configured folder. A root's device is its latest look's, with no
// look; any other is one look, and a current one is not looked at again.
#[tokio::test(start_paused = true)]
async fn one_filesystem_is_answered_as_a_look_on_the_spot_would() {
    let ([data, intermediate, complete], movies, beside) = device_layout("cached");
    let elsewhere = complete.join("elsewhere");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let storage = StorageCapacity::with_probe(
        [data.clone(), intermediate.clone(), complete.clone()],
        NEVER,
        counting_probe(calls),
    );
    for root in StorageRoot::ALL {
        storage.wait_for_probes(root, 1);
    }
    let targets = [complete.clone(), movies.clone(), beside.clone(), elsewhere];
    let base: Vec<_> = targets
        .iter()
        .map(|root| staging_choice(same_device_now(&intermediate, root), root))
        .collect();
    let looks = DISK_LOOKS.load(Ordering::Relaxed);
    let mut owned = Vec::new();
    for root in &targets {
        let one = storage.same_filesystem(&intermediate, root).await;
        owned.push(staging_choice(one, root));
    }
    assert_eq!(owned, base);
    assert_eq!(
        owned,
        vec![
            Some(complete.join(".weaver-staging").join("7")),
            Some(movies.join(".weaver-staging").join("7")),
            None,
            Some(targets[3].join(".weaver-staging").join("7")),
        ]
    );
    // The device is answered before its look reads the rest; the count
    // is taken once every look is done.
    storage.looks_returned().await;
    assert_eq!(
        DISK_LOOKS.load(Ordering::Relaxed),
        looks + 3 * 2,
        "one look for each folder that is not a root, its device and its kind; none for the roots"
    );
    for root in &targets {
        let _ = storage.same_filesystem(&intermediate, root).await;
    }
    storage.looks_returned().await;
    assert_eq!(
        DISK_LOOKS.load(Ordering::Relaxed),
        looks + 3 * 2,
        "current looks are not repeated"
    );
}

// With start-up disk work still running no root has looked, and the
// devices are read on demand at once, as a look on the spot read them:
// a job then stages on a destination on another device.
#[tokio::test(start_paused = true)]
async fn one_filesystem_is_answered_before_the_roots_look() {
    let ([data, intermediate, complete], movies, beside) = device_layout("gate");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let storage = StorageCapacity::with_probe_after(
        [data, intermediate.clone(), complete.clone()],
        NEVER,
        counting_probe(calls),
        StartupDiskGate::closed(),
    );
    assert!(!storage.same_filesystem(&intermediate, &complete).await);
    assert!(!storage.same_filesystem(&intermediate, &movies).await);
    assert!(storage.same_filesystem(&intermediate, &beside).await);
    assert_eq!(
        staging_choice(false, &complete),
        Some(complete.join(".weaver-staging").join("7"))
    );
    for root in StorageRoot::ALL {
        assert_eq!(storage.slot(root).lock().completed_probes, 0);
    }
}
