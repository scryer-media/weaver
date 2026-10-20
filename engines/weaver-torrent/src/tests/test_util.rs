// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    io::Write,
    path::Path,
    sync::{Arc, Weak},
    time::Duration,
};

use crate::core::{Id20, peer_id::generate_azereus_style};
use crate::crate_version;
use anyhow::bail;
use parking_lot::RwLock;
use rand::{Rng, RngExt, SeedableRng, rng};
use tempfile::TempDir;
use tracing::{info, trace};

pub fn setup_test_logging() {
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "info") };
    }
    let _ = tracing_subscriber::fmt::try_init();
}

pub fn create_new_file_with_random_content(path: &Path, mut size: usize) {
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();

    trace!(?path, "creating temp file");

    const BUF_SIZE: usize = 8192 * 16;
    let mut rng = rand::rngs::SmallRng::seed_from_u64(rand::random());
    let mut write_buf = [0; BUF_SIZE];
    while size > 0 {
        rng.fill_bytes(&mut write_buf[..]);
        let written = file.write(&write_buf[..size.min(BUF_SIZE)]).unwrap();
        size -= written;
    }
}

pub fn create_default_random_dir_with_torrents(
    num_files: usize,
    file_size: usize,
    tempdir_prefix: Option<&str>,
) -> TempDir {
    let dir = TempDir::with_prefix(tempdir_prefix.unwrap_or("rqbit_test")).unwrap();
    info!(path=?dir.path(), "created tempdir");
    for f in 0..num_files {
        create_new_file_with_random_content(&dir.path().join(format!("{f}.data")), file_size);
    }
    dir
}

#[derive(Debug)]
pub struct TestPeerMetadata {
    pub server_id: u8,
    pub max_random_sleep_ms: u8,
}

impl TestPeerMetadata {
    pub fn good() -> Self {
        Self {
            server_id: 0,
            max_random_sleep_ms: 0,
        }
    }

    pub fn as_peer_id(&self) -> Id20 {
        let mut peer_id = generate_azereus_style(*b"WV", crate_version!());
        peer_id.0[15..19].copy_from_slice(b"test");
        rng().fill(&mut peer_id.0);
        peer_id.0[14] = self.server_id;
        peer_id.0[13] = self.max_random_sleep_ms;
        peer_id
    }

    pub fn from_peer_id(peer_id: Id20) -> Self {
        if &peer_id.0[15..19] != b"test" {
            return Self::good();
        }
        Self {
            server_id: peer_id.0[14],
            max_random_sleep_ms: peer_id.0[13],
        }
    }

    pub fn disconnect_probability(&self) -> f64 {
        if self.server_id % 2 == 1 {
            return 0.05f64;
        }
        0f64
    }

    pub fn bad_data_probability(&self) -> f64 {
        if self.server_id % 2 == 1 {
            return 0.05f64;
        }
        0f64
    }
}

async fn debug_server() -> anyhow::Result<()> {
    Ok(())
}

// Lift the open-file soft limit toward the hard limit, so a test that opens
// many files is not cut short by an inherited soft limit of 256. Returns the
// limit now in force.
pub fn raise_nofile_limit() -> u64 {
    #[cfg(unix)]
    {
        use nix::sys::resource::{Resource, getrlimit, setrlimit};
        let Ok((soft, hard)) = getrlimit(Resource::RLIMIT_NOFILE) else {
            return 0;
        };
        for target in [1024 * 1024, 65_536, 16_384, 4_096, 1_024] {
            let wanted = hard.min(target);
            if wanted > soft && setrlimit(Resource::RLIMIT_NOFILE, wanted, hard).is_ok() {
                return wanted;
            }
        }
        soft
    }
    #[cfg(not(unix))]
    {
        0
    }
}

#[allow(dead_code)]
pub fn spawn_debug_server() -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(debug_server())
}

pub trait DropPlaceholder: Send + Sync {}
impl<T: Send + Sync> DropPlaceholder for T {}

struct DropCheck {
    obj: Weak<dyn DropPlaceholder>,
    name: String,
}

#[derive(Default, Clone)]
pub struct DropChecks(Arc<RwLock<Vec<DropCheck>>>);

impl DropChecks {
    pub fn add<T: DropPlaceholder + 'static, S: Into<String>>(&self, obj: &Arc<T>, name: S) {
        let weak = Arc::downgrade(obj);
        self.0.write().push(DropCheck {
            obj: weak as Weak<dyn DropPlaceholder>,
            name: name.into(),
        })
    }

    pub fn check(&self) -> anyhow::Result<()> {
        let mut still_running = Vec::new();
        for dc in self.0.read().iter() {
            if dc.obj.upgrade().is_some() {
                still_running.push(dc.name.clone())
            }
        }
        if !still_running.is_empty() {
            anyhow::bail!(
                "still existing objects that were supposed to be dropped: {still_running:#?}"
            )
        }
        Ok(())
    }
}

pub async fn wait_until(
    mut cond: impl FnMut() -> anyhow::Result<()>,
    timeout: Duration,
) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    let mut last_err: Option<anyhow::Error> = None;
    let res = tokio::time::timeout(timeout, async {
        loop {
            interval.tick().await;
            match cond() {
                Ok(()) => return Ok::<_, anyhow::Error>(()),
                Err(e) => last_err = Some(e),
            }
        }
    })
    .await;
    if res.is_err() {
        bail!("wait_until timeout: last result = {last_err:?}")
    }
    Ok(())
}

pub async fn wait_until_i_am_the_last_task() -> anyhow::Result<()> {
    let metrics = tokio::runtime::Handle::current().metrics();
    wait_until(
        || {
            let num_alive = metrics.num_alive_tasks();
            if num_alive != 0 {
                bail!("metrics.num_alive_tasks() = {num_alive}, expected 0")
            }
            Ok(())
        },
        // This needs to be higher than the timeout the tasks print "still running"
        Duration::from_secs(15),
    )
    .await
}
