// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::persistence::Database;
use crate::proxies::ProxiesNetworkPersistenceQueries;
pub use weaver_server_composition::servers::service_queries::*;

// Shared serialization for server mutations and whole-generation reloads.
pub static SERVER_MUTATION_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone)]
pub struct ServersService {
    db: Database,
    config: crate::settings::SharedConfig,
    handle: crate::SchedulerHandle,
}

impl ServersService {
    pub fn new(
        db: Database,
        config: crate::settings::SharedConfig,
        handle: crate::SchedulerHandle,
    ) -> Self {
        Self { db, config, handle }
    }

    // Persist operator intent even when the provider is offline. Scheduled
    // activation deliberately performs no connection probe.
    pub async fn set_active(&self, server_id: u32, active: bool) -> Result<(), String> {
        let _guard = SERVER_MUTATION_GUARD.lock().await;
        let previous_active = self
            .config
            .read()
            .await
            .servers
            .iter()
            .find(|server| server.id == server_id)
            .ok_or_else(|| format!("server {server_id} is missing from the runtime configuration"))?
            .active;
        if previous_active == active {
            return Ok(());
        }
        let proxy_runtime = self.handle.proxy_runtime();
        let _proxy_guard = match &proxy_runtime {
            Some(runtime) => Some(runtime.mutations.lock().await),
            None => None,
        };
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.set_server_active(server_id, active))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        {
            let mut config = self.config.write().await;
            let server = config
                .servers
                .iter_mut()
                .find(|server| server.id == server_id)
                .ok_or_else(|| {
                    format!("server {server_id} is missing from the runtime configuration")
                })?;
            server.active = active;
        }
        // The network runtime re-budgets its tunnels for the server's
        // connections before the NNTP runtime opens them.
        let reloaded = match &proxy_runtime {
            Some(runtime) => runtime.reload().await,
            None => Ok(()),
        };
        let network_moved = reloaded.is_ok();
        let rebuilt = match reloaded {
            Ok(()) => crate::runtime::reload::rebuild_nntp_from_config(&self.config, &self.handle)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Err(error) => Err(error),
        };
        if let Err(error) = rebuilt {
            // Failed activations must leave persistence and runtime in agreement,
            // including when the next attempt happens after a process restart.
            if let Some(server) = self
                .config
                .write()
                .await
                .servers
                .iter_mut()
                .find(|server| server.id == server_id)
            {
                server.active = previous_active;
            }
            // A rollback restores what was saved.
            let db = self.db.clone();
            let rollback = tokio::task::spawn_blocking(move || {
                db.set_server_active(server_id, previous_active)
            })
            .await;
            if !matches!(rollback, Ok(Ok(()))) {
                return Err(format!(
                    "{error}; restoring server activation failed: {rollback:?}"
                ));
            }
            // The tunnels were re-budgeted for the change; budget them again
            // for the server as it is restored.
            if network_moved
                && let Some(runtime) = &proxy_runtime
                && let Err(reload) = runtime.reload().await
            {
                return Err(format!(
                    "{error}; re-budgeting the tunnels for the restored server failed: {reload}"
                ));
            }
            return Err(error.to_string());
        }
        Ok(())
    }

    // Save a provider's speed limit and put it in force, as an edit on the
    // Servers screen does. 0 removes the limit.
    pub async fn set_server_speed_limit(
        &self,
        server_id: u32,
        bytes_per_sec: u64,
    ) -> Result<(), String> {
        let _guard = SERVER_MUTATION_GUARD.lock().await;
        let previous = self
            .config
            .read()
            .await
            .servers
            .iter()
            .find(|server| server.id == server_id)
            .ok_or_else(|| format!("server {server_id} is missing from the runtime configuration"))?
            .max_download_speed;
        if previous == bytes_per_sec {
            return Ok(());
        }
        let proxy_runtime = self.handle.proxy_runtime();
        let _proxy_guard = match &proxy_runtime {
            Some(runtime) => Some(runtime.mutations.lock().await),
            None => None,
        };
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.set_server_max_download_speed(server_id, bytes_per_sec)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        let set = |speed| {
            let config = self.config.clone();
            async move {
                if let Some(server) = config
                    .write()
                    .await
                    .servers
                    .iter_mut()
                    .find(|server| server.id == server_id)
                {
                    server.max_download_speed = speed;
                }
            }
        };
        set(bytes_per_sec).await;
        if let Err(error) =
            crate::runtime::reload::rebuild_nntp_from_config(&self.config, &self.handle).await
        {
            set(previous).await;
            let db = self.db.clone();
            let rollback = tokio::task::spawn_blocking(move || {
                db.set_server_max_download_speed(server_id, previous)
            })
            .await;
            if !matches!(rollback, Ok(Ok(()))) {
                return Err(format!(
                    "{error}; restoring the server's speed limit failed: {rollback:?}"
                ));
            }
            return Err(error.to_string());
        }
        Ok(())
    }

    // Save an egress's download limit and put it in force, as an edit on the
    // Networking screen does. `None` removes the limit.
    pub async fn set_egress_speed_limit(
        &self,
        egress_id: u32,
        bytes_per_sec: Option<u64>,
    ) -> Result<(), String> {
        let runtime = self
            .handle
            .proxy_runtime()
            .ok_or("networking is not available")?;
        let _guard = runtime.mutations.lock().await;
        let db = self.db.clone();
        let previous = tokio::task::spawn_blocking(move || {
            let Some(mut egress) = db
                .list_egress_interfaces()?
                .into_iter()
                .find(|egress| egress.id == egress_id)
            else {
                // Deleted since the rule was loaded; the next load drops it
                // from the rule.
                tracing::warn!(egress_id, "a speed rule names an egress that is gone");
                return Ok(None);
            };
            let previous = egress.download_bps;
            if previous == bytes_per_sec {
                return Ok(None);
            }
            egress.download_bps = bytes_per_sec;
            db.update_egress_interface(&egress)?;
            Ok::<_, StateError>(Some(previous))
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        let Some(previous) = previous else {
            return Ok(());
        };
        if let Err(error) = runtime.reload().await {
            // Put the saved limit back, so the retry sees a change to make
            // and reloads again.
            let db = self.db.clone();
            let rollback = tokio::task::spawn_blocking(move || {
                let mut egress = db
                    .list_egress_interfaces()?
                    .into_iter()
                    .find(|egress| egress.id == egress_id)
                    .ok_or_else(|| StateError::Database(format!("egress {egress_id} not found")))?;
                egress.download_bps = previous;
                db.update_egress_interface(&egress).map(|_| ())
            })
            .await;
            if !matches!(rollback, Ok(Ok(()))) {
                return Err(format!(
                    "{error}; restoring the egress's speed limit failed: {rollback:?}"
                ));
            }
            return Err(error);
        }
        Ok(())
    }
}
