// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Raises QUOTA_NORMAL, QUOTA_TRICKLE and QUOTA_STOPPED as an egress's quota
// comes to a new state in either direction. It wakes only when the policy
// registry rings: a saved egress, an upload crossing a cap, a download
// quota starting or stopping to turn work away, a reset or a new period.
// Each ring reads the egresses' states from memory and compares them with
// the last ones seen, so an idle daemon does no work here.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use super::events::EventContext;
use super::model::{QueueEvent, ScriptEventLabel, vars};
use super::runner::CompatibilityFacts;
use crate::Database;
use crate::bandwidth::QuotaState;
use crate::servers::transfer_policy::{EgressQuotaStates, ServerTransferPolicyRegistry};

/// The folders a quota event's script is told of; it runs in `data`.
#[derive(Debug, Clone, Default)]
pub struct QuotaEventDirs {
    pub data: PathBuf,
    pub intermediate: PathBuf,
    pub complete: PathBuf,
}

/// Follow `policy` and queue a quota event each time an egress's state
/// changes in a direction. The states at start are where it begins: none
/// is raised for them, nor for an egress the first time it is seen. Ends
/// once the registry is dropped.
pub fn spawn_quota_events(
    policy: &Arc<ServerTransferPolicyRegistry>,
    db: Database,
    dirs: QuotaEventDirs,
) -> tokio::task::JoinHandle<()> {
    let mut changes = policy.subscribe_changes();
    let mut capacity = policy.subscribe_capacity_changes();
    let mut quota = policy.subscribe_quota_changes();
    let mut seen = states(policy);
    let policy = Arc::downgrade(policy);
    tokio::spawn(async move {
        loop {
            let rang = tokio::select! {
                changed = changes.changed() => changed.is_ok(),
                changed = capacity.changed() => changed.is_ok(),
                changed = quota.changed() => changed.is_ok(),
            };
            let Some(policy) = policy.upgrade().filter(|_| rang) else {
                break;
            };
            let now = states(&policy);
            drop(policy);
            for (egress_id, after) in &now {
                let Some(before) = seen.get(egress_id) else {
                    continue;
                };
                for (direction, before, after) in [
                    ("download", before.download, after.download),
                    ("upload", before.upload, after.upload),
                ] {
                    if before != after && db.queue_scripts_possible() {
                        drop(db.admit_queue_script_event(
                            quota_event(*egress_id, direction, after, &dirs),
                            false,
                        ));
                    }
                }
            }
            seen = now;
        }
    })
}

fn states(policy: &ServerTransferPolicyRegistry) -> HashMap<u32, EgressQuotaStates> {
    policy
        .egress_ids()
        .into_iter()
        .filter_map(|id| Some((id, policy.egress_quota_states(id)?)))
        .collect()
}

// The event for `egress_id`'s quota coming to `state` in `direction`. It
// belongs to no job.
pub(crate) fn quota_event(
    egress_id: u32,
    direction: &str,
    state: QuotaState,
    dirs: &QuotaEventDirs,
) -> EventContext {
    let event = QueueEvent::quota(state);
    EventContext {
        job_id: None,
        event: ScriptEventLabel::Queue(event),
        category: None,
        cwd: dirs.data.clone(),
        env: [
            ("NZBNA_EVENT", event.as_str().to_string()),
            (vars::EGRESS_ID, egress_id.to_string()),
            (vars::QUOTA_DIRECTION, direction.to_string()),
            (vars::QUOTA_STATE, state.as_str().to_string()),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect(),
        facts: CompatibilityFacts {
            data_dir: Some(dirs.data.clone()),
            intermediate_dir: Some(dirs.intermediate.clone()),
            complete_dir: Some(dirs.complete.clone()),
            ..Default::default()
        },
        instances: None,
        scratch: None,
    }
}
