// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Raises ROUTE_DOWN as a configured route goes from up to down. The routes'
// health tells of each one as it turns, from the interface check and from
// the hops' own dials, so an idle daemon does no work here.

use super::events::EventContext;
use super::model::{QueueEvent, ScriptEventLabel, vars};
use super::quota_events::QuotaEventDirs;
use super::runner::CompatibilityFacts;
use crate::Database;
use crate::proxies::{RouteDown, RouteHealth};

/// Queue a ROUTE_DOWN event for each route `health` tells of going down.
/// Ends once `health` is dropped.
pub fn spawn_route_events(
    health: &RouteHealth,
    db: Database,
    dirs: QuotaEventDirs,
) -> tokio::task::JoinHandle<()> {
    let mut downs = health.subscribe();
    tokio::spawn(async move {
        while let Some(down) = downs.recv().await {
            if db.queue_scripts_possible() {
                drop(db.admit_queue_script_event(route_event(&down, &dirs), false));
            }
        }
    })
}

// The event for `down`. It belongs to no job.
pub(crate) fn route_event(down: &RouteDown, dirs: &QuotaEventDirs) -> EventContext {
    let event = QueueEvent::RouteDown;
    EventContext {
        job_id: None,
        event: ScriptEventLabel::Queue(event),
        category: None,
        cwd: dirs.data.clone(),
        env: [
            ("NZBNA_EVENT", event.as_str().to_string()),
            (vars::EGRESS_ID, down.egress_id.to_string()),
            (vars::ROUTE_KIND, down.kind.as_str().to_string()),
            (vars::ROUTE_NAME, down.name.clone()),
            (vars::ROUTE_REASON, down.reason.clone()),
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
