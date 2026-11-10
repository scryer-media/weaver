// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Raises ROUTE_DOWN as a configured route goes down, and ROUTE_UP as one
// told down comes back. The routes' health tells of each one as it turns,
// from the interface check and from the hops' own dials, so an idle daemon
// does no work here beyond waking for a down held by the quiet period.

use std::sync::Arc;

use super::events::EventContext;
use super::model::{QueueEvent, ScriptEventLabel, vars};
use super::quota_events::QuotaEventDirs;
use super::runner::CompatibilityFacts;
use crate::Database;
use crate::proxies::{RouteChange, RouteEvent, RouteHealth};

/// Queue a ROUTE_DOWN or ROUTE_UP event for each route `health` tells of,
/// and tell the downs that waited out the quiet period when it ends. Ends
/// once `health` is dropped.
pub fn spawn_route_events(
    health: &Arc<RouteHealth>,
    db: Database,
    dirs: QuotaEventDirs,
) -> tokio::task::JoinHandle<()> {
    follow_routes(health, move |event| {
        if db.queue_scripts_possible() {
            drop(db.admit_queue_script_event(route_event(event, &dirs), false));
        }
    })
}

// Hand `raise` each route `health` tells of, and tell the downs that waited
// out the quiet period when it ends. Ends once `health` is dropped.
pub(crate) fn follow_routes(
    health: &Arc<RouteHealth>,
    mut raise: impl FnMut(&RouteEvent) + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    let mut events = health.subscribe();
    let rearmed = health.rearmed();
    let health = Arc::downgrade(health);
    tokio::spawn(async move {
        loop {
            let due = match health.upgrade() {
                Some(health) => health.due(),
                None => return,
            };
            let wait = async {
                match due {
                    Some(due) => tokio::time::sleep_until(due.into()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                event = events.recv() => {
                    let Some(event) = event else {
                        return;
                    };
                    raise(&event);
                }
                () = rearmed.notified() => {}
                () = wait => {
                    if let Some(health) = health.upgrade() {
                        health.settle();
                    }
                }
            }
        }
    })
}

// The event for `route`. It belongs to no job, and only a down has a reason.
pub(crate) fn route_event(route: &RouteEvent, dirs: &QuotaEventDirs) -> EventContext {
    let event = match route.change {
        RouteChange::Down => QueueEvent::RouteDown,
        RouteChange::Up => QueueEvent::RouteUp,
    };
    let reason =
        (route.change == RouteChange::Down).then(|| (vars::ROUTE_REASON, route.reason.clone()));
    EventContext {
        job_id: None,
        event: ScriptEventLabel::Queue(event),
        category: None,
        cwd: dirs.data.clone(),
        env: [
            ("NZBNA_EVENT", event.as_str().to_string()),
            (vars::EGRESS_ID, route.egress_id.to_string()),
            (vars::ROUTE_KIND, route.kind.as_str().to_string()),
            (vars::ROUTE_NAME, route.name.clone()),
        ]
        .into_iter()
        .chain(reason)
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
