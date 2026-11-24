// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use weaver_tunnel::pipe::DialError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegDialResult {
    Success,
    Failed,
    Blocked,
    Ignored,
}

impl LegDialResult {
    pub const ALL: [Self; 4] = [Self::Success, Self::Failed, Self::Blocked, Self::Ignored];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Ignored => "ignored",
        }
    }

    // A failure that is no evidence about the path (a capacity refusal or a
    // skipped dial) neither counts toward the leg's cooldown nor is a path
    // failure here.
    pub fn of_error(error: &DialError) -> Self {
        if !error.is_path_evidence() {
            Self::Ignored
        } else if matches!(error, DialError::Fatal(_)) {
            Self::Blocked
        } else {
            Self::Failed
        }
    }
}

#[derive(Default)]
struct EgressSlots {
    dials: [AtomicU64; 4],
    cooldowns: AtomicU64,
}

static EGRESSES: LazyLock<RwLock<HashMap<u32, Arc<EgressSlots>>>> = LazyLock::new(Default::default);

fn egress_slots(egress_id: u32) -> Arc<EgressSlots> {
    if let Some(slots) = EGRESSES
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&egress_id)
    {
        return slots.clone();
    }
    EGRESSES
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .entry(egress_id)
        .or_default()
        .clone()
}

pub(crate) fn record_leg_dial(egress_id: u32, result: LegDialResult) {
    egress_slots(egress_id).dials[result as usize].fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_leg_cooldown(egress_id: u32) {
    egress_slots(egress_id)
        .cooldowns
        .fetch_add(1, Ordering::Relaxed);
}

pub fn egress_counters(egress_id: u32) -> ([(LegDialResult, u64); 4], u64) {
    let slots = EGRESSES
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&egress_id)
        .cloned();
    let dials = LegDialResult::ALL.map(|result| {
        (
            result,
            slots
                .as_ref()
                .map_or(0, |s| s.dials[result as usize].load(Ordering::Relaxed)),
        )
    });
    let cooldowns = slots
        .as_ref()
        .map_or(0, |s| s.cooldowns.load(Ordering::Relaxed));
    (dials, cooldowns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leg_dials_count_per_egress_and_result() {
        // A private egress id keeps this test independent of any other test
        // that dials through the shared registry.
        let egress = 0xfeed_0001;
        record_leg_dial(egress, LegDialResult::Success);
        record_leg_dial(egress, LegDialResult::Success);
        record_leg_dial(egress, LegDialResult::Blocked);
        record_leg_cooldown(egress);
        let (dials, cooldowns) = egress_counters(egress);
        assert_eq!(
            dials,
            [
                (LegDialResult::Success, 2),
                (LegDialResult::Failed, 0),
                (LegDialResult::Blocked, 1),
                (LegDialResult::Ignored, 0),
            ]
        );
        assert_eq!(cooldowns, 1);
    }
}
