// Process-wide timing of the connection plans: how long a pin lives, how
// often its delivery is judged and a challenger is tried, and how long a
// refused or failed route is left alone.
//
// Production always runs on [`PlanTiming::PRODUCTION`]. A test harness that
// cannot afford to wait out these timers in real time may install a
// [`PlanTiming::scaled`] copy once, at process start, before any plan or pool
// exists. Counts (samples, connects, retries) and per-dial timeouts are not
// part of this: they bound work, not waiting, and stay the same under any
// scale. Nor are the two timers that bound delivery evidence: how old a pin
// may get before the next connect races again, and how long booked delivery
// stays evidence. A race discards every candidate's delivery, and gathering
// the samples a verdict needs is paced by the wire, not the clock, so
// shortening those two would leave a slow or churning pin unjudged for good
// rather than judged sooner.

use std::num::NonZeroU32;
use std::sync::OnceLock;
use std::time::Duration;

use crate::address_plan::{
    ADDRESS_REPLAN_INTERVAL, DELIVERY_EVIDENCE_AGE, DELIVERY_MIN_WIRE, DELIVERY_VERDICT_INTERVAL,
    FAILED_RACE_HOLDOFF, SHADOW_INTERVAL, SHADOW_MIN_PIN_AGE,
};
use crate::pool::{OVER_LIMIT_HOLDOFF_INITIAL, OVER_LIMIT_PROBE_WINDOW};

// Shortest any scaled duration may become, so no timer collapses to zero.
const SCALED_FLOOR: Duration = Duration::from_millis(1);

// Every time-based threshold the connection plans wait on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanTiming {
    // How old a pin may get before the next connect races again.
    pub replan_interval: Duration,
    // How long connects dial candidates one by one after a race found
    // nothing answering.
    pub failed_race_holdoff: Duration,
    // Wire time booked fetches must add up to before they count as evidence.
    pub delivery_min_wire: Duration,
    // Least time between two delivery verdicts.
    pub delivery_verdict_interval: Duration,
    // How long booked delivery stays evidence.
    pub delivery_evidence_age: Duration,
    // Youngest a pin may be when a reconnect is first pointed at a challenger.
    pub shadow_min_pin_age: Duration,
    // Least time between two reconnects pointed at a challenger.
    pub shadow_interval: Duration,
    // First pause fresh connects take after a provider reports too many
    // connections. It doubles per refused probe up to a fixed ceiling.
    pub over_limit_holdoff_initial: Duration,
    // How long an over-limit probe owns the right to ask the provider.
    pub over_limit_probe_window: Duration,
    // How long a proxy route that failed to connect is skipped.
    pub route_cooldown: Duration,
    // First cooldown of a weighted egress leg that went down. It doubles per
    // failed probe, up to ten times this.
    pub leg_cooldown_initial: Duration,
    // How long a fallback ladder leaves a rung alone after its path failed.
    pub rung_cooldown: Duration,
}

impl PlanTiming {
    // The timing every production process runs on.
    pub const PRODUCTION: PlanTiming = PlanTiming {
        replan_interval: ADDRESS_REPLAN_INTERVAL,
        failed_race_holdoff: FAILED_RACE_HOLDOFF,
        delivery_min_wire: DELIVERY_MIN_WIRE,
        delivery_verdict_interval: DELIVERY_VERDICT_INTERVAL,
        delivery_evidence_age: DELIVERY_EVIDENCE_AGE,
        shadow_min_pin_age: SHADOW_MIN_PIN_AGE,
        shadow_interval: SHADOW_INTERVAL,
        over_limit_holdoff_initial: OVER_LIMIT_HOLDOFF_INITIAL,
        over_limit_probe_window: OVER_LIMIT_PROBE_WINDOW,
        route_cooldown: weaver_tunnel::pipe::PATH_COOLDOWN,
        leg_cooldown_initial: weaver_tunnel::pipe::PATH_COOLDOWN,
        rung_cooldown: weaver_tunnel::pipe::PATH_COOLDOWN,
    };

    // [`Self::PRODUCTION`] with every waiting duration divided by `scale`,
    // none shorter than a millisecond. `replan_interval` and
    // `delivery_evidence_age` stay at production: they bound evidence, not
    // waiting (see the module notes).
    pub fn scaled(scale: NonZeroU32) -> PlanTiming {
        let s = |d: Duration| (d / scale.get()).max(SCALED_FLOOR);
        let p = Self::PRODUCTION;
        PlanTiming {
            replan_interval: p.replan_interval,
            failed_race_holdoff: s(p.failed_race_holdoff),
            delivery_min_wire: s(p.delivery_min_wire),
            delivery_verdict_interval: s(p.delivery_verdict_interval),
            delivery_evidence_age: p.delivery_evidence_age,
            shadow_min_pin_age: s(p.shadow_min_pin_age),
            shadow_interval: s(p.shadow_interval),
            over_limit_holdoff_initial: s(p.over_limit_holdoff_initial),
            over_limit_probe_window: s(p.over_limit_probe_window),
            route_cooldown: s(p.route_cooldown),
            leg_cooldown_initial: s(p.leg_cooldown_initial),
            rung_cooldown: s(p.rung_cooldown),
        }
    }
}

static TIMING: OnceLock<PlanTiming> = OnceLock::new();

// The timing this process runs on: whatever [`install`] set, or
// [`PlanTiming::PRODUCTION`] if nothing was installed before first use.
pub fn timing() -> &'static PlanTiming {
    TIMING.get_or_init(|| PlanTiming::PRODUCTION)
}

// Sets this process's timing. Must be called at process start, before any
// plan or pool exists: the first [`timing`] call fixes the timing for the
// life of the process, after which this returns the rejected value.
pub fn install(timing: PlanTiming) -> Result<(), Box<PlanTiming>> {
    install_into(&TIMING, timing)
}

fn install_into(cell: &OnceLock<PlanTiming>, timing: PlanTiming) -> Result<(), Box<PlanTiming>> {
    cell.set(timing).map_err(Box::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address_plan::{DELIVERY_MIN_SAMPLES, SHADOW_EVERY_CONNECTS, SHADOW_RETRIES};

    // The durations scaling divides: everything but the two evidence bounds.
    fn waiting_fields(t: &PlanTiming) -> [Duration; 10] {
        [
            t.failed_race_holdoff,
            t.delivery_min_wire,
            t.delivery_verdict_interval,
            t.shadow_min_pin_age,
            t.shadow_interval,
            t.over_limit_holdoff_initial,
            t.over_limit_probe_window,
            t.route_cooldown,
            t.leg_cooldown_initial,
            t.rung_cooldown,
        ]
    }

    #[test]
    fn scale_one_is_production() {
        assert_eq!(PlanTiming::scaled(NonZeroU32::MIN), PlanTiming::PRODUCTION);
    }

    #[test]
    fn scaled_divides_every_waiting_duration() {
        let scaled = PlanTiming::scaled(NonZeroU32::new(10).unwrap());
        for (production, scaled) in waiting_fields(&PlanTiming::PRODUCTION)
            .into_iter()
            .zip(waiting_fields(&scaled))
        {
            assert_eq!(scaled, production / 10);
        }
        assert_eq!(scaled.delivery_verdict_interval, Duration::from_secs(6));
        assert_eq!(scaled.shadow_interval, Duration::from_secs(3));
        assert_eq!(scaled.rung_cooldown, Duration::from_secs(3));
    }

    #[test]
    fn scaling_leaves_the_evidence_bounds_alone() {
        // A race wipes delivery and the samples a verdict needs come at the
        // wire's pace, so a pin that must be judged in production must still
        // get its full ten minutes under any scale.
        let scaled = PlanTiming::scaled(NonZeroU32::new(10).unwrap());
        assert_eq!(
            scaled.replan_interval,
            PlanTiming::PRODUCTION.replan_interval
        );
        assert_eq!(
            scaled.delivery_evidence_age,
            PlanTiming::PRODUCTION.delivery_evidence_age
        );
        assert_eq!(scaled.replan_interval, Duration::from_secs(600));
    }

    #[test]
    fn scaled_floors_at_one_millisecond() {
        let scaled = PlanTiming::scaled(NonZeroU32::MAX);
        for field in waiting_fields(&scaled) {
            assert_eq!(field, SCALED_FLOOR);
        }
    }

    #[test]
    fn scaling_leaves_counts_alone() {
        // Counts are constants outside the timing; scaling builds a value and
        // never touches them.
        let _ = PlanTiming::scaled(NonZeroU32::new(10).unwrap());
        assert_eq!(DELIVERY_MIN_SAMPLES, 16);
        assert_eq!(SHADOW_EVERY_CONNECTS, 8);
        assert_eq!(SHADOW_RETRIES, 8);
    }

    #[test]
    fn install_after_first_use_is_rejected() {
        let cell = OnceLock::new();
        let scaled = PlanTiming::scaled(NonZeroU32::new(10).unwrap());
        assert_eq!(
            *cell.get_or_init(|| PlanTiming::PRODUCTION),
            PlanTiming::PRODUCTION
        );
        assert_eq!(install_into(&cell, scaled), Err(Box::new(scaled)));
        assert_eq!(cell.get(), Some(&PlanTiming::PRODUCTION));
    }

    #[test]
    fn install_before_first_use_takes_effect_once() {
        let cell = OnceLock::new();
        let scaled = PlanTiming::scaled(NonZeroU32::new(10).unwrap());
        assert_eq!(install_into(&cell, scaled), Ok(()));
        assert_eq!(cell.get(), Some(&scaled));
        assert_eq!(
            install_into(&cell, PlanTiming::PRODUCTION),
            Err(Box::new(PlanTiming::PRODUCTION))
        );
    }

    #[test]
    fn process_timing_rejects_install_once_used() {
        // Production timing is the only value this can leave behind, so the
        // other tests in this process are unaffected whatever the order.
        let used = *timing();
        assert_eq!(
            install(PlanTiming::PRODUCTION),
            Err(Box::new(PlanTiming::PRODUCTION))
        );
        assert_eq!(*timing(), used);
    }
}
