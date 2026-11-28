// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::model::{TorrentCounters, TorrentTracker, TrackerEntry, TrackerStatus};
use std::collections::HashMap;
use weaver_server_net::proxies::ResolvedBy;
use weaver_server_settings::settings::model::{Seeding, SeedingGoalMode};
use weaver_torrent::{AnnounceErrorKind, NamedBy, TrackerAnnounce};

/// What a torrent, or anything asked of one, is refused with while torrents
/// are turned off.
pub const TORRENTS_OFF: &str = "torrents are off";

/// Uploaded over downloaded; over the bytes the torrent holds while less
/// than 1% of them were downloaded here, as for a torrent added complete
/// (qBittorrent `TorrentImpl::realRatio`, over libtorrent's `total_done`).
/// Never over the torrent's size: one file taken from a large torrent owes
/// a ratio on that file.
pub fn ratio(counters: &TorrentCounters, held: u64) -> f64 {
    let over = ratio_base(counters.downloaded_bytes, held);
    if over == 0 {
        0.0
    } else {
        counters.uploaded_bytes as f64 / over as f64
    }
}

/// The bytes a ratio is taken over: those downloaded, or those the torrent
/// holds while less than 1% of them were downloaded here.
pub fn ratio_base(downloaded_bytes: u64, held: u64) -> u64 {
    if downloaded_bytes.saturating_mul(100) < held {
        held
    } else {
        downloaded_bytes
    }
}

/// Which seeding goal a [`GoalProgress`] measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GoalKind {
    Ratio,
    SeedTime,
    InactiveTime,
}

/// One configured seeding goal and how far the torrent is toward it: a
/// ratio as a ratio, the times in whole minutes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GoalProgress {
    pub kind: GoalKind,
    pub met: bool,
    pub current: f64,
    pub target: f64,
}

/// The goals the policy sets, in the order ratio, seed time, inactive time;
/// empty when it sets none. The inactive time is 0 while there has been no
/// activity, and that goal is not met then.
pub fn goals(
    policy: &Seeding,
    counters: &TorrentCounters,
    held_bytes: u64,
    now_ms: i64,
) -> Vec<GoalProgress> {
    let mut goals = Vec::with_capacity(3);
    if let Some(goal) = policy.ratio_goal {
        let ratio = ratio(counters, held_bytes);
        goals.push(GoalProgress {
            kind: GoalKind::Ratio,
            met: ratio >= goal,
            current: ratio,
            target: goal,
        });
    }
    if let Some(minutes) = policy.seed_time_goal_minutes {
        let seeded = policy.seeded_seconds(counters) / 60;
        goals.push(GoalProgress {
            kind: GoalKind::SeedTime,
            met: seeded >= u64::from(minutes),
            current: seeded as f64,
            target: f64::from(minutes),
        });
    }
    if let Some(minutes) = policy.inactive_goal_minutes {
        let idle = counters
            .last_activity_ms
            .map(|at| (now_ms - at).max(0) as u64 / 60_000);
        goals.push(GoalProgress {
            kind: GoalKind::InactiveTime,
            met: idle.is_some_and(|idle| idle >= u64::from(minutes)),
            current: idle.unwrap_or(0) as f64,
            target: f64::from(minutes),
        });
    }
    goals
}

/// Whether `goals` are met by the policy's mode: any of them, or all of
/// them. Terms with no goal are never met.
pub fn goals_met(policy: &Seeding, goals: &[GoalProgress]) -> bool {
    if goals.is_empty() {
        return false;
    }
    match policy.mode {
        SeedingGoalMode::Any => goals.iter().any(|goal| goal.met),
        SeedingGoalMode::All => goals.iter().all(|goal| goal.met),
    }
}

/// The goal still to meet, named, or `None` once it is met. Terms with no
/// goal are never met.
pub fn goal_left(
    policy: &Seeding,
    counters: &TorrentCounters,
    held_bytes: u64,
    now_ms: i64,
) -> Option<String> {
    let goals = goals(policy, counters, held_bytes, now_ms);
    if goals.is_empty() {
        return Some("no seeding goal".to_owned());
    }
    (!goals_met(policy, &goals)).then(|| {
        goals
            .iter()
            .filter(|goal| !goal.met)
            .map(|goal| {
                let (current, target) = (goal.current, goal.target);
                match goal.kind {
                    GoalKind::Ratio => format!("ratio {current:.2} of {target:.2}"),
                    GoalKind::SeedTime => {
                        format!("seeded {} of {} minutes", current as u64, target as u32)
                    }
                    GoalKind::InactiveTime => {
                        format!("inactive {} of {} minutes", current as u64, target as u32)
                    }
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    })
}

/// Seconds left at the download speed, while there is a speed.
pub fn eta_seconds(left_bytes: u64, bytes_per_second: u64) -> Option<u64> {
    (bytes_per_second > 0 && left_bytes > 0).then(|| left_bytes.div_ceil(bytes_per_second))
}

/// The seconds a torrent has been active: the whole seconds its counter
/// holds and the part of a second run past them, a second begun counting
/// whole. Its averages run over these same seconds, so its first second
/// reads every byte moved in it as its rate.
pub fn active_seconds(whole_seconds: u64, part_ms: u64) -> u64 {
    whole_seconds.saturating_add(part_ms.div_ceil(1000))
}

/// Bytes over the seconds active; 0 before any time is active.
pub fn average_bps(bytes: u64, active_seconds: u64) -> u64 {
    bytes.checked_div(active_seconds).unwrap_or(0)
}

/// A tracker as the views show it, from how announcing to it last went.
///
/// The announce loop answers every accepted announce by announcing again
/// once the interval has passed, and retries a failed one without end, so
/// while the run announces to the tracker the latest outcome stands until
/// the next one replaces it: an accepted announce reads as working however
/// long ago it was. A tracker the run no longer announces to, nothing due
/// and nothing in flight, keeps its last outcome, with no floor for a next
/// announce that is not coming. It reads as the torrent's own until the
/// torrent's list says who named it.
pub fn tracker_view(tier: u32, announce: &TrackerAnnounce) -> TorrentTracker {
    let ms = |at: std::time::SystemTime| {
        at.duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|since| i64::try_from(since.as_millis()).ok())
    };
    let last = announce.last.as_ref();
    let accepted = last.and_then(|last| last.result.as_ref().ok());
    let announcing = announce.in_flight || announce.next_announce.is_some();
    TorrentTracker {
        tier,
        url: announce.url.clone(),
        named_by: NamedBy::Torrent,
        // The state is read off the last outcome; an announce in flight is
        // told apart by `updating`.
        status: match last.map(|last| last.result.as_ref()) {
            None => TrackerStatus::NotContacted,
            Some(Ok(_)) => TrackerStatus::Working,
            Some(Err(error)) => match error.kind {
                AnnounceErrorKind::Refused => TrackerStatus::TrackerError,
                AnnounceErrorKind::Skipped => TrackerStatus::Unreachable,
                AnnounceErrorKind::Failed => TrackerStatus::NotWorking,
            },
        },
        updating: announce.in_flight,
        last_announce_ms: last.and_then(|last| ms(last.at)),
        next_announce_ms: announce.next_announce.and_then(ms),
        min_announce_ms: last
            .zip(accepted)
            .filter(|_| announcing)
            .and_then(|(last, accepted)| ms(last.at + accepted.min_interval)),
        // libtorrent `announce_entry::message`, the tracker's warning, else
        // the error (qBittorrent shows one, then the other).
        message: last.and_then(|last| match &last.result {
            Ok(accepted) => accepted.warning.clone(),
            Err(error) => Some(error.message.clone()),
        }),
        seeders: accepted.and_then(|accepted| accepted.seeders),
        leechers: accepted.and_then(|accepted| accepted.leechers),
        downloaded: accepted.and_then(|accepted| accepted.downloaded),
        peers: accepted.map(|accepted| u32::try_from(accepted.peers.len()).unwrap_or(u32::MAX)),
        fails: u32::from(announce.fails),
        resolved_by: None,
    }
}

/// The swarm as the trackers last counted it, and when the next announce is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swarm {
    /// The most seeds any tracker counts: trackers count the same swarm, so
    /// their counts overlap rather than add up.
    pub seeds: Option<u32>,
    pub leechers: Option<u32>,
    /// Seconds until the soonest tracker announce.
    pub reannounce_seconds: Option<u64>,
    /// When the soonest tracker announce is, in milliseconds since the
    /// epoch: what `reannounce_seconds` counts down to.
    pub next_announce_ms: Option<i64>,
}

/// The swarm read off how announcing to each tracker went, at `now_ms`.
pub fn swarm(announces: &[TrackerAnnounce], now_ms: i64) -> Swarm {
    let trackers: Vec<TorrentTracker> = announces
        .iter()
        .map(|announce| tracker_view(0, announce))
        .collect();
    let next_announce_ms = trackers
        .iter()
        .filter_map(|tracker| tracker.next_announce_ms)
        .min();
    Swarm {
        seeds: trackers.iter().filter_map(|tracker| tracker.seeders).max(),
        leechers: trackers.iter().filter_map(|tracker| tracker.leechers).max(),
        reannounce_seconds: next_announce_ms.map(|next| reannounce_seconds(next, now_ms)),
        next_announce_ms,
    }
}

/// Whole seconds from `now_ms` to an announce due at `next_ms`, rounded up;
/// one already due is due now.
pub fn reannounce_seconds(next_ms: i64, now_ms: i64) -> u64 {
    ((next_ms - now_ms).max(0) as u64).div_ceil(1000)
}

/// A torrent's trackers: the ones its run announces to, each in the tier
/// the torrent lists it in and as whoever named it there, or, before it has
/// run, the ones it lists. A tracker the torrent does not list came with its
/// magnet, whose trackers each take a tier of their own in the link's order.
/// Each says who resolves its host on a route whose names `names` resolves.
pub fn trackers(
    listed: Vec<TrackerEntry>,
    announces: &[TrackerAnnounce],
    names: Option<ResolvedBy>,
) -> Vec<TorrentTracker> {
    let mut trackers: Vec<TorrentTracker> = if announces.is_empty() {
        listed
            .into_iter()
            .map(|entry| TorrentTracker {
                tier: entry.tier,
                url: entry.url,
                named_by: entry.named_by,
                status: TrackerStatus::NotContacted,
                updating: false,
                last_announce_ms: None,
                next_announce_ms: None,
                min_announce_ms: None,
                message: None,
                seeders: None,
                leechers: None,
                downloaded: None,
                peers: None,
                fails: 0,
                resolved_by: None,
            })
            .collect()
    } else {
        // The run names a tracker by its parsed URL. A URL listed twice is
        // in the tier, and as whoever, it was first listed in.
        let mut tiers: HashMap<String, (u32, NamedBy)> = HashMap::with_capacity(listed.len());
        for entry in listed {
            if let Ok(url) = weaver_torrent::Url::parse(&entry.url) {
                tiers
                    .entry(url.into())
                    .or_insert((entry.tier, entry.named_by));
            }
        }
        announces
            .iter()
            .map(|announce| {
                let (tier, named_by) = tiers
                    .get(announce.url.as_str())
                    .copied()
                    .unwrap_or((0, NamedBy::Torrent));
                TorrentTracker {
                    named_by,
                    ..tracker_view(tier, announce)
                }
            })
            .collect()
    };
    trackers.sort_by(|a, b| (a.tier, &a.url).cmp(&(b.tier, &b.url)));
    for tracker in &mut trackers {
        tracker.resolved_by = resolved_by(&tracker.url, names);
    }
    trackers
}

/// Who resolves the host `url` names, on a route whose names `names`
/// resolves: no one for an address, which is dialled as it stands.
pub fn resolved_by(url: &str, names: Option<ResolvedBy>) -> Option<ResolvedBy> {
    let host = weaver_torrent::Url::parse(url).ok()?.host_str()?.to_owned();
    let address = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>();
    match address {
        Ok(_) => Some(ResolvedBy::Literal),
        Err(_) => names,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};
    use weaver_server_settings::settings::model::{SeedTimeCounting, SeedingAction};
    use weaver_torrent::{AnnounceError, AnnounceOk, LastAnnounce};

    // The seconds active and the average over them come from one
    // quantity: the bytes over the seconds is the average, a second begun
    // counts whole, and the first second's bytes are its rate.
    #[test]
    fn the_average_is_the_bytes_over_the_seconds_active() {
        for (whole, part_ms, bytes, seconds, average) in [
            (0, 0, 0, 0, 0),
            (0, 1, 16_384, 1, 16_384),
            (0, 500, 16_384, 1, 16_384),
            (1, 0, 16_384, 1, 16_384),
            (4, 0, 262_144, 4, 65_536),
            (4, 943, 262_144, 5, 52_428),
            (10, 999, 1_000_000, 11, 90_909),
        ] {
            let active = active_seconds(whole, part_ms);
            let bps = average_bps(bytes, active);
            assert_eq!((active, bps), (seconds, average), "{whole} s {part_ms} ms");
            if let Some(per) = bytes.checked_div(active) {
                assert_eq!(per, bps);
            }
        }
    }

    fn policy(ratio: Option<f64>, seed: Option<u32>, mode: SeedingGoalMode) -> Seeding {
        Seeding {
            seed_time_counting: Default::default(),
            ratio_goal: ratio,
            seed_time_goal_minutes: seed,
            inactive_goal_minutes: None,
            mode,
            action: SeedingAction::Stop,
        }
    }

    #[test]
    fn no_goal_is_never_met_and_a_set_goal_is_met_by_its_mode() {
        let counters = TorrentCounters {
            uploaded_bytes: 150,
            finished_duration_s: 30 * 60,
            ..TorrentCounters::default()
        };
        let none = policy(None, None, SeedingGoalMode::Any);
        assert!(goal_left(&none, &counters, 100, 0).is_some());
        let any = policy(Some(1.0), Some(60), SeedingGoalMode::Any);
        assert_eq!(goal_left(&any, &counters, 100, 0), None);
        let all = policy(Some(1.0), Some(60), SeedingGoalMode::All);
        assert_eq!(
            goal_left(&all, &counters, 100, 0).as_deref(),
            Some("seeded 30 of 60 minutes")
        );
        let inactive = Seeding {
            inactive_goal_minutes: Some(10),
            ..none
        };
        let idle = TorrentCounters {
            last_activity_ms: Some(0),
            ..counters
        };
        assert!(goal_left(&inactive, &idle, 100, 9 * 60_000).is_some());
        assert_eq!(goal_left(&inactive, &idle, 100, 10 * 60_000), None);
    }

    fn terms(
        ratio: Option<f64>,
        seed: Option<u32>,
        inactive: Option<u32>,
        mode: SeedingGoalMode,
    ) -> Seeding {
        Seeding {
            inactive_goal_minutes: inactive,
            ..policy(ratio, seed, mode)
        }
    }

    // The text names each goal left, and the structured goals say the same:
    // ratio 1.50 of 2.00, seeded 30 of 60 minutes, inactive 4 of 10 minutes.
    #[test]
    fn the_goal_left_text_is_pinned_for_each_goal_and_mode() {
        use SeedingGoalMode::{All, Any};
        let counters = TorrentCounters {
            uploaded_bytes: 150,
            downloaded_bytes: 100,
            finished_duration_s: 30 * 60 + 59,
            last_activity_ms: Some(0),
            ..TorrentCounters::default()
        };
        let now = 4 * 60_000 + 59_999;
        let quiet = TorrentCounters {
            last_activity_ms: None,
            ..counters
        };
        let cases: [(Seeding, &TorrentCounters, Option<&str>); 16] = [
            (
                terms(None, None, None, Any),
                &counters,
                Some("no seeding goal"),
            ),
            (
                terms(None, None, None, All),
                &counters,
                Some("no seeding goal"),
            ),
            (terms(Some(1.5), None, None, Any), &counters, None),
            (
                terms(Some(2.0), None, None, Any),
                &counters,
                Some("ratio 1.50 of 2.00"),
            ),
            (terms(None, Some(30), None, Any), &counters, None),
            (
                terms(None, Some(60), None, Any),
                &counters,
                Some("seeded 30 of 60 minutes"),
            ),
            (terms(None, None, Some(4), Any), &counters, None),
            (
                terms(None, None, Some(10), Any),
                &counters,
                Some("inactive 4 of 10 minutes"),
            ),
            // No activity yet: no time inactive, and the goal is not met.
            (
                terms(None, None, Some(0), Any),
                &quiet,
                Some("inactive 0 of 0 minutes"),
            ),
            (terms(Some(1.0), Some(60), Some(10), Any), &counters, None),
            (
                terms(Some(2.0), Some(60), Some(10), Any),
                &counters,
                Some("ratio 1.50 of 2.00, seeded 30 of 60 minutes, inactive 4 of 10 minutes"),
            ),
            (
                terms(Some(1.0), Some(60), Some(10), All),
                &counters,
                Some("seeded 30 of 60 minutes, inactive 4 of 10 minutes"),
            ),
            (
                terms(Some(2.0), Some(30), Some(4), All),
                &counters,
                Some("ratio 1.50 of 2.00"),
            ),
            (terms(Some(1.0), Some(30), Some(4), All), &counters, None),
            (
                terms(Some(0.333), None, Some(10), All),
                &counters,
                Some("inactive 4 of 10 minutes"),
            ),
            (
                terms(Some(2.0), Some(60), None, All),
                &counters,
                Some("ratio 1.50 of 2.00, seeded 30 of 60 minutes"),
            ),
        ];
        for (index, (terms, counters, text)) in cases.iter().enumerate() {
            let left = goal_left(terms, counters, 0, now);
            assert_eq!(left.as_deref(), *text, "case {index}");
            let goals = goals(terms, counters, 0, now);
            assert_eq!(goals_met(terms, &goals), text.is_none(), "case {index}");
        }
    }

    #[test]
    fn the_goals_are_the_configured_ones_in_order() {
        let counters = TorrentCounters {
            uploaded_bytes: 150,
            downloaded_bytes: 100,
            finished_duration_s: 30 * 60,
            ..TorrentCounters::default()
        };
        let none = terms(None, None, None, SeedingGoalMode::Any);
        assert!(goals(&none, &counters, 0, 0).is_empty());
        assert!(!goals_met(&none, &[]));
        let all = terms(Some(1.0), Some(60), Some(10), SeedingGoalMode::All);
        assert_eq!(
            goals(&all, &counters, 0, 0),
            [
                GoalProgress {
                    kind: GoalKind::Ratio,
                    met: true,
                    current: 1.5,
                    target: 1.0,
                },
                GoalProgress {
                    kind: GoalKind::SeedTime,
                    met: false,
                    current: 30.0,
                    target: 60.0,
                },
                GoalProgress {
                    kind: GoalKind::InactiveTime,
                    met: false,
                    current: 0.0,
                    target: 10.0,
                },
            ]
        );
        let seed_only = terms(None, Some(60), None, SeedingGoalMode::Any);
        let kinds: Vec<GoalKind> = goals(&seed_only, &counters, 0, 0)
            .iter()
            .map(|goal| goal.kind)
            .collect();
        assert_eq!(kinds, [GoalKind::SeedTime]);
    }

    // The text as it was built before the structured goals, kept to show
    // the rewrite reads the same for every term, counter and mode.
    fn goal_left_before(
        policy: &Seeding,
        counters: &TorrentCounters,
        held_bytes: u64,
        now_ms: i64,
    ) -> Option<String> {
        let mut goals: Vec<(bool, String)> = Vec::new();
        if let Some(goal) = policy.ratio_goal {
            let ratio = ratio(counters, held_bytes);
            goals.push((ratio >= goal, format!("ratio {ratio:.2} of {goal:.2}")));
        }
        if let Some(minutes) = policy.seed_time_goal_minutes {
            let seeded = policy.seeded_seconds(counters) / 60;
            goals.push((
                seeded >= u64::from(minutes),
                format!("seeded {seeded} of {minutes} minutes"),
            ));
        }
        if let Some(minutes) = policy.inactive_goal_minutes {
            let idle = counters
                .last_activity_ms
                .map(|at| (now_ms - at).max(0) as u64 / 60_000);
            goals.push((
                idle.is_some_and(|idle| idle >= u64::from(minutes)),
                format!("inactive {} of {minutes} minutes", idle.unwrap_or(0)),
            ));
        }
        if goals.is_empty() {
            return Some("no seeding goal".to_owned());
        }
        let met = match policy.mode {
            SeedingGoalMode::Any => goals.iter().any(|(met, _)| *met),
            SeedingGoalMode::All => goals.iter().all(|(met, _)| *met),
        };
        (!met).then(|| {
            goals
                .into_iter()
                .filter(|(met, _)| !met)
                .map(|(_, goal)| goal)
                .collect::<Vec<_>>()
                .join(", ")
        })
    }

    #[test]
    fn the_goal_left_text_reads_as_it_did_before_the_structured_goals() {
        let ratios = [
            None,
            Some(0.0),
            Some(0.5),
            Some(1.0),
            Some(1.005),
            Some(2.0),
        ];
        let minutes = [None, Some(0), Some(1), Some(30), Some(60), Some(u32::MAX)];
        let uploads = [0u64, 50, 100, 150, 100_501];
        // Up to a century of seeding, and activity before, at and after now.
        let seconds = [0u64, 59, 60, 30 * 60, 60 * 60 + 1, 100 * 365 * 86_400];
        let activity = [None, Some(0i64), Some(60_000), Some(700_000)];
        let nows = [0i64, 59_999, 600_000, 1 << 40];
        for ratio_goal in ratios {
            for seed in minutes {
                for inactive in minutes {
                    for mode in [SeedingGoalMode::Any, SeedingGoalMode::All] {
                        for counting in
                            [SeedTimeCounting::Cumulative, SeedTimeCounting::Consecutive]
                        {
                            let terms = Seeding {
                                seed_time_counting: counting,
                                ..terms(ratio_goal, seed, inactive, mode)
                            };
                            for uploaded_bytes in uploads {
                                for seconds in seconds {
                                    for last_activity_ms in activity {
                                        for now in nows {
                                            let counters = TorrentCounters {
                                                uploaded_bytes,
                                                downloaded_bytes: 100,
                                                finished_duration_s: seconds,
                                                consecutive_seeding_s: seconds / 2,
                                                last_activity_ms,
                                                ..TorrentCounters::default()
                                            };
                                            assert_eq!(
                                                goal_left(&terms, &counters, 100, now),
                                                goal_left_before(&terms, &counters, 100, now),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // The seed-time goal reads the counter its counting mode names: all the
    // seeding there has been, or only the current unbroken run.
    #[test]
    fn a_seed_time_goal_counts_all_seeding_or_only_the_current_run() {
        let counters = TorrentCounters {
            finished_duration_s: 90 * 60,
            consecutive_seeding_s: 30 * 60,
            ..TorrentCounters::default()
        };
        let cumulative = policy(None, Some(60), SeedingGoalMode::All);
        assert_eq!(goal_left(&cumulative, &counters, 100, 0), None);
        let consecutive = Seeding {
            seed_time_counting: SeedTimeCounting::Consecutive,
            ..cumulative
        };
        assert_eq!(
            goal_left(&consecutive, &counters, 100, 0).as_deref(),
            Some("seeded 30 of 60 minutes")
        );
        let run_on = TorrentCounters {
            consecutive_seeding_s: 60 * 60,
            ..counters
        };
        assert_eq!(goal_left(&consecutive, &run_on, 100, 0), None);
    }

    #[test]
    fn the_ratio_is_over_what_was_downloaded_or_over_what_is_held_until_one_percent() {
        let counters = |uploaded_bytes, downloaded_bytes| TorrentCounters {
            uploaded_bytes,
            downloaded_bytes,
            ..TorrentCounters::default()
        };
        assert_eq!(ratio(&counters(500, 250), 1000), 2.0);
        // Added complete: nothing downloaded, so what it holds stands in.
        assert_eq!(ratio(&counters(500, 0), 1000), 0.5);
        assert_eq!(ratio(&counters(500, 9), 1000), 0.5);
        assert_eq!(ratio(&counters(500, 10), 1000), 50.0);
        assert_eq!(ratio(&counters(0, 0), 0), 0.0);
        let policy = policy(Some(2.0), None, SeedingGoalMode::Any);
        assert_eq!(goal_left(&policy, &counters(500, 250), 1000, 0), None);
        // One 400 MiB file of a 100 GiB torrent, downloaded here: the ratio
        // is over the 400 MiB held, never over the torrent's size.
        const MIB: u64 = 1 << 20;
        let one_file = counters(400 * MIB, 400 * MIB);
        assert_eq!(ratio(&one_file, 400 * MIB), 1.0);
        let policy = self::policy(Some(1.0), None, SeedingGoalMode::Any);
        assert_eq!(goal_left(&policy, &one_file, 400 * MIB, 0), None);
    }

    // An announce that ended at 1,000 s, and the next as the engine sets it:
    // after the interval, or after the first retry's wait.
    fn announce(
        in_flight: bool,
        last: Option<Result<AnnounceOk, AnnounceError>>,
    ) -> TrackerAnnounce {
        let at = UNIX_EPOCH + Duration::from_secs(1_000);
        let (fails, next) = match &last {
            None => (0, None),
            Some(Ok(ok)) => (0, Some(at + ok.interval.max(ok.min_interval))),
            Some(Err(_)) => (1, Some(at + Duration::from_secs(17))),
        };
        TrackerAnnounce {
            url: "http://tracker.invalid/announce".into(),
            in_flight,
            last: last.map(|result| LastAnnounce { at, result }),
            fails,
            next_announce: next,
        }
    }

    fn accepted(interval: u64, min_interval: u64) -> AnnounceOk {
        AnnounceOk {
            interval: Duration::from_secs(interval),
            min_interval: Duration::from_secs(min_interval),
            seeders: Some(3),
            leechers: Some(5),
            downloaded: Some(8),
            peers: vec![
                std::net::SocketAddr::from(([10, 0, 0, 1], 6881)),
                std::net::SocketAddr::from(([10, 0, 0, 2], 6881)),
            ],
            warning: None,
        }
    }

    fn refused(kind: AnnounceErrorKind) -> AnnounceError {
        AnnounceError {
            kind,
            message: "refused".into(),
        }
    }

    #[test]
    fn a_trackers_status_is_its_latest_announce() {
        let never = tracker_view(2, &announce(false, None));
        assert_eq!(
            (never.tier, never.status, never.last_announce_ms),
            (2, TrackerStatus::NotContacted, None)
        );
        // In flight, a tracker keeps the state of its last outcome.
        let updating = tracker_view(0, &announce(true, None));
        assert_eq!(
            (updating.status, updating.updating),
            (TrackerStatus::NotContacted, true)
        );
        let updating = tracker_view(0, &announce(true, Some(Ok(accepted(1800, 30)))));
        assert_eq!(
            (updating.status, updating.updating),
            (TrackerStatus::Working, true)
        );

        let working = tracker_view(0, &announce(false, Some(Ok(accepted(1800, 30)))));
        assert_eq!(working.status, TrackerStatus::Working);
        assert!(!working.updating);
        assert_eq!(working.last_announce_ms, Some(1_000_000));
        assert_eq!(working.next_announce_ms, Some(2_800_000));
        assert_eq!(working.min_announce_ms, Some(1_030_000));
        assert_eq!(
            (
                working.seeders,
                working.leechers,
                working.downloaded,
                working.peers
            ),
            (Some(3), Some(5), Some(8), Some(2))
        );
        assert_eq!((working.message, working.fails), (None, 0));
        // The tracker's warning is its message.
        let warned = AnnounceOk {
            warning: Some("slow".into()),
            ..accepted(1800, 30)
        };
        assert_eq!(
            tracker_view(0, &announce(false, Some(Ok(warned))))
                .message
                .as_deref(),
            Some("slow")
        );

        // qBittorrent TorrentImpl: tracker_failure is a tracker error,
        // announce_skipped unreachable, anything else not working.
        for (kind, status) in [
            (AnnounceErrorKind::Refused, TrackerStatus::TrackerError),
            (AnnounceErrorKind::Skipped, TrackerStatus::Unreachable),
            (AnnounceErrorKind::Failed, TrackerStatus::NotWorking),
        ] {
            let failed = tracker_view(0, &announce(false, Some(Err(refused(kind)))));
            assert_eq!(failed.status, status);
            assert_eq!(failed.message.as_deref(), Some("refused"));
            assert_eq!(failed.last_announce_ms, Some(1_000_000));
            assert_eq!(
                (
                    failed.seeders,
                    failed.leechers,
                    failed.next_announce_ms,
                    failed.fails
                ),
                (None, None, Some(1_017_000), 1)
            );
        }
    }

    #[test]
    fn the_swarm_is_the_largest_tracker_count_and_the_soonest_announce() {
        let counted = |seeders, leechers, interval| {
            announce(
                false,
                Some(Ok(AnnounceOk {
                    seeders,
                    leechers,
                    ..accepted(interval, 30)
                })),
            )
        };
        let announces = [
            counted(Some(3), Some(9), 1800),
            counted(Some(7), None, 60),
            announce(false, Some(Err(refused(AnnounceErrorKind::Failed)))),
        ];
        // The failed announce at 1,000 s is retried at 1,017 s.
        assert_eq!(
            swarm(&announces, 1_010_500),
            Swarm {
                seeds: Some(7),
                leechers: Some(9),
                reannounce_seconds: Some(7),
                next_announce_ms: Some(1_017_000),
            }
        );
        // An announce already due is due now.
        assert_eq!(swarm(&announces, 1_100_000).reannounce_seconds, Some(0));
        assert_eq!(swarm(&[], 0), Swarm::default());
    }

    // An answer whose interval has long passed with no announce in flight
    // stands while the run still announces to the tracker.
    #[test]
    fn an_answer_stands_past_its_interval() {
        let stale = announce(false, Some(Ok(accepted(1, 30))));
        assert_eq!(tracker_view(0, &stale).status, TrackerStatus::Working);
    }

    // The run has stopped announcing to the tracker: its `stopped` answer,
    // or the failure of it, is the last outcome the row reads, with no
    // floor for an announce that is not coming.
    #[test]
    fn a_tracker_the_run_stopped_announcing_to_keeps_its_last_outcome() {
        let stopped = TrackerAnnounce {
            next_announce: None,
            ..announce(false, Some(Ok(accepted(1800, 30))))
        };
        let view = tracker_view(0, &stopped);
        assert_eq!(
            (
                view.status,
                view.updating,
                view.next_announce_ms,
                view.min_announce_ms
            ),
            (TrackerStatus::Working, false, None, None)
        );
        assert_eq!(
            (view.last_announce_ms, view.peers, view.seeders),
            (Some(1_000_000), Some(2), Some(3))
        );
        let failed = TrackerAnnounce {
            next_announce: None,
            ..announce(false, Some(Err(refused(AnnounceErrorKind::Failed))))
        };
        let view = tracker_view(0, &failed);
        assert_eq!(
            (view.status, view.message.as_deref(), view.min_announce_ms),
            (TrackerStatus::NotWorking, Some("refused"), None)
        );
    }

    // A tracker named by its address is resolved by no one; one named by
    // its host, by whoever resolves the route's names.
    #[test]
    fn a_trackers_host_is_resolved_by_the_routes_naming_unless_it_is_an_address() {
        for (url, by) in [
            ("http://tracker.invalid/announce", Some(ResolvedBy::Proxy)),
            ("udp://tracker.invalid:80", Some(ResolvedBy::Proxy)),
            ("http://192.0.2.7:6969/announce", Some(ResolvedBy::Literal)),
            ("udp://192.0.2.7:6969", Some(ResolvedBy::Literal)),
            ("udp://[2001:db8::7]:6969", Some(ResolvedBy::Literal)),
            ("not a url", None),
        ] {
            assert_eq!(resolved_by(url, Some(ResolvedBy::Proxy)), by, "{url}");
        }
        assert_eq!(resolved_by("http://tracker.invalid/announce", None), None);

        let listed = vec![TrackerEntry {
            tier: 0,
            url: "http://tracker.invalid/announce".into(),
            named_by: NamedBy::Torrent,
        }];
        let rows = trackers(listed, &[], Some(ResolvedBy::Host));
        assert_eq!(rows[0].resolved_by, Some(ResolvedBy::Host));
    }

    #[test]
    fn a_torrent_lists_its_trackers_until_it_runs_then_the_runs() {
        let listed = vec![
            TrackerEntry {
                tier: 1,
                url: "udp://backup.invalid:80".into(),
                named_by: NamedBy::Operator,
            },
            TrackerEntry {
                tier: 0,
                url: "http://tracker.invalid".into(),
                named_by: NamedBy::Torrent,
            },
        ];
        let never: Vec<_> = trackers(listed.clone(), &[], None)
            .into_iter()
            .map(|tracker| (tracker.tier, tracker.url, tracker.status, tracker.named_by))
            .collect();
        assert_eq!(
            never,
            [
                (
                    0,
                    "http://tracker.invalid".into(),
                    TrackerStatus::NotContacted,
                    NamedBy::Torrent
                ),
                (
                    1,
                    "udp://backup.invalid:80".into(),
                    TrackerStatus::NotContacted,
                    NamedBy::Operator
                ),
            ]
        );

        // The run names each tracker by its parsed URL, and one the torrent
        // does not list came with its magnet.
        let run = [
            TrackerAnnounce {
                url: "udp://backup.invalid:80".into(),
                ..Default::default()
            },
            TrackerAnnounce {
                url: "http://tracker.invalid/".into(),
                in_flight: true,
                ..Default::default()
            },
            TrackerAnnounce {
                url: "http://magnet.invalid/announce".into(),
                ..Default::default()
            },
        ];
        let ran: Vec<_> = trackers(listed, &run, None)
            .into_iter()
            .map(|tracker| {
                (
                    tracker.tier,
                    tracker.url,
                    tracker.updating,
                    tracker.named_by,
                )
            })
            .collect();
        assert_eq!(
            ran,
            [
                (
                    0,
                    "http://magnet.invalid/announce".into(),
                    false,
                    NamedBy::Torrent
                ),
                (0, "http://tracker.invalid/".into(), true, NamedBy::Torrent),
                (
                    1,
                    "udp://backup.invalid:80".into(),
                    false,
                    NamedBy::Operator
                ),
            ]
        );
    }

    // Many trackers, some listed twice in two tiers and some the run names
    // that the torrent does not list: each in the tier, and as whoever, the
    // search of the listed trackers found it in.
    #[test]
    fn a_runs_trackers_take_the_tiers_the_search_gave() {
        let listed: Vec<TrackerEntry> = (0..3_000u32)
            .map(|index| TrackerEntry {
                tier: (index * 7) % 5,
                url: format!("http://host{}.invalid/announce", index / 2),
                named_by: if index % 3 == 0 {
                    NamedBy::Operator
                } else {
                    NamedBy::Torrent
                },
            })
            .chain([TrackerEntry {
                tier: 9,
                url: "not a url".into(),
                named_by: NamedBy::Operator,
            }])
            .collect();
        let run: Vec<TrackerAnnounce> = (0..2_000u32)
            .map(|index| TrackerAnnounce {
                url: format!("http://host{index}.invalid/announce"),
                ..Default::default()
            })
            .collect();
        let tiers: Vec<(String, u32, NamedBy)> = listed
            .iter()
            .filter_map(|entry| {
                let url = weaver_torrent::Url::parse(&entry.url).ok()?.into();
                Some((url, entry.tier, entry.named_by))
            })
            .collect();
        let mut by_search: Vec<(u32, String, NamedBy)> = run
            .iter()
            .map(|announce| {
                let (tier, named_by) = tiers
                    .iter()
                    .find(|(url, ..)| *url == announce.url)
                    .map_or((0, NamedBy::Torrent), |(_, tier, named_by)| {
                        (*tier, *named_by)
                    });
                (tier, announce.url.clone(), named_by)
            })
            .collect();
        by_search.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        let shown: Vec<(u32, String, NamedBy)> = trackers(listed, &run, None)
            .into_iter()
            .map(|tracker| (tracker.tier, tracker.url, tracker.named_by))
            .collect();
        assert!(shown.iter().any(|tracker| tracker.0 == 4));
        assert_eq!(shown, by_search);
    }
}
