use std::collections::BTreeSet;
use std::path::PathBuf;

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};

use super::events::{EventContext, run_event};
use super::instances::InstanceTrigger;
use super::model::{ScriptEventLabel, ScriptTaskTime};
use super::runner::CompatibilityFacts;
use crate::bandwidth::schedule::SharedSchedules;
use crate::bandwidth::{ScheduleAction, ScheduleEntry, Weekday};
use crate::settings::SharedConfig;
use crate::{Database, StateError};

#[derive(Default)]
pub struct Occurrences {
    last: Option<NaiveDateTime>,
    fired: BTreeSet<(String, NaiveDate, NaiveTime)>,
}

impl Occurrences {
    pub fn advance(&mut self, entries: &[ScheduleEntry], now: NaiveDateTime) -> Vec<ScheduleEntry> {
        let previous = self.last.replace(now);
        self.fired
            .retain(|(_, date, _)| *date >= now.date() - Duration::days(2));
        let Some(previous) = previous else {
            return entries
                .iter()
                .filter(|entry| {
                    entry.enabled
                        && matches!(
                            entry.action,
                            ScheduleAction::RunScript {
                                run_at_startup: true,
                                ..
                            }
                        )
                })
                .cloned()
                .collect();
        };
        let delta = now - previous;
        if delta <= Duration::zero() || delta > Duration::minutes(90) {
            return Vec::new();
        }
        let mut due = Vec::new();
        for entry in entries.iter().filter(|entry| {
            entry.enabled && matches!(entry.action, ScheduleAction::RunScript { .. })
        }) {
            for date in [previous.date(), now.date()]
                .into_iter()
                .collect::<BTreeSet<_>>()
            {
                if !entry.days.is_empty()
                    && !entry.days.contains(&Weekday::from_chrono(date.weekday()))
                {
                    continue;
                }
                for declaration in entry.time.split([',', ';']) {
                    let Ok(time) = declaration.trim().parse::<ScriptTaskTime>() else {
                        continue;
                    };
                    let times = match time {
                        ScriptTaskTime::Startup => Vec::new(),
                        ScriptTaskTime::Daily { hour, minute } => vec![
                            NaiveTime::from_hms_opt(hour.into(), minute.into(), 0)
                                .expect("validated time"),
                        ],
                        ScriptTaskTime::Hourly { minute } => (0..24)
                            .map(|hour| {
                                NaiveTime::from_hms_opt(hour, minute.into(), 0)
                                    .expect("validated time")
                            })
                            .collect(),
                    };
                    for time in times {
                        let occurrence = date.and_time(time);
                        if previous < occurrence
                            && occurrence <= now
                            && self.fired.insert((entry.id.clone(), date, time))
                        {
                            due.push(entry.clone());
                        }
                    }
                }
            }
        }
        due
    }
}

pub fn spawn_script_evaluator(db: Database, config: SharedConfig, schedules: SharedSchedules) {
    tokio::spawn(async move {
        let mut occurrences = Occurrences::default();
        let mut running = std::collections::BTreeMap::<String, tokio::task::JoinHandle<()>>::new();
        let mut interval = tokio::time::interval(crate::e2e_clock::schedule_poll_interval());
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            running.retain(|_, task| !task.is_finished());
            let entries = schedules.read().await.clone();
            for entry in occurrences.advance(&entries, crate::e2e_clock::local_now().naive_local())
            {
                if running.contains_key(&entry.id) {
                    tracing::debug!(rule_id = %entry.id, "skipping overlapping scheduled script");
                    continue;
                }
                let db = db.clone();
                let config = config.clone();
                let id = entry.id.clone();
                let task = tokio::spawn(async move {
                    if let Err(error) = run_scheduled_script(&db, &config, &entry).await {
                        tracing::warn!(rule_id = %entry.id, %error, "scheduled script failed");
                    }
                });
                running.insert(id, task);
            }
        }
    });
}

async fn run_scheduled_script(
    db: &Database,
    config: &SharedConfig,
    entry: &ScheduleEntry,
) -> Result<(), StateError> {
    let ScheduleAction::RunScript { instance_id, .. } = &entry.action else {
        return Ok(());
    };
    let instance = tokio::task::spawn_blocking({
        let db = db.clone();
        let instance_id = instance_id.clone();
        move || db.script_instance(&instance_id)
    })
    .await
    .map_err(|error| StateError::Database(error.to_string()))??;
    // A row may outlive the instance it names, or the instance may have been
    // turned off or given another trigger since. Either way there is nothing
    // for this row to run.
    let Some(instance) = instance
        .filter(|instance| instance.enabled && instance.trigger == InstanceTrigger::Schedule)
    else {
        tracing::debug!(rule_id = %entry.id, "scheduled script has no instance to run");
        return Ok(());
    };
    let digest = blake3::hash(entry.id.as_bytes());
    let task_id =
        u64::from_le_bytes(digest.as_bytes()[..8].try_into().expect("eight bytes")).max(1);
    let config = config.read().await;
    let cwd = PathBuf::from(&config.data_dir);
    let mut context = EventContext {
        job_id: None,
        event: ScriptEventLabel::Scheduler(task_id),
        category: None,
        cwd: cwd.clone(),
        env: [("NZBSP_TASKID".into(), task_id.to_string())]
            .into_iter()
            .collect(),
        facts: CompatibilityFacts {
            data_dir: Some(cwd),
            intermediate_dir: Some(PathBuf::from(config.intermediate_dir())),
            complete_dir: Some(PathBuf::from(config.complete_dir())),
            ..Default::default()
        },
        instances: Some(vec![instance]),
        scratch: None,
    };
    drop(config);
    let mut nonce = [0_u8; 16];
    getrandom::fill(&mut nonce).map_err(|error| StateError::Database(error.to_string()))?;
    run_event(
        db,
        &mut context,
        &format!("scheduler:{}:{}", entry.id, hex::encode(nonce)),
        None,
        None,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn at(time: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M").unwrap()
    }
    fn rule(time: &str) -> ScheduleEntry {
        ScheduleEntry {
            id: "test".into(),
            enabled: true,
            label: String::new(),
            days: Vec::new(),
            time: time.into(),
            times: Vec::new(),
            every_hour_at_minute: None,
            action: ScheduleAction::RunScript {
                instance_id: "test".into(),
                run_at_startup: false,
            },
        }
    }
    #[test]
    fn startup_and_clock_jumps_do_not_replay_missed_runs() {
        let mut tracker = Occurrences::default();
        let rules = [rule("01:30")];
        assert!(tracker.advance(&rules, at("2026-10-25 01:31")).is_empty());
        assert!(tracker.advance(&rules, at("2026-10-26 01:31")).is_empty());
        assert!(tracker.advance(&rules, at("2026-10-26 01:29")).is_empty());
        assert_eq!(tracker.advance(&rules, at("2026-10-26 01:30")).len(), 1);
        assert!(tracker.advance(&rules, at("2026-10-26 01:00")).is_empty());
        assert!(tracker.advance(&rules, at("2026-10-26 01:31")).is_empty());
    }
    #[test]
    fn spring_gap_and_hourly_rules_fire_once_per_occurrence() {
        let mut tracker = Occurrences::default();
        let rules = [rule("02:30")];
        tracker.advance(&rules, at("2026-03-08 01:59"));
        assert_eq!(tracker.advance(&rules, at("2026-03-08 03:00")).len(), 1);
        assert!(tracker.advance(&rules, at("2026-03-08 03:01")).is_empty());
        let rules = [rule("*:15")];
        assert_eq!(tracker.advance(&rules, at("2026-03-08 03:15")).len(), 1);
        assert_eq!(tracker.advance(&rules, at("2026-03-08 04:15")).len(), 1);
    }

    #[test]
    fn startup_requires_an_explicit_opt_in_and_fires_only_once() {
        let mut startup = rule("*");
        let mut tracker = Occurrences::default();
        assert!(
            tracker
                .advance(&[startup.clone()], at("2026-03-08 01:00"))
                .is_empty()
        );
        startup.action = ScheduleAction::RunScript {
            instance_id: "test".into(),
            run_at_startup: true,
        };
        let mut tracker = Occurrences::default();
        assert_eq!(
            tracker
                .advance(&[startup.clone()], at("2026-03-08 01:00"))
                .len(),
            1
        );
        assert!(
            tracker
                .advance(&[startup], at("2026-03-08 01:01"))
                .is_empty()
        );
    }

    #[test]
    fn time_lists_dedupe_matching_hourly_and_daily_occurrences() {
        let mut tracker = Occurrences::default();
        let rules = [rule("*:15, 03:15;04:30")];
        tracker.advance(&rules, at("2026-03-08 03:14"));
        assert_eq!(tracker.advance(&rules, at("2026-03-08 03:15")).len(), 1);
        assert_eq!(tracker.advance(&rules, at("2026-03-08 04:15")).len(), 1);
        assert_eq!(tracker.advance(&rules, at("2026-03-08 04:30")).len(), 1);
    }
}
