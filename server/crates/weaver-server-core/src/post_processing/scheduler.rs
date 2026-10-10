//! Runs schedule jobs at the times saved on each job.
//!
//! The evaluator ticks once a minute from memory. It reads the jobs from the
//! database only after one of them changed, never to decide what is due.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};

use super::events::{EventContext, run_event};
use super::instances::{InstanceTrigger, ScriptInstance};
use super::model::{ScriptEventLabel, ScriptTaskTime};
use super::runner::CompatibilityFacts;
use crate::bandwidth::Weekday;
use crate::settings::SharedConfig;
use crate::{Database, StateError};

/// Which occurrences have fired, so each fires once however the ticks fall.
#[derive(Default)]
pub struct Occurrences {
    last: Option<NaiveDateTime>,
    fired: BTreeSet<(String, NaiveDate, NaiveTime)>,
}

impl Occurrences {
    /// The jobs due between the last tick and `now`. The first tick starts
    /// only startup jobs. After a forward clock jump, each job catches up its
    /// latest missed occurrence once, without replaying the entire backlog.
    pub fn advance(&mut self, jobs: &[ScriptInstance], now: NaiveDateTime) -> Vec<String> {
        let previous = self.last.replace(now);
        self.fired
            .retain(|(_, date, _)| *date >= now.date() - Duration::days(2));
        let scheduled = jobs
            .iter()
            .filter(|job| job.enabled && job.trigger == InstanceTrigger::Schedule);
        let Some(previous) = previous else {
            return scheduled
                .filter(|job| job.schedule.run_at_startup)
                .map(|job| job.id.clone())
                .collect();
        };
        let delta = now - previous;
        if delta <= Duration::zero() {
            return Vec::new();
        }
        let mut due = Vec::new();
        for job in scheduled {
            let mut times = BTreeSet::new();
            for time in job.schedule.task_times() {
                match time {
                    ScriptTaskTime::Startup => {}
                    ScriptTaskTime::Daily { hour, minute } => {
                        times.extend(NaiveTime::from_hms_opt(hour.into(), minute.into(), 0));
                    }
                    ScriptTaskTime::Hourly { minute } => times.extend(
                        (0..24).filter_map(|hour| NaiveTime::from_hms_opt(hour, minute.into(), 0)),
                    ),
                }
            }
            let mut fired = false;
            // Every supported rule repeats at least weekly. Eight dates
            // cover the latest eligible occurrence even after a long sleep.
            'dates: for offset in 0..=7 {
                let Some(date) = now.date().checked_sub_signed(Duration::days(offset)) else {
                    break;
                };
                if date < previous.date() {
                    break;
                }
                if !job.schedule.days.is_empty()
                    && !job
                        .schedule
                        .days
                        .contains(&Weekday::from_chrono(date.weekday()))
                {
                    continue;
                }
                for time in times.iter().rev() {
                    let occurrence = date.and_time(*time);
                    if previous < occurrence
                        && occurrence <= now
                        && self.fired.insert((job.id.clone(), date, *time))
                    {
                        fired = true;
                        break 'dates;
                    }
                }
            }
            if fired {
                due.push(job.id.clone());
            }
        }
        due
    }
}

impl Database {
    /// Every script job, as the evaluator last read it. A write to any job
    /// clears this, and the next tick reads them again.
    fn schedule_jobs(&self) -> Result<Arc<Vec<ScriptInstance>>, StateError> {
        let revision = {
            let cache = self
                .script_runtime
                .schedule_jobs
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(jobs) = &cache.1 {
                return Ok(jobs.clone());
            }
            cache.0
        };
        let jobs = Arc::new(
            self.script_instances()?
                .into_iter()
                .filter(|job| job.trigger == InstanceTrigger::Schedule)
                .collect::<Vec<_>>(),
        );
        let mut cache = self
            .script_runtime
            .schedule_jobs
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if cache.0 == revision {
            cache.1 = Some(jobs.clone());
        }
        Ok(jobs)
    }
}

pub fn spawn_script_evaluator(db: Database, config: SharedConfig) {
    tokio::spawn(async move {
        let mut occurrences = Occurrences::default();
        let mut next_trim = tokio::time::Instant::now();
        let mut running = std::collections::BTreeMap::<String, tokio::task::JoinHandle<()>>::new();
        let mut interval = tokio::time::interval(crate::e2e_clock::schedule_poll_interval());
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if tokio::time::Instant::now() >= next_trim {
                db.request_script_output_trim();
                next_trim = tokio::time::Instant::now() + std::time::Duration::from_secs(3600);
            }
            running.retain(|_, task| !task.is_finished());
            let jobs = match tokio::task::spawn_blocking({
                let db = db.clone();
                move || db.schedule_jobs()
            })
            .await
            {
                Ok(Ok(jobs)) => jobs,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "could not read the schedule jobs");
                    continue;
                }
                Err(error) => {
                    tracing::warn!(%error, "could not read the schedule jobs");
                    continue;
                }
            };
            for id in occurrences.advance(&jobs, crate::e2e_clock::local_now().naive_local()) {
                if running.contains_key(&id) {
                    tracing::debug!(instance = %id, "skipping an overlapping scheduled script");
                    continue;
                }
                let db = db.clone();
                let config = config.clone();
                let job = id.clone();
                let task = tokio::spawn(async move {
                    match run_scheduled_script(&db, &config, &job).await {
                        Ok(Scheduled::Ran | Scheduled::Gone) => {}
                        Err(error) => {
                            tracing::warn!(instance = %job, %error, "scheduled script failed");
                        }
                    }
                });
                running.insert(id, task);
            }
        }
    });
}

/// What came of a job's occurrence.
#[derive(Debug, Eq, PartialEq)]
enum Scheduled {
    Ran,
    /// Deleted, turned off or moved to another trigger since it was due.
    Gone,
}

async fn run_scheduled_script(
    db: &Database,
    config: &SharedConfig,
    instance_id: &str,
) -> Result<Scheduled, StateError> {
    let instance = tokio::task::spawn_blocking({
        let db = db.clone();
        let instance_id = instance_id.to_string();
        move || db.script_instance(&instance_id)
    })
    .await
    .map_err(|error| StateError::Database(error.to_string()))??;
    let Some(instance) = instance
        .filter(|instance| instance.enabled && instance.trigger == InstanceTrigger::Schedule)
    else {
        return Ok(Scheduled::Gone);
    };
    let digest = blake3::hash(instance.id.as_bytes());
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
        &format!("scheduler:{instance_id}:{}", hex::encode(nonce)),
        None,
        None,
    )
    .await?;
    Ok(Scheduled::Ran)
}

#[cfg(test)]
mod tests {
    use super::super::instances::{InstanceSchedule, ScriptInstanceDraft};
    use super::super::model::ScriptName;
    use super::*;

    fn at(time: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M").unwrap()
    }

    fn job(times: &[&str]) -> ScriptInstance {
        ScriptInstance {
            id: "test".into(),
            name: "test".into(),
            script: ScriptName::new("tidy.sh").unwrap(),
            trigger: InstanceTrigger::Schedule,
            inputs: Vec::new(),
            categories: Vec::new(),
            enabled: true,
            blocking: true,
            timeout_seconds: None,
            run_order: 0,
            schedule: InstanceSchedule {
                days: Vec::new(),
                times: times.iter().map(|time| time.to_string()).collect(),
                run_at_startup: false,
            },
        }
    }

    #[test]
    fn startup_skips_old_runs_and_forward_jumps_catch_up_once() {
        let mut tracker = Occurrences::default();
        let jobs = [job(&["01:30"])];
        assert!(tracker.advance(&jobs, at("2026-10-25 01:31")).is_empty());
        assert_eq!(tracker.advance(&jobs, at("2026-10-26 01:31")), ["test"]);
        assert!(tracker.advance(&jobs, at("2026-10-26 01:29")).is_empty());
        assert!(tracker.advance(&jobs, at("2026-10-26 01:30")).is_empty());
        assert!(tracker.advance(&jobs, at("2026-10-26 01:00")).is_empty());
        assert!(tracker.advance(&jobs, at("2026-10-26 01:31")).is_empty());
    }

    #[test]
    fn a_long_sleep_catches_up_the_latest_weekly_occurrence_once() {
        let mut tracker = Occurrences::default();
        let mut weekly = job(&["01:30", "02:30"]);
        weekly.schedule.days = vec![Weekday::Mon];
        let jobs = [weekly];
        tracker.advance(&jobs, at("2026-09-01 00:00"));
        assert_eq!(tracker.advance(&jobs, at("2026-10-09 12:00")), ["test"]);
        assert!(tracker.fired.contains(&(
            "test".into(),
            at("2026-10-05 02:30").date(),
            at("2026-10-05 02:30").time()
        )));
        assert!(tracker.advance(&jobs, at("2026-10-09 12:01")).is_empty());
    }

    #[test]
    fn spring_gap_and_hourly_times_fire_once_per_occurrence() {
        let mut tracker = Occurrences::default();
        let jobs = [job(&["02:30"])];
        tracker.advance(&jobs, at("2026-03-08 01:59"));
        assert_eq!(tracker.advance(&jobs, at("2026-03-08 03:00")).len(), 1);
        assert!(tracker.advance(&jobs, at("2026-03-08 03:01")).is_empty());
        let jobs = [job(&["*:15"])];
        assert_eq!(tracker.advance(&jobs, at("2026-03-08 03:15")).len(), 1);
        assert_eq!(tracker.advance(&jobs, at("2026-03-08 04:15")).len(), 1);
    }

    #[test]
    fn startup_requires_an_explicit_opt_in_and_fires_only_once() {
        let mut startup = job(&[]);
        let mut tracker = Occurrences::default();
        assert!(
            tracker
                .advance(&[startup.clone()], at("2026-03-08 01:00"))
                .is_empty()
        );
        startup.schedule.run_at_startup = true;
        let mut tracker = Occurrences::default();
        assert_eq!(
            tracker.advance(&[startup.clone()], at("2026-03-08 01:00")),
            ["test"]
        );
        assert!(
            tracker
                .advance(&[startup], at("2026-03-08 01:01"))
                .is_empty()
        );
    }

    #[test]
    fn a_time_named_twice_runs_the_job_once() {
        let mut tracker = Occurrences::default();
        let jobs = [job(&["*:15", "03:15", "04:30"])];
        tracker.advance(&jobs, at("2026-03-08 03:14"));
        assert_eq!(tracker.advance(&jobs, at("2026-03-08 03:15")), ["test"]);
        assert_eq!(tracker.advance(&jobs, at("2026-03-08 04:15")), ["test"]);
        assert_eq!(tracker.advance(&jobs, at("2026-03-08 04:30")), ["test"]);
    }

    #[test]
    fn a_job_runs_only_on_its_days_and_only_while_turned_on() {
        let mut friday = job(&["07:00"]);
        friday.schedule.days = vec![Weekday::Fri];
        let mut off = job(&["07:00"]);
        off.id = "off".into();
        off.enabled = false;
        let jobs = [friday, off];
        let mut tracker = Occurrences::default();
        // 2026-10-08 is a Thursday.
        tracker.advance(&jobs, at("2026-10-08 06:59"));
        assert!(tracker.advance(&jobs, at("2026-10-08 07:00")).is_empty());
        tracker.advance(&jobs, at("2026-10-09 06:59"));
        assert_eq!(tracker.advance(&jobs, at("2026-10-09 07:00")), ["test"]);
    }

    #[test]
    fn the_jobs_are_read_again_only_after_one_changed() {
        let db = Database::open_in_memory().unwrap();
        let script = ScriptName::new("tidy.sh").unwrap();
        assert!(db.schedule_jobs().unwrap().is_empty());
        let first = db
            .create_script_instance(
                ScriptInstanceDraft::new(script.clone(), InstanceTrigger::Schedule)
                    .runs_at(&[], &["07:00"]),
            )
            .unwrap();
        let read = db.schedule_jobs().unwrap();
        assert_eq!(read.len(), 1);
        // Served from memory until something changes.
        assert!(Arc::ptr_eq(&read, &db.schedule_jobs().unwrap()));

        let mut moved = ScriptInstanceDraft::from_instance(&first);
        moved.trigger = InstanceTrigger::Scan;
        db.update_script_instance(&first.id, moved).unwrap();
        assert!(db.schedule_jobs().unwrap().is_empty());
    }
}
