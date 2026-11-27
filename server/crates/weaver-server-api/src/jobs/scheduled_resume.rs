// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use weaver_server_core::settings::persistence::SettingsPersistenceQueries;
use weaver_server_core::settings::queries::SettingsQueries;

use tokio::sync::Mutex;
use tracing::error;
use weaver_server_core::jobs::HeldClass;
use weaver_server_core::{Database, SchedulerError, SchedulerHandle, StateError};

const SCHEDULED_RESUME_SETTING: &str = "nzbget.scheduled_resume_at";
/// A class's timed resume is saved under this prefix and its protocol's key.
pub const CLASS_RESUME_SETTING_PREFIX: &str = "scheduled_resume_at.class.";

#[derive(Debug, thiserror::Error)]
pub enum ScheduledResumeError {
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    #[error(transparent)]
    State(#[from] StateError),
    #[error("scheduled-resume database task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// What a pause holds: the whole queue, or every job of one class. Each has
/// its own timed resume, so lifting one never lifts the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Target {
    All,
    Class(HeldClass),
}

impl Target {
    fn setting(self) -> String {
        match self {
            Self::All => SCHEDULED_RESUME_SETTING.to_owned(),
            Self::Class(class) => {
                format!("{CLASS_RESUME_SETTING_PREFIX}{}", class.protocol().as_str())
            }
        }
    }

    fn targets() -> impl Iterator<Item = Self> {
        std::iter::once(Self::All).chain(HeldClass::all().map(Self::Class))
    }
}

#[derive(Clone)]
pub struct ScheduledResumeCoordinator {
    inner: Arc<ScheduledResumeInner>,
}

struct ScheduledResumeInner {
    db: Database,
    handle: SchedulerHandle,
    state: Mutex<ScheduledResumeState>,
}

#[derive(Default)]
struct ScheduledResumeState {
    generation: u64,
    // Each armed timer's deadline, in epoch seconds, and the generation
    // that armed it.
    armed: BTreeMap<Target, (u64, u64)>,
}

impl ScheduledResumeState {
    fn invalidate(&mut self, target: Target) {
        self.armed.remove(&target);
    }

    fn arm(&mut self, target: Target, resume_at_epoch_secs: u64) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.armed
            .insert(target, (resume_at_epoch_secs, self.generation));
        self.generation
    }

    fn deadline(&self, target: Target) -> u64 {
        self.armed.get(&target).map_or(0, |(at, _)| *at)
    }
}

impl ScheduledResumeCoordinator {
    pub fn new(db: Database, handle: SchedulerHandle) -> Self {
        Self {
            inner: Arc::new(ScheduledResumeInner {
                db,
                handle,
                state: Mutex::new(ScheduledResumeState::default()),
            }),
        }
    }

    pub async fn resume_at(&self) -> u64 {
        self.inner.state.lock().await.deadline(Target::All)
    }

    /// When `class`'s timed pause lifts, in epoch seconds; 0 when none is set.
    pub async fn class_resume_at(&self, class: HeldClass) -> u64 {
        self.inner.state.lock().await.deadline(Target::Class(class))
    }

    // Restore the persisted resume deadlines once during server startup.
    pub async fn recover(&self) -> Result<(), ScheduledResumeError> {
        for target in Target::targets() {
            self.recover_one(target).await?;
        }
        Ok(())
    }

    async fn recover_one(&self, target: Target) -> Result<(), ScheduledResumeError> {
        let db = self.inner.db.clone();
        let Some(value) = read_setting(db.clone(), target).await? else {
            return Ok(());
        };
        let Ok(resume_at_epoch_secs) = value.parse::<u64>() else {
            delete_setting(db, target).await?;
            return Ok(());
        };

        if resume_at_epoch_secs <= unix_now_secs() {
            let mut state = self.inner.state.lock().await;
            state.invalidate(target);
            let resume_result = self.hold(target, false).await;
            let clear_result = delete_setting_if_value(db, target, resume_at_epoch_secs).await;
            resume_result?;
            clear_result?;
            return Ok(());
        }

        let generation = {
            let mut state = self.inner.state.lock().await;
            state.arm(target, resume_at_epoch_secs)
        };
        self.spawn_timer(target, resume_at_epoch_secs, generation);
        Ok(())
    }

    pub async fn schedule_resume(
        &self,
        resume_at_epoch_secs: u64,
    ) -> Result<(), ScheduledResumeError> {
        let generation = {
            let mut state = self.inner.state.lock().await;
            write_setting(self.inner.db.clone(), Target::All, resume_at_epoch_secs).await?;
            state.arm(Target::All, resume_at_epoch_secs)
        };
        self.spawn_timer(Target::All, resume_at_epoch_secs, generation);
        Ok(())
    }

    pub async fn pause_all(&self) -> Result<(), ScheduledResumeError> {
        self.set_paused(Target::All, true).await
    }

    pub async fn pause_until(&self, resume_at_epoch_secs: u64) -> Result<(), ScheduledResumeError> {
        self.pause_target_until(Target::All, resume_at_epoch_secs)
            .await
    }

    pub async fn resume_all(&self) -> Result<(), ScheduledResumeError> {
        self.set_paused(Target::All, false).await
    }

    /// The operator's pause of every job of `class`, with no timer.
    pub async fn pause_class(&self, class: HeldClass) -> Result<(), ScheduledResumeError> {
        self.set_paused(Target::Class(class), true).await
    }

    /// Pause every job of `class` now and lift that pause at the deadline.
    pub async fn pause_class_until(
        &self,
        class: HeldClass,
        resume_at_epoch_secs: u64,
    ) -> Result<(), ScheduledResumeError> {
        self.pause_target_until(Target::Class(class), resume_at_epoch_secs)
            .await
    }

    pub async fn resume_class(&self, class: HeldClass) -> Result<(), ScheduledResumeError> {
        self.set_paused(Target::Class(class), false).await
    }

    // Pause now and resume at the deadline, under one lock so no other
    // pause or resume lands between the two.
    async fn pause_target_until(
        &self,
        target: Target,
        resume_at_epoch_secs: u64,
    ) -> Result<(), ScheduledResumeError> {
        let generation = {
            let mut state = self.inner.state.lock().await;
            state.invalidate(target);
            self.hold(target, true).await?;
            write_setting(self.inner.db.clone(), target, resume_at_epoch_secs).await?;
            state.arm(target, resume_at_epoch_secs)
        };
        self.spawn_timer(target, resume_at_epoch_secs, generation);
        Ok(())
    }

    // Serialize user-initiated pause/resume operations with timer expiry.
    //
    // Core bandwidth schedule actions update the pipeline directly and deliberately
    // bypass this API-layer coordinator. An armed timer can therefore still
    // supersede `ScheduleAction::Pause`; moving that ownership into core is outside
    // this coordinator's scope.
    async fn set_paused(&self, target: Target, paused: bool) -> Result<(), ScheduledResumeError> {
        let mut state = self.inner.state.lock().await;
        state.invalidate(target);
        let scheduler_result = self.hold(target, paused).await;
        let clear_result = delete_setting(self.inner.db.clone(), target).await;
        scheduler_result?;
        clear_result
    }

    async fn hold(&self, target: Target, paused: bool) -> Result<(), SchedulerError> {
        let handle = &self.inner.handle;
        match (target, paused) {
            (Target::All, true) => handle.pause_all().await,
            (Target::All, false) => handle.resume_all().await,
            (Target::Class(class), true) => handle.pause_class(class).await,
            (Target::Class(class), false) => handle.resume_class(class).await,
        }
    }

    fn spawn_timer(&self, target: Target, resume_at_epoch_secs: u64, generation: u64) {
        let coordinator = self.clone();
        tokio::spawn(async move {
            let delay = resume_at_epoch_secs.saturating_sub(unix_now_secs());
            tokio::time::sleep(Duration::from_secs(delay)).await;
            if let Err(error) = coordinator
                .resume_if_current(target, resume_at_epoch_secs, generation)
                .await
            {
                error!(%error, "scheduled resume failed");
            }
        });
    }

    async fn resume_if_current(
        &self,
        target: Target,
        resume_at_epoch_secs: u64,
        generation: u64,
    ) -> Result<(), ScheduledResumeError> {
        let mut state = self.inner.state.lock().await;
        if state.armed.get(&target) != Some(&(resume_at_epoch_secs, generation)) {
            return Ok(());
        }

        state.invalidate(target);
        let resume_result = self.hold(target, false).await;
        let clear_result =
            delete_setting_if_value(self.inner.db.clone(), target, resume_at_epoch_secs).await;
        resume_result?;
        clear_result
    }
}

async fn read_setting(
    db: Database,
    target: Target,
) -> Result<Option<String>, ScheduledResumeError> {
    Ok(tokio::task::spawn_blocking(move || db.get_setting(&target.setting())).await??)
}

async fn write_setting(
    db: Database,
    target: Target,
    resume_at_epoch_secs: u64,
) -> Result<(), ScheduledResumeError> {
    tokio::task::spawn_blocking(move || {
        db.set_setting(&target.setting(), &resume_at_epoch_secs.to_string())
    })
    .await??;
    Ok(())
}

async fn delete_setting(db: Database, target: Target) -> Result<(), ScheduledResumeError> {
    tokio::task::spawn_blocking(move || db.delete_setting(&target.setting())).await??;
    Ok(())
}

async fn delete_setting_if_value(
    db: Database,
    target: Target,
    resume_at_epoch_secs: u64,
) -> Result<(), ScheduledResumeError> {
    tokio::task::spawn_blocking(move || {
        db.delete_setting_if_value(&target.setting(), &resume_at_epoch_secs.to_string())
    })
    .await??;
    Ok(())
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
