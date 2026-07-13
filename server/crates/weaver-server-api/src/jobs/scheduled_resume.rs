use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tracing::error;
use weaver_server_core::{Database, SchedulerError, SchedulerHandle, StateError};

const SCHEDULED_RESUME_SETTING: &str = "nzbget.scheduled_resume_at";

#[derive(Debug, thiserror::Error)]
pub enum ScheduledResumeError {
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    #[error(transparent)]
    State(#[from] StateError),
    #[error("scheduled-resume database task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
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
    resume_at_epoch_secs: Option<u64>,
}

impl ScheduledResumeState {
    fn invalidate(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.resume_at_epoch_secs = None;
    }

    fn arm(&mut self, resume_at_epoch_secs: u64) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.resume_at_epoch_secs = Some(resume_at_epoch_secs);
        self.generation
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
        self.inner
            .state
            .lock()
            .await
            .resume_at_epoch_secs
            .unwrap_or(0)
    }

    /// Restore the persisted NZBGet resume deadline once during server startup.
    pub async fn recover(&self) -> Result<(), ScheduledResumeError> {
        let Some(value) = Self::read_setting(self.inner.db.clone()).await? else {
            return Ok(());
        };
        let Ok(resume_at_epoch_secs) = value.parse::<u64>() else {
            Self::delete_setting(self.inner.db.clone()).await?;
            return Ok(());
        };

        if resume_at_epoch_secs <= unix_now_secs() {
            let mut state = self.inner.state.lock().await;
            state.invalidate();
            let resume_result = self.inner.handle.resume_all().await;
            let clear_result =
                Self::delete_setting_if_value(self.inner.db.clone(), resume_at_epoch_secs).await;
            resume_result?;
            clear_result?;
            return Ok(());
        }

        let generation = {
            let mut state = self.inner.state.lock().await;
            state.arm(resume_at_epoch_secs)
        };
        self.spawn_timer(resume_at_epoch_secs, generation);
        Ok(())
    }

    pub async fn schedule_resume(
        &self,
        resume_at_epoch_secs: u64,
    ) -> Result<(), ScheduledResumeError> {
        let generation = {
            let mut state = self.inner.state.lock().await;
            Self::write_setting(self.inner.db.clone(), resume_at_epoch_secs).await?;
            state.arm(resume_at_epoch_secs)
        };
        self.spawn_timer(resume_at_epoch_secs, generation);
        Ok(())
    }

    pub async fn pause_all(&self) -> Result<(), ScheduledResumeError> {
        self.set_paused(true).await
    }

    pub async fn resume_all(&self) -> Result<(), ScheduledResumeError> {
        self.set_paused(false).await
    }

    /// Serialize user-initiated pause/resume operations with NZBGet timer expiry.
    ///
    /// Core bandwidth schedule actions update the pipeline directly and deliberately
    /// bypass this API-layer coordinator. An armed NZBGet timer can therefore still
    /// supersede `ScheduleAction::Pause`; moving that ownership into core is outside
    /// this coordinator's scope.
    async fn set_paused(&self, paused: bool) -> Result<(), ScheduledResumeError> {
        let mut state = self.inner.state.lock().await;
        state.invalidate();
        let scheduler_result = if paused {
            self.inner.handle.pause_all().await
        } else {
            self.inner.handle.resume_all().await
        };
        let clear_result = Self::delete_setting(self.inner.db.clone()).await;
        scheduler_result?;
        clear_result
    }

    fn spawn_timer(&self, resume_at_epoch_secs: u64, generation: u64) {
        let coordinator = self.clone();
        tokio::spawn(async move {
            let delay = resume_at_epoch_secs.saturating_sub(unix_now_secs());
            tokio::time::sleep(Duration::from_secs(delay)).await;
            if let Err(error) = coordinator
                .resume_if_current(resume_at_epoch_secs, generation)
                .await
            {
                error!(%error, "scheduled NZBGet resume failed");
            }
        });
    }

    async fn resume_if_current(
        &self,
        resume_at_epoch_secs: u64,
        generation: u64,
    ) -> Result<(), ScheduledResumeError> {
        let mut state = self.inner.state.lock().await;
        if state.generation != generation
            || state.resume_at_epoch_secs != Some(resume_at_epoch_secs)
        {
            return Ok(());
        }

        state.invalidate();
        let resume_result = self.inner.handle.resume_all().await;
        let clear_result =
            Self::delete_setting_if_value(self.inner.db.clone(), resume_at_epoch_secs).await;
        resume_result?;
        clear_result
    }

    async fn read_setting(db: Database) -> Result<Option<String>, ScheduledResumeError> {
        Ok(tokio::task::spawn_blocking(move || db.get_setting(SCHEDULED_RESUME_SETTING)).await??)
    }

    async fn write_setting(
        db: Database,
        resume_at_epoch_secs: u64,
    ) -> Result<(), ScheduledResumeError> {
        tokio::task::spawn_blocking(move || {
            db.set_setting(SCHEDULED_RESUME_SETTING, &resume_at_epoch_secs.to_string())
        })
        .await??;
        Ok(())
    }

    async fn delete_setting(db: Database) -> Result<(), ScheduledResumeError> {
        tokio::task::spawn_blocking(move || db.delete_setting(SCHEDULED_RESUME_SETTING)).await??;
        Ok(())
    }

    async fn delete_setting_if_value(
        db: Database,
        resume_at_epoch_secs: u64,
    ) -> Result<(), ScheduledResumeError> {
        tokio::task::spawn_blocking(move || {
            db.delete_setting_if_value(SCHEDULED_RESUME_SETTING, &resume_at_epoch_secs.to_string())
        })
        .await??;
        Ok(())
    }
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
