use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

use tokio::sync::{RwLock, broadcast};

use crate::jobs::types::{
    PersistedQueueEvent, QueueDownloadState, QueueEvent, QueueEventKind, QueueItem, QueueItemState,
    QueuePhase, QueuePostState, QueueWaitReason, encode_event_cursor, global_queue_state,
    queue_event_from_record, queue_item_from_job,
};
use weaver_server_core::SchedulerHandle;
use weaver_server_core::events::model::PipelineEvent;
use weaver_server_core::events::publish::pipeline_job_id;
use weaver_server_core::settings::SharedConfig;

const QUEUE_EVENT_REPLAY_CAPACITY: usize = 2048;
const QUEUE_EVENT_CHANNEL_CAPACITY: usize = 256;

type DetailSignature = (
    QueueItemState,
    QueueDownloadState,
    QueuePostState,
    Option<QueueWaitReason>,
);
type AttentionSignature = Option<(String, String)>;
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgressSignature {
    overall_bucket: u8,
    phases: Vec<(QueuePhase, u8, bool)>,
}

#[derive(Clone)]
pub(crate) struct QueueEventReplay {
    inner: Arc<QueueEventReplayInner>,
}

struct QueueEventReplayInner {
    capacity: usize,
    state: RwLock<QueueEventReplayState>,
    sender: broadcast::Sender<ReplayNotification>,
}

#[derive(Clone)]
pub(crate) struct ReplayNotification {
    pub(crate) id: i64,
    pub(crate) event: QueueEvent,
}

struct QueueEventReplayState {
    latest_id: i64,
    events: VecDeque<ReplayNotification>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplayCursorExpired {
    oldest_id: i64,
    latest_id: i64,
}

impl fmt::Display for ReplayCursorExpired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cursor fell behind the bounded replay window (oldest={}, latest={})",
            encode_event_cursor(self.oldest_id.saturating_sub(1)),
            encode_event_cursor(self.latest_id),
        )
    }
}

impl Default for QueueEventReplay {
    fn default() -> Self {
        Self::new(QUEUE_EVENT_REPLAY_CAPACITY)
    }
}

impl QueueEventReplay {
    pub(crate) fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (sender, _) = broadcast::channel(QUEUE_EVENT_CHANNEL_CAPACITY);
        Self {
            inner: Arc::new(QueueEventReplayInner {
                capacity,
                state: RwLock::new(QueueEventReplayState {
                    latest_id: 0,
                    events: VecDeque::with_capacity(capacity),
                }),
                sender,
            }),
        }
    }

    pub(crate) fn spawn_producer(&self, handle: SchedulerHandle, config: SharedConfig) {
        let replay = self.clone();
        let rx = handle.subscribe_events();
        let mut caches = QueueEventCaches::default();
        caches.seed_from_handle(&handle);

        tokio::spawn(async move {
            if let Err(panic) = tokio::spawn(replay.run(rx, handle, config, caches)).await {
                tracing::error!(
                    error = %panic,
                    "queue event replay producer panicked - replayed queue events will stop"
                );
            }
        });
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<ReplayNotification> {
        self.inner.sender.subscribe()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub(crate) async fn latest_cursor(&self) -> String {
        let latest_id = self.inner.state.read().await.latest_id;
        encode_event_cursor(latest_id)
    }

    pub(crate) async fn replay_after(
        &self,
        after: Option<i64>,
    ) -> Result<Vec<ReplayNotification>, ReplayCursorExpired> {
        Ok(self
            .snapshot_events(after)
            .await?
            .iter()
            .filter(|notification| after.is_none_or(|cursor| notification.id > cursor))
            .cloned()
            .collect())
    }

    pub(crate) async fn replay_for_item(
        &self,
        item_id: u64,
        after: Option<i64>,
        limit: usize,
    ) -> Result<Vec<QueueEvent>, ReplayCursorExpired> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let events = self.snapshot_events(after).await?;
        Ok(events
            .into_iter()
            .filter(|notification| after.is_none_or(|cursor| notification.id > cursor))
            .filter(|notification| notification.event.item_id == Some(item_id))
            .take(limit)
            .map(|notification| notification.event)
            .collect())
    }

    pub(crate) async fn append(&self, record: PersistedQueueEvent) -> QueueEvent {
        let notification = {
            let mut state = self.inner.state.write().await;
            state.latest_id += 1;
            let id = state.latest_id;
            let notification = ReplayNotification {
                id,
                event: queue_event_from_record(id, record),
            };
            state.events.push_back(notification.clone());
            while state.events.len() > self.inner.capacity {
                state.events.pop_front();
            }
            notification
        };

        let _ = self.inner.sender.send(notification.clone());
        notification.event
    }

    async fn run(
        self,
        mut rx: broadcast::Receiver<PipelineEvent>,
        handle: SchedulerHandle,
        config: SharedConfig,
        mut caches: QueueEventCaches,
    ) {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    for record in queue_event_records_from_pipeline_event(
                        &event,
                        &handle,
                        &config,
                        &mut caches,
                    )
                    .await
                    {
                        self.append(record).await;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::debug!(
                        skipped,
                        "queue event replay producer lagged; reseeding local cache"
                    );
                    caches.seed_from_handle(&handle);
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    async fn snapshot_events(
        &self,
        after: Option<i64>,
    ) -> Result<Vec<ReplayNotification>, ReplayCursorExpired> {
        let state = self.inner.state.read().await;
        validate_cursor(&state, after)?;
        Ok(state.events.iter().cloned().collect())
    }
}

fn validate_cursor(
    state: &QueueEventReplayState,
    after: Option<i64>,
) -> Result<(), ReplayCursorExpired> {
    let Some(after) = after else {
        return Ok(());
    };
    let Some(oldest_id) = state.events.front().map(|notification| notification.id) else {
        return Ok(());
    };
    if after < oldest_id.saturating_sub(1) {
        return Err(ReplayCursorExpired {
            oldest_id,
            latest_id: state.latest_id,
        });
    }
    Ok(())
}

#[derive(Default)]
struct QueueEventCaches {
    last_items: HashMap<u64, QueueItem>,
    last_item_details: HashMap<u64, DetailSignature>,
    last_progress_buckets: HashMap<u64, ProgressSignature>,
    last_attention: HashMap<u64, AttentionSignature>,
}

impl QueueEventCaches {
    fn seed_from_handle(&mut self, handle: &SchedulerHandle) {
        self.last_items.clear();
        self.last_item_details.clear();
        self.last_progress_buckets.clear();
        self.last_attention.clear();

        for info in handle.list_jobs() {
            let item = queue_item_from_job(&info);
            self.last_item_details
                .insert(item.id, queue_event_detail_signature(&item));
            self.last_progress_buckets
                .insert(item.id, queue_event_progress_signature(&item));
            self.last_attention
                .insert(item.id, queue_event_attention_signature(&item));
            self.last_items.insert(item.id, item);
        }
    }

    fn evict(&mut self, item_id: u64) -> Option<QueueItem> {
        self.last_item_details.remove(&item_id);
        self.last_progress_buckets.remove(&item_id);
        self.last_attention.remove(&item_id);
        self.last_items.remove(&item_id)
    }
}

async fn queue_event_records_from_pipeline_event(
    event: &PipelineEvent,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    caches: &mut QueueEventCaches,
) -> Vec<PersistedQueueEvent> {
    let occurred_at_ms = chrono::Utc::now().timestamp_millis();
    let mut queue_events = Vec::new();

    if matches!(
        event,
        PipelineEvent::GlobalPaused | PipelineEvent::GlobalResumed
    ) {
        let cfg = config.read().await;
        queue_events.push(PersistedQueueEvent {
            occurred_at_ms,
            kind: QueueEventKind::GlobalStateChanged,
            item_id: None,
            item: None,
            state: None,
            previous_state: None,
            attention: None,
            global_state: Some(global_queue_state(
                handle.is_globally_paused(),
                &handle.get_download_block(),
                cfg.max_download_speed.unwrap_or(0),
            )),
        });
    }

    let Some(job_id) = pipeline_job_id(event) else {
        return queue_events;
    };

    if matches!(event, PipelineEvent::JobCancelled { .. }) {
        caches.evict(job_id);
        return queue_events;
    }

    let Ok(info) = handle.get_job(weaver_server_core::jobs::ids::JobId(job_id)) else {
        // The scheduler already purged this job, so the cached item is the only
        // payload left for it. Only a terminal outcome may consume that entry:
        // the pipeline emits non-terminal events (MoveToCompleteFinished,
        // DownloadFinished, phase updates) right before the terminal one, and
        // evicting on those would leave the JobCompleted/JobFailed behind them
        // with nothing to publish.
        if !matches!(
            event,
            PipelineEvent::JobCompleted { .. } | PipelineEvent::JobFailed { .. }
        ) {
            return queue_events;
        }
        if let Some(mut previous_item) = caches.evict(job_id) {
            match event {
                PipelineEvent::JobCompleted { .. } => {
                    let previous_state = previous_item.state;
                    previous_item.state = QueueItemState::Completed;
                    previous_item.wait_reason = None;
                    previous_item.attention = None;
                    queue_events.push(PersistedQueueEvent {
                        occurred_at_ms,
                        kind: QueueEventKind::ItemCompleted,
                        item_id: Some(job_id),
                        item: Some(previous_item),
                        state: Some(QueueItemState::Completed),
                        previous_state: Some(previous_state),
                        attention: None,
                        global_state: None,
                    });
                }
                PipelineEvent::JobFailed { error, .. } => {
                    let previous_state = previous_item.state;
                    previous_item.state = QueueItemState::Failed;
                    previous_item.error = Some(error.clone());
                    queue_events.push(PersistedQueueEvent {
                        occurred_at_ms,
                        kind: QueueEventKind::ItemStateChanged,
                        item_id: Some(job_id),
                        item: Some(previous_item.clone()),
                        state: Some(QueueItemState::Failed),
                        previous_state: Some(previous_state),
                        attention: previous_item.attention.clone(),
                        global_state: None,
                    });
                }
                _ => {}
            }
        }
        return queue_events;
    };

    let item = queue_item_from_job(&info);
    let detail_signature = queue_event_detail_signature(&item);
    let progress_signature = queue_event_progress_signature(&item);
    let attention_signature = queue_event_attention_signature(&item);
    let previous_item = caches.last_items.insert(job_id, item.clone());
    let previous_state = previous_item.as_ref().map(|value| value.state);
    let previous_detail = caches.last_item_details.insert(job_id, detail_signature);
    let previous_progress = caches
        .last_progress_buckets
        .insert(job_id, progress_signature.clone());
    let previous_attention = caches
        .last_attention
        .insert(job_id, attention_signature.clone());

    if matches!(event, PipelineEvent::JobCreated { .. }) {
        queue_events.push(PersistedQueueEvent {
            occurred_at_ms,
            kind: QueueEventKind::ItemCreated,
            item_id: Some(job_id),
            item: Some(item.clone()),
            state: Some(item.state),
            previous_state,
            attention: item.attention.clone(),
            global_state: None,
        });
    }

    if let Some(previous_detail) = previous_detail
        && previous_detail != detail_signature
    {
        debug_assert_eq!(previous_state, Some(previous_detail.0));
        queue_events.push(PersistedQueueEvent {
            occurred_at_ms,
            kind: if item.state == QueueItemState::Completed {
                QueueEventKind::ItemCompleted
            } else {
                QueueEventKind::ItemStateChanged
            },
            item_id: Some(job_id),
            item: Some(item.clone()),
            state: Some(item.state),
            previous_state,
            attention: item.attention.clone(),
            global_state: None,
        });
    }

    if item.state != QueueItemState::Completed
        && item.state != QueueItemState::Failed
        && progress_signature_has_progress(&progress_signature)
        && previous_progress.is_none_or(|value| value != progress_signature)
    {
        queue_events.push(PersistedQueueEvent {
            occurred_at_ms,
            kind: QueueEventKind::ItemProgress,
            item_id: Some(job_id),
            item: Some(item.clone()),
            state: Some(item.state),
            previous_state: None,
            attention: None,
            global_state: None,
        });
    }

    if attention_signature.is_some() && attention_signature != previous_attention.flatten() {
        queue_events.push(PersistedQueueEvent {
            occurred_at_ms,
            kind: QueueEventKind::ItemAttention,
            item_id: Some(job_id),
            item: Some(item.clone()),
            state: Some(item.state),
            previous_state: None,
            attention: item.attention.clone(),
            global_state: None,
        });
    }

    if matches!(
        item.state,
        QueueItemState::Completed | QueueItemState::Failed
    ) {
        caches.evict(job_id);
    }

    queue_events
}

fn queue_event_detail_signature(item: &QueueItem) -> DetailSignature {
    (
        item.state,
        item.download_state,
        item.post_state,
        item.wait_reason,
    )
}

fn queue_event_attention_signature(item: &QueueItem) -> AttentionSignature {
    item.attention
        .as_ref()
        .map(|value| (value.code.clone(), value.message.clone()))
}

fn queue_event_progress_signature(item: &QueueItem) -> ProgressSignature {
    let mut phases = item
        .phase_progress
        .iter()
        .map(|phase| {
            (
                phase.phase,
                phase.progress_percent.floor().clamp(0.0, 100.0) as u8,
                phase.rate_bps.is_some(),
            )
        })
        .collect::<Vec<_>>();
    phases.sort_by_key(|(phase, _, _)| *phase as u8);
    ProgressSignature {
        overall_bucket: item.progress_percent.floor().clamp(0.0, 100.0) as u8,
        phases,
    }
}

fn progress_signature_has_progress(signature: &ProgressSignature) -> bool {
    signature.overall_bucket > 0 || !signature.phases.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::sync::mpsc;
    use weaver_server_core::jobs::handle::{JobInfo, SharedPipelineState};
    use weaver_server_core::jobs::ids::JobId;
    use weaver_server_core::operations::metrics::PipelineMetrics;
    use weaver_server_core::settings::Config;
    use weaver_server_core::{DownloadState, JobStatus, PostState, RunState, SchedulerCommand};

    const PURGED_JOB_ID: u64 = 4242;

    fn test_config() -> SharedConfig {
        Arc::new(RwLock::new(Config {
            data_dir: "/tmp/weaver".to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: weaver_server_core::jobs::DuplicatePolicy::default(),
            direct_store: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        }))
    }

    fn finalizing_job_info() -> JobInfo {
        JobInfo {
            job_id: JobId(PURGED_JOB_ID),
            job_hash: None,
            name: "Purged Completion".to_string(),
            error: None,
            download_wait_reason: None,
            download_retry_at_epoch_ms: None,
            status: JobStatus::Moving,
            download_state: DownloadState::Complete,
            finalizing_download: false,
            fetching_repair_data: false,
            post_state: PostState::Finalizing,
            run_state: RunState::Active,
            progress: 100.0,
            total_bytes: 1024,
            downloaded_bytes: 1024,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            phase_progress: Vec::new(),
            failed_bytes: 0,
            health: 1000,
            terminal_discards: Vec::new(),
            total_files: 1,
            completed_files: 1,
            remaining_par_files: 0,
            password: None,
            category: None,
            metadata: Vec::new(),
            output_dir: None,
            created_at_epoch_ms: 0.0,
        }
    }

    /// Seed the replay caches from a live finalizing job, then purge it from the
    /// scheduler the way the pipeline does before its terminal event lands.
    fn purged_job_fixture() -> (SchedulerHandle, SharedConfig, QueueEventCaches) {
        let (cmd_tx, _cmd_rx) = mpsc::channel::<SchedulerCommand>(4);
        let (event_tx, _event_rx) = broadcast::channel(4);
        let state = SharedPipelineState::new(PipelineMetrics::new(), vec![finalizing_job_info()]);
        let handle = SchedulerHandle::new(cmd_tx, event_tx, state.clone());

        let mut caches = QueueEventCaches::default();
        caches.seed_from_handle(&handle);
        state.publish_jobs(Vec::new());

        (handle, test_config(), caches)
    }

    fn completed_job_info() -> JobInfo {
        let status = JobStatus::Complete;
        let (download_state, post_state, run_state) =
            weaver_server_core::runtime_lanes_from_status_snapshot(&status);
        JobInfo {
            status,
            download_state,
            post_state,
            run_state,
            ..finalizing_job_info()
        }
    }

    /// The ordering production actually takes: `record_job_history` moves the
    /// job into `finished_jobs` and republishes the snapshot *before* the
    /// terminal event is released, so the job is still resolvable — the
    /// evicted-cache path is only the fallback.
    #[tokio::test]
    async fn completed_job_in_finished_snapshot_emits_item_completed() {
        let (cmd_tx, _cmd_rx) = mpsc::channel::<SchedulerCommand>(4);
        let (event_tx, _event_rx) = broadcast::channel(4);
        let state = SharedPipelineState::new(PipelineMetrics::new(), vec![finalizing_job_info()]);
        let handle = SchedulerHandle::new(cmd_tx, event_tx, state.clone());
        let config = test_config();

        let mut caches = QueueEventCaches::default();
        caches.seed_from_handle(&handle);
        state.publish_jobs(vec![completed_job_info()]);

        let records = queue_event_records_from_pipeline_event(
            &PipelineEvent::JobCompleted {
                job_id: JobId(PURGED_JOB_ID),
            },
            &handle,
            &config,
            &mut caches,
        )
        .await;

        assert_eq!(records.len(), 1, "{records:?}");
        let record = &records[0];
        assert_eq!(record.kind, QueueEventKind::ItemCompleted);
        assert_eq!(record.item_id, Some(PURGED_JOB_ID));
        assert_eq!(record.state, Some(QueueItemState::Completed));
        let item = record
            .item
            .as_ref()
            .expect("completed queue event must carry an item payload");
        assert_eq!(item.id, PURGED_JOB_ID);
        assert_eq!(item.state, QueueItemState::Completed);
    }

    async fn assert_item_completed_survives(intermediate: PipelineEvent) {
        let (handle, config, mut caches) = purged_job_fixture();

        let intermediate_records =
            queue_event_records_from_pipeline_event(&intermediate, &handle, &config, &mut caches)
                .await;
        assert!(
            intermediate_records.is_empty(),
            "intermediate event for a purged job should not publish a queue event"
        );

        let records = queue_event_records_from_pipeline_event(
            &PipelineEvent::JobCompleted {
                job_id: JobId(PURGED_JOB_ID),
            },
            &handle,
            &config,
            &mut caches,
        )
        .await;

        assert_eq!(records.len(), 1, "{records:?}");
        let record = &records[0];
        assert_eq!(record.kind, QueueEventKind::ItemCompleted);
        assert_eq!(record.item_id, Some(PURGED_JOB_ID));
        assert_eq!(record.state, Some(QueueItemState::Completed));
        let item = record
            .item
            .as_ref()
            .expect("completed queue event must carry an item payload");
        assert_eq!(item.id, PURGED_JOB_ID);
        assert_eq!(item.state, QueueItemState::Completed);
    }

    #[tokio::test]
    async fn move_to_complete_finished_keeps_item_completed_for_purged_job() {
        assert_item_completed_survives(PipelineEvent::MoveToCompleteFinished {
            job_id: JobId(PURGED_JOB_ID),
        })
        .await;
    }

    #[tokio::test]
    async fn download_finished_keeps_item_completed_for_purged_job() {
        assert_item_completed_survives(PipelineEvent::DownloadFinished {
            job_id: JobId(PURGED_JOB_ID),
            finalization_pending: false,
        })
        .await;
    }

    #[tokio::test]
    async fn intermediate_event_keeps_failed_state_change_for_purged_job() {
        let (handle, config, mut caches) = purged_job_fixture();

        let intermediate_records = queue_event_records_from_pipeline_event(
            &PipelineEvent::MoveToCompleteFinished {
                job_id: JobId(PURGED_JOB_ID),
            },
            &handle,
            &config,
            &mut caches,
        )
        .await;
        assert!(intermediate_records.is_empty());

        let records = queue_event_records_from_pipeline_event(
            &PipelineEvent::JobFailed {
                job_id: JobId(PURGED_JOB_ID),
                error: "boom".to_string(),
            },
            &handle,
            &config,
            &mut caches,
        )
        .await;

        assert_eq!(records.len(), 1, "{records:?}");
        let record = &records[0];
        assert_eq!(record.kind, QueueEventKind::ItemStateChanged);
        assert_eq!(record.item_id, Some(PURGED_JOB_ID));
        assert_eq!(record.state, Some(QueueItemState::Failed));
        let item = record
            .item
            .as_ref()
            .expect("failed queue event must carry an item payload");
        assert_eq!(item.id, PURGED_JOB_ID);
        assert_eq!(item.state, QueueItemState::Failed);
        assert_eq!(item.error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn terminal_event_still_evicts_the_cached_item() {
        let (handle, config, mut caches) = purged_job_fixture();

        let first = queue_event_records_from_pipeline_event(
            &PipelineEvent::JobCompleted {
                job_id: JobId(PURGED_JOB_ID),
            },
            &handle,
            &config,
            &mut caches,
        )
        .await;
        assert_eq!(first.len(), 1);

        let replayed = queue_event_records_from_pipeline_event(
            &PipelineEvent::JobCompleted {
                job_id: JobId(PURGED_JOB_ID),
            },
            &handle,
            &config,
            &mut caches,
        )
        .await;
        assert!(
            replayed.is_empty(),
            "a terminal event must publish once, not per repeat"
        );
    }
}
