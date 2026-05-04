use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

use tokio::sync::{RwLock, broadcast};

use crate::jobs::types::{
    PersistedQueueEvent, QueueDownloadState, QueueEvent, QueueEventKind,
    QueueItem, QueueItemState, QueuePostState, QueueWaitReason, encode_event_cursor,
    global_queue_state, queue_event_from_record, queue_item_from_job,
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
    last_progress_buckets: HashMap<u64, u8>,
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
                .insert(item.id, item.progress_percent.floor() as u8);
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

    if matches!(event, PipelineEvent::GlobalPaused | PipelineEvent::GlobalResumed) {
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
    let progress_bucket = item.progress_percent.floor() as u8;
    let attention_signature = queue_event_attention_signature(&item);
    let previous_item = caches.last_items.insert(job_id, item.clone());
    let previous_state = previous_item.as_ref().map(|value| value.state);
    let previous_detail = caches.last_item_details.insert(job_id, detail_signature);
    let previous_progress = caches.last_progress_buckets.insert(job_id, progress_bucket);
    let previous_attention = caches.last_attention.insert(job_id, attention_signature.clone());

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
        && progress_bucket > 0
        && previous_progress.is_none_or(|value| progress_bucket > value)
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

    if matches!(item.state, QueueItemState::Completed | QueueItemState::Failed) {
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
