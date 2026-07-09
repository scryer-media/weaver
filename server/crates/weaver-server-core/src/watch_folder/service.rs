use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use notify::{RecursiveMode, Watcher};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::settings::SharedConfig;
use crate::{Database, SchedulerHandle, StateError};

use super::{
    WatchFolderConfig, WatchFolderMode, WatchFolderScanReport, WatchFolderScanner,
    WatchFolderScannerError,
};

const PROCESSING_PREFIX: &str = ".weaver-processing-";
const MARKER_SUFFIXES: [&str; 4] = [".queued", ".error", ".partial", ".processed"];

#[derive(Debug, thiserror::Error)]
pub enum WatchFolderServiceError {
    #[error("state error: {0}")]
    State(#[from] StateError),
    #[error("watcher error: {0}")]
    Watcher(#[from] notify::Error),
    #[error("scan error: {0}")]
    Scanner(#[from] WatchFolderScannerError),
}

#[derive(Clone)]
pub struct WatchFolderService {
    inner: Arc<WatchFolderServiceInner>,
}

struct WatchFolderServiceInner {
    db: Database,
    config: SharedConfig,
    scanner: WatchFolderScanner,
    runtime: Mutex<WatchFolderRuntime>,
}

#[derive(Default)]
struct WatchFolderRuntime {
    task: Option<tokio::task::JoinHandle<()>>,
    active_mode: Option<WatchFolderMode>,
}

impl WatchFolderService {
    pub fn new(db: Database, handle: SchedulerHandle, config: SharedConfig) -> Self {
        let scanner = WatchFolderScanner::new(db.clone(), handle, config.clone());
        Self {
            inner: Arc::new(WatchFolderServiceInner {
                db,
                config,
                scanner,
                runtime: Mutex::new(WatchFolderRuntime::default()),
            }),
        }
    }

    pub async fn reconcile_from_config(&self) -> Result<(), WatchFolderServiceError> {
        let settings = {
            let cfg = self.inner.config.read().await;
            cfg.watch_folder.clone()
        };
        self.configure(settings).await
    }

    pub async fn stop(&self) {
        let mut runtime = self.inner.runtime.lock().await;
        stop_runtime(&mut runtime);
    }

    pub async fn scan_now(&self) -> Result<WatchFolderScanReport, WatchFolderServiceError> {
        let settings = {
            let cfg = self.inner.config.read().await;
            cfg.watch_folder.clone()
        };
        self.inner
            .scanner
            .scan_once(settings)
            .await
            .map_err(Into::into)
    }

    pub async fn set_scanning_paused(&self, paused: bool) -> Result<(), WatchFolderServiceError> {
        let db = self.inner.db.clone();
        tokio::task::spawn_blocking(move || {
            db.set_setting("watch_folder.scanning_paused", &paused.to_string())
        })
        .await
        .map_err(|error| StateError::Database(error.to_string()))??;

        {
            let mut cfg = self.inner.config.write().await;
            cfg.watch_folder.scanning_paused = paused;
        }
        self.reconcile_from_config().await
    }

    pub async fn configure(
        &self,
        settings: WatchFolderConfig,
    ) -> Result<(), WatchFolderServiceError> {
        let mut runtime = self.inner.runtime.lock().await;
        stop_runtime(&mut runtime);

        if !settings.automatic_scanning_enabled() {
            debug!(
                mode = settings.mode.as_str(),
                paused = settings.scanning_paused,
                "watch folder automatic scanning disabled"
            );
            return Ok(());
        }

        match settings.mode {
            WatchFolderMode::Off => Ok(()),
            WatchFolderMode::Polling => {
                let task = spawn_polling_task(self.inner.scanner.clone(), settings);
                runtime.task = Some(task);
                runtime.active_mode = Some(WatchFolderMode::Polling);
                info!("watch folder polling started");
                Ok(())
            }
            WatchFolderMode::Realtime => {
                let task = spawn_realtime_task(
                    self.inner.scanner.clone(),
                    self.inner.config.clone(),
                    settings,
                );
                runtime.task = Some(task);
                runtime.active_mode = Some(WatchFolderMode::Realtime);
                info!("watch folder realtime watcher started");
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub async fn active_mode_for_tests(&self) -> Option<WatchFolderMode> {
        self.inner.runtime.lock().await.active_mode
    }
}

fn stop_runtime(runtime: &mut WatchFolderRuntime) {
    if let Some(task) = runtime.task.take() {
        task.abort();
    }
    runtime.active_mode = None;
}

fn spawn_polling_task(
    scanner: WatchFolderScanner,
    settings: WatchFolderConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(settings.poll_interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            match scanner.scan_once(settings.clone()).await {
                Ok(report) => log_scan_report("polling", &report),
                Err(error) => warn!(error = %error, "watch folder polling scan failed"),
            }
        }
    })
}

fn spawn_realtime_task(
    scanner: WatchFolderScanner,
    config: SharedConfig,
    settings: WatchFolderConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(error) =
                run_realtime_watch_cycle(scanner.clone(), config.clone(), settings.clone()).await
            {
                warn!(error = %error, "watch folder realtime watcher setup failed; retrying");
            }
            tokio::time::sleep(realtime_retry_delay(&settings)).await;
        }
    })
}

async fn run_realtime_watch_cycle(
    scanner: WatchFolderScanner,
    config: SharedConfig,
    settings: WatchFolderConfig,
) -> notify::Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut watcher = notify::recommended_watcher(move |event| {
        let _ = enqueue_scan_signal(&tx, event);
    })?;
    let mut watched_paths = HashSet::new();

    refresh_watch_paths(&mut watcher, &mut watched_paths, &config, &settings).await?;

    match scanner.scan_once(settings.clone()).await {
        Ok(report) => log_scan_report("realtime-startup", &report),
        Err(error) => warn!(error = %error, "watch folder startup scan failed"),
    }

    let mut fallback_interval = tokio::time::interval(realtime_fallback_interval(&settings));
    fallback_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    fallback_interval.tick().await;

    loop {
        tokio::select! {
            signal = rx.recv() => {
                let Some(()) = signal else {
                    return Ok(());
                };
                tokio::time::sleep(Duration::from_millis(500)).await;
                while rx.try_recv().is_ok() {
                }
                refresh_watch_paths(&mut watcher, &mut watched_paths, &config, &settings).await?;
                match scanner.scan_once(settings.clone()).await {
                    Ok(report) => log_scan_report("realtime", &report),
                    Err(error) => warn!(error = %error, "watch folder realtime scan failed"),
                }
            }
            _ = fallback_interval.tick() => {
                refresh_watch_paths(&mut watcher, &mut watched_paths, &config, &settings).await?;
                match scanner.scan_once(settings.clone()).await {
                    Ok(report) => log_scan_report("realtime-reconcile", &report),
                    Err(error) => warn!(error = %error, "watch folder realtime reconciliation scan failed"),
                }
            }
        }
    }
}

async fn refresh_watch_paths(
    watcher: &mut notify::RecommendedWatcher,
    watched_paths: &mut HashSet<PathBuf>,
    config: &SharedConfig,
    settings: &WatchFolderConfig,
) -> notify::Result<()> {
    let desired_paths = watch_paths(config, settings).await;
    let desired_set = desired_paths.iter().cloned().collect::<HashSet<_>>();
    let stale_paths = watched_paths
        .iter()
        .filter(|path| !desired_set.contains(*path) || !path.exists())
        .cloned()
        .collect::<Vec<_>>();
    for path in stale_paths {
        if let Err(error) = watcher.unwatch(&path) {
            debug!(path = %path.display(), error = %error, "failed to unwatch stale watch folder path");
        }
        watched_paths.remove(&path);
    }

    for (idx, path) in desired_paths.into_iter().enumerate() {
        if watched_paths.contains(&path) {
            continue;
        }
        match watcher.watch(&path, RecursiveMode::NonRecursive) {
            Ok(()) => {
                watched_paths.insert(path);
            }
            Err(error) if idx == 0 => return Err(error),
            Err(error) => {
                warn!(path = %path.display(), error = %error, "failed to watch category folder");
            }
        }
    }
    Ok(())
}

fn realtime_retry_delay(settings: &WatchFolderConfig) -> Duration {
    Duration::from_secs(settings.poll_interval_secs.clamp(5, 60))
}

fn realtime_fallback_interval(settings: &WatchFolderConfig) -> Duration {
    Duration::from_secs(settings.poll_interval_secs.clamp(30, 300))
}

fn event_result_requests_scan(event: notify::Result<notify::Event>) -> bool {
    match event {
        Ok(event) => watch_event_requests_scan(&event),
        Err(error) => {
            warn!(error = %error, "watch folder realtime event failed");
            true
        }
    }
}

fn enqueue_scan_signal(
    tx: &tokio::sync::mpsc::Sender<()>,
    event: notify::Result<notify::Event>,
) -> bool {
    event_result_requests_scan(event) && tx.try_send(()).is_ok()
}

fn watch_event_requests_scan(event: &notify::Event) -> bool {
    event.paths.is_empty()
        || event
            .paths
            .iter()
            .any(|path| !is_ignored_watch_event_path(path))
}

fn is_ignored_watch_event_path(path: &std::path::Path) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let lower = name.to_ascii_lowercase();
    name.starts_with(PROCESSING_PREFIX)
        || MARKER_SUFFIXES.iter().any(|suffix| lower.ends_with(suffix))
}

async fn watch_paths(config: &SharedConfig, settings: &WatchFolderConfig) -> Vec<PathBuf> {
    let Some(root) = settings.normalized_path().map(PathBuf::from) else {
        return Vec::new();
    };
    let mut paths = vec![root.clone()];
    if settings.category_from_subfolders {
        let categories = {
            let cfg = config.read().await;
            cfg.categories.clone()
        };
        for category in categories {
            let path = root.join(category.name);
            if path.is_dir() {
                paths.push(path);
            }
        }
    }
    paths
}

fn log_scan_report(source: &str, report: &WatchFolderScanReport) {
    if report.discovered_files.is_empty()
        && report.queued_nzbs.is_empty()
        && report.permanent_errors.is_empty()
        && report.transient_errors.is_empty()
    {
        return;
    }
    info!(
        source,
        discovered = report.discovered_files.len(),
        queued = report.queued_nzbs.len(),
        skipped = report.skipped_inputs.len(),
        permanent_errors = report.permanent_errors.len(),
        transient_errors = report.transient_errors.len(),
        markers = report.marker_renamed_sources.len(),
        "watch folder scan completed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::Config;
    use crate::{PipelineMetrics, SharedPipelineState};
    use tokio::sync::{broadcast, mpsc};

    fn test_scheduler_handle() -> SchedulerHandle {
        let (cmd_tx, _cmd_rx) = mpsc::channel(16);
        let (event_tx, _) = broadcast::channel(16);
        let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
        SchedulerHandle::new(cmd_tx, event_tx, state)
    }

    fn shared_config(watch_folder: WatchFolderConfig) -> SharedConfig {
        Arc::new(tokio::sync::RwLock::new(Config {
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
            watch_folder,
            config_path: None,
        }))
    }

    #[tokio::test]
    async fn off_mode_spawns_no_runtime() {
        let service = WatchFolderService::new(
            Database::open_in_memory().unwrap(),
            test_scheduler_handle(),
            shared_config(WatchFolderConfig::default()),
        );

        service.reconcile_from_config().await.unwrap();

        assert_eq!(service.active_mode_for_tests().await, None);
    }

    #[tokio::test]
    async fn polling_mode_spawns_polling_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = WatchFolderConfig {
            mode: WatchFolderMode::Polling,
            path: Some(temp.path().display().to_string()),
            poll_interval_secs: 60,
            stability_secs: 0,
            category_from_subfolders: true,
            scanning_paused: false,
        };
        let service = WatchFolderService::new(
            Database::open_in_memory().unwrap(),
            test_scheduler_handle(),
            shared_config(cfg),
        );

        service.reconcile_from_config().await.unwrap();

        assert_eq!(
            service.active_mode_for_tests().await,
            Some(WatchFolderMode::Polling)
        );
    }

    #[tokio::test]
    async fn realtime_missing_path_does_not_fail_reconcile() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("not-mounted");
        let cfg = WatchFolderConfig {
            mode: WatchFolderMode::Realtime,
            path: Some(missing.display().to_string()),
            poll_interval_secs: 60,
            stability_secs: 0,
            category_from_subfolders: true,
            scanning_paused: false,
        };
        let service = WatchFolderService::new(
            Database::open_in_memory().unwrap(),
            test_scheduler_handle(),
            shared_config(cfg),
        );

        service.reconcile_from_config().await.unwrap();

        assert_eq!(
            service.active_mode_for_tests().await,
            Some(WatchFolderMode::Realtime)
        );
        service.stop().await;
    }

    #[tokio::test]
    async fn pause_stops_automatic_runtime_without_changing_mode() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = WatchFolderConfig {
            mode: WatchFolderMode::Polling,
            path: Some(temp.path().display().to_string()),
            poll_interval_secs: 60,
            stability_secs: 0,
            category_from_subfolders: true,
            scanning_paused: false,
        };
        let shared = shared_config(cfg);
        let service = WatchFolderService::new(
            Database::open_in_memory().unwrap(),
            test_scheduler_handle(),
            shared.clone(),
        );
        service.reconcile_from_config().await.unwrap();

        service.set_scanning_paused(true).await.unwrap();

        assert_eq!(service.active_mode_for_tests().await, None);
        assert_eq!(
            shared.read().await.watch_folder.mode,
            WatchFolderMode::Polling
        );
        assert!(shared.read().await.watch_folder.scanning_paused);
    }

    #[test]
    fn realtime_fallback_interval_clamps_to_slow_reconcile_range() {
        let mut cfg = WatchFolderConfig {
            poll_interval_secs: 1,
            ..WatchFolderConfig::default()
        };
        assert_eq!(realtime_fallback_interval(&cfg), Duration::from_secs(30));

        cfg.poll_interval_secs = 120;
        assert_eq!(realtime_fallback_interval(&cfg), Duration::from_secs(120));

        cfg.poll_interval_secs = 999;
        assert_eq!(realtime_fallback_interval(&cfg), Duration::from_secs(300));
    }

    #[test]
    fn realtime_event_filter_ignores_self_generated_marker_and_claim_paths() {
        let marker = notify::Event::new(notify::EventKind::Any)
            .add_path(PathBuf::from("/watch/release.nzb.queued"));
        let claim = notify::Event::new(notify::EventKind::Any)
            .add_path(PathBuf::from("/watch/.weaver-processing-0-release.nzb"));
        let candidate =
            notify::Event::new(notify::EventKind::Any).add_path(PathBuf::from("/watch/new.nzb"));
        let mixed = notify::Event::new(notify::EventKind::Any)
            .add_path(PathBuf::from("/watch/release.nzb.queued"))
            .add_path(PathBuf::from("/watch/new.nzb"));

        assert!(!watch_event_requests_scan(&marker));
        assert!(!watch_event_requests_scan(&claim));
        assert!(watch_event_requests_scan(&candidate));
        assert!(watch_event_requests_scan(&mixed));
    }

    #[test]
    fn realtime_scan_signal_channel_filters_and_coalesces_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let marker = notify::Event::new(notify::EventKind::Any)
            .add_path(PathBuf::from("/watch/release.nzb.queued"));
        let first =
            notify::Event::new(notify::EventKind::Any).add_path(PathBuf::from("/watch/first.nzb"));
        let second =
            notify::Event::new(notify::EventKind::Any).add_path(PathBuf::from("/watch/second.nzb"));

        assert!(!enqueue_scan_signal(&tx, Ok(marker)));
        assert!(rx.try_recv().is_err());

        assert!(enqueue_scan_signal(&tx, Ok(first)));
        assert!(!enqueue_scan_signal(&tx, Ok(second)));
        assert_eq!(rx.try_recv().unwrap(), ());
        assert!(rx.try_recv().is_err());
    }
}
