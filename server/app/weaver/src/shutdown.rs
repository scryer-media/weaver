use tracing::error;

use weaver_server_core::{Database, SchedulerHandle};

pub(crate) async fn wait_for_shutdown() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
    }
}

pub(crate) fn pipeline_exit_error(result: Result<(), tokio::task::JoinError>) -> std::io::Error {
    match result {
        Ok(()) => {
            error!("pipeline task exited unexpectedly");
            std::io::Error::other("pipeline task exited unexpectedly")
        }
        Err(join_error) => {
            error!(error = %join_error, "pipeline task exited unexpectedly");
            std::io::Error::other(format!("pipeline task exited unexpectedly: {join_error}"))
        }
    }
}

pub(crate) fn spawn_metrics_history_task(
    handle: SchedulerHandle,
    db: Database,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let metrics = handle.get_metrics();
            let jobs = handle.list_jobs();
            let db = db.clone();
            let recorded_at_epoch_sec = epoch_sec_now();
            match tokio::task::spawn_blocking(move || {
                db.record_metrics_history_sample(recorded_at_epoch_sec, &metrics, &jobs)
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "failed to persist metrics history sample");
                }
                Err(join_error) => {
                    tracing::warn!(
                        error = %join_error,
                        "metrics history persistence task failed"
                    );
                }
            }
        }
    })
}

fn epoch_sec_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
