use std::sync::Arc;

use weaver_nntp::transfer::ServerTransferControl;

use super::http::{HttpLeg, HttpOptions, routed_get};
use super::progress::{LegRecorder, Progress};
use super::runner::Stop;

// One stream of a bandwidth run.
pub(crate) struct BandwidthStream {
    pub leg: HttpLeg,
    pub recorder: LegRecorder,
    // The live egress control the stream's bytes are booked against.
    pub egress_control: Option<Arc<ServerTransferControl>>,
}

pub(crate) async fn run(
    streams: Vec<BandwidthStream>,
    url: reqwest::Url,
    bytes: u64,
    options: HttpOptions,
    progress: Arc<Progress>,
    stop: Stop,
) {
    let mut tasks = tokio::task::JoinSet::new();
    for stream in streams {
        tasks.spawn(run_stream(
            stream,
            url.clone(),
            bytes,
            options.clone(),
            progress.clone(),
            stop.clone(),
        ));
    }
    while tasks.join_next().await.is_some() {}
}

async fn run_stream(
    stream: BandwidthStream,
    url: reqwest::Url,
    bytes: u64,
    options: HttpOptions,
    progress: Arc<Progress>,
    mut stop: Stop,
) {
    let recorder = stream.recorder.clone();
    let mut permit = match &stream.egress_control {
        Some(control) => match control.try_reserve(bytes) {
            Ok(permit) => Some(permit),
            Err(_) => {
                progress.end_leg_connection(
                    recorder.leg,
                    recorder.egress,
                    format!("egress {} has used its download quota", recorder.egress),
                );
                return;
            }
        },
        None => None,
    };
    let request = routed_get(&stream.leg, &url, &options, Some(recorder.clone()));
    let mut routed = tokio::select! {
        biased;
        _ = stop.stopped() => return,
        routed = request => match routed {
            Ok(routed) => routed,
            Err((provider, error)) => {
                match provider {
                    Some(provider) if provider.dialed_any() => provider.fail(error),
                    _ => progress.end_leg_connection(recorder.leg, recorder.egress, error),
                }
                return;
            }
        },
    };
    loop {
        let chunk = tokio::select! {
            biased;
            _ = stop.stopped() => break,
            chunk = routed.response.chunk() => chunk,
        };
        match chunk {
            Ok(Some(chunk)) => {
                let n = chunk.len();
                progress.add_leg_bytes(recorder.leg, n as u64);
                routed.provider.add_bytes(n as u64);
                if let Some(permit) = &mut permit {
                    permit.record_unpaced(n);
                }
            }
            Ok(None) => break,
            Err(error) => {
                routed
                    .provider
                    .fail(format!("reading the body failed: {error}"));
                break;
            }
        }
    }
    routed.close().await;
    progress.counted();
}
