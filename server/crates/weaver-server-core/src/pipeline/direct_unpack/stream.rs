use std::sync::{Arc, Mutex};

use crate::jobs::ids::JobId;
use crate::pipeline::extraction::output::channel::StreamOwner;
use crate::pipeline::extraction::runtime::{RequestId, decoded::DecodedState, root::RootDispatch};
use crate::pipeline::{ExtractionDone, ExtractionError, FullSetExtractionOutcome, Pipeline};

#[derive(Clone, Default)]
pub(crate) struct ChaseManifest {
    streamed: Arc<Mutex<std::collections::HashSet<String>>>,
}
impl ChaseManifest {
    pub(super) fn route(&self, path: &std::path::Path) -> bool {
        if !archive_member(path) {
            return false;
        }
        self.streamed
            .lock()
            .expect("chase manifest")
            .insert(path.to_string_lossy().replace('\\', "/"));
        true
    }
    pub(crate) fn streamed(&self) -> std::collections::HashSet<String> {
        self.streamed.lock().expect("chase manifest").clone()
    }
}
pub(crate) struct ChaseExpectedMembers {
    pub(crate) declared: std::collections::HashSet<String>,
    pub(crate) manifest: ChaseManifest,
}
impl ChaseExpectedMembers {
    pub(crate) fn matches(&self, ordinary: &[String]) -> bool {
        let mut names = self.manifest.streamed();
        names.extend(ordinary.iter().cloned());
        names == self.declared
    }
}

type Collected = (
    Result<FullSetExtractionOutcome, ExtractionError>,
    Option<Box<DecodedState>>,
);

#[derive(Default)]
struct State {
    accepted: bool,
    discarded: bool,
    collected: Option<Collected>,
}

#[derive(Clone)]
pub(super) struct ChaseStream {
    pub(super) request: RequestId,
    owner: StreamOwner,
    manifest: ChaseManifest,
    staging: std::path::PathBuf,
    job_id: JobId,
    state: Arc<Mutex<State>>,
    sender: tokio::sync::mpsc::Sender<ExtractionDone>,
    lease: Arc<Mutex<Option<crate::pipeline::extraction::session::SessionLease>>>,
}

pub(super) fn archive_member(path: &std::path::Path) -> bool {
    matches!(
        weaver_model::files::FileRole::from_filename(&path.to_string_lossy()),
        weaver_model::files::FileRole::RarVolume { .. }
            | weaver_model::files::FileRole::SevenZipArchive
            | weaver_model::files::FileRole::SevenZipSplit { .. }
    )
}

impl State {
    fn take_ready(&mut self) -> Option<Collected> {
        if self.accepted && !self.discarded {
            self.collected.take()
        } else {
            None
        }
    }
}

impl ChaseStream {
    pub(super) fn new(
        job_id: JobId,
        dispatch: &RootDispatch,
        sender: tokio::sync::mpsc::Sender<ExtractionDone>,
        manifest: ChaseManifest,
        staging: std::path::PathBuf,
    ) -> Self {
        Self {
            request: dispatch.request.clone(),
            owner: dispatch.owner,
            manifest,
            staging,
            job_id,
            state: Arc::default(),
            sender,
            lease: Arc::new(Mutex::new(Some(dispatch.lease()))),
        }
    }

    pub(super) fn collect(
        &self,
        dispatch: RootDispatch,
        worker: tokio::task::JoinHandle<Result<FullSetExtractionOutcome, String>>,
    ) -> tokio::task::JoinHandle<Result<FullSetExtractionOutcome, String>> {
        let stream = self.clone();
        tokio::spawn(async move {
            let worker = tokio::spawn(async move {
                worker
                    .await
                    .map_err(|error| ExtractionError::terminal(error.to_string()))?
                    .map_err(ExtractionError::recoverable)
            });
            let (mut result, decoded) = dispatch.collect(worker).await;
            if let Ok(outcome) = &mut result {
                let streamed = stream.manifest.streamed();
                outcome
                    .extracted
                    .retain(|name| !streamed.contains(&name.replace('\\', "/")));
            }
            let scoped = result
                .as_ref()
                .map(|outcome| FullSetExtractionOutcome {
                    // Ordinary native output is installed by its chase owner.
                    // A decoded namespace needs its sibling roster for repair;
                    // a plain chase has no private outputs to publish again.
                    extracted: if decoded.is_some() {
                        outcome.extracted.clone()
                    } else {
                        Vec::new()
                    },
                    failed: outcome.failed.clone(),
                    selected_password: outcome.selected_password.clone(),
                })
                .map_err(Clone::clone);
            {
                let mut state = stream.state.lock().expect("chase stream state");
                if !state.discarded {
                    state.collected = Some((scoped, decoded));
                }
            }
            stream.deliver();
            result.map_err(String::from)
        })
    }

    fn deliver(&self) {
        let collected = {
            let mut state = self.state.lock().expect("chase stream state");
            state.take_ready()
        };
        if let Some((result, decoded)) = collected {
            let sender = self.sender.clone();
            let job_id = self.job_id;
            let owner = self.owner;
            let request = self.request.clone();
            let Some(lease) = self.lease.lock().expect("chase stream lease").take() else {
                return;
            };
            tokio::spawn(async move {
                let _ = lease
                    .send_event(
                        &sender,
                        ExtractionDone::Scoped {
                            job_id,
                            owner,
                            request,
                            decoded,
                            result,
                        },
                    )
                    .await;
            });
        }
    }
}

impl Pipeline {
    pub(crate) async fn retire_unconsumed_chase_streams(&mut self, job_id: JobId) {
        let unused: Vec<_> = self
            .direct_unpack
            .streams
            .iter()
            .filter_map(|((job, set), stream)| {
                (*job == job_id
                    && self
                        .extracted_archives
                        .get(&job_id)
                        .is_some_and(|sets| sets.contains(set))
                    && !stream.state.lock().expect("chase stream state").accepted)
                    .then_some(set.clone())
            })
            .collect();
        for set in unused {
            self.direct_unpack_abort_set(
                job_id,
                &set,
                "archive extracted by another owner",
                super::wiring::AbortLatch::Permanent,
                super::wiring::DemotionReason::DownloadEnded,
            );
            self.taint_direct_unpack_set(job_id, &set);
            self.discard_chase_stream(job_id, &set);
            self.drain_direct_unpack_set(job_id, &set).await;
        }
    }

    pub(crate) fn chase_stream_manifest(&self, job_id: JobId, set_name: &str) -> ChaseManifest {
        self.direct_unpack
            .streams
            .get(&(job_id, set_name.to_owned()))
            .map(|stream| stream.manifest.clone())
            .unwrap_or_default()
    }

    pub(crate) fn chase_stream_owner(&self, job_id: JobId, set_name: &str) -> Option<StreamOwner> {
        self.direct_unpack
            .streams
            .get(&(job_id, set_name.to_owned()))
            .map(|stream| stream.owner)
    }

    pub(crate) fn has_scoped_chase(&self, job_id: JobId, set_name: &str) -> bool {
        self.direct_unpack
            .streams
            .contains_key(&(job_id, set_name.to_owned()))
    }

    pub(crate) fn accept_chase_stream(&mut self, job_id: JobId, set_name: &str) -> bool {
        if let Some(stream) = self
            .direct_unpack
            .streams
            .get(&(job_id, set_name.to_owned()))
        {
            let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
                return false;
            };
            if !runtime.sessions.execution_current(stream.owner)
                || runtime.accept_speculative(&stream.request).is_err()
            {
                return false;
            }
            stream.state.lock().expect("chase stream state").accepted = true;
            stream.deliver();
            return true;
        }
        false
    }

    pub(crate) async fn discard_chase_stream_joined(&mut self, job_id: JobId, set_name: &str) {
        if let Some(stream) = self
            .direct_unpack
            .streams
            .remove(&(job_id, set_name.to_owned()))
        {
            {
                let mut state = stream.state.lock().expect("chase stream state");
                state.discarded = true;
                state.collected.take();
            }
            stream.lease.lock().expect("chase stream lease").take();
            if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
                let _ = runtime.discard_speculative(&stream.request).await;
            }
        }
    }

    pub(crate) fn discard_chase_stream(&mut self, job_id: JobId, set_name: &str) {
        if let Some(stream) = self
            .direct_unpack
            .streams
            .remove(&(job_id, set_name.to_owned()))
        {
            let accepted = {
                let mut state = stream.state.lock().expect("chase stream state");
                state.discarded = true;
                state.collected.take();
                state.accepted
            };
            stream.lease.lock().expect("chase stream lease").take();
            if let Some(runtime) = self.archive_runtime.get_mut(&job_id)
                && let Ok(barrier) = runtime.retire_owner_barrier(stream.owner)
                && !accepted
            {
                self.direct_unpack
                    .retirement_barriers
                    .insert(stream.staging, barrier);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collected() -> Collected {
        (
            Ok(FullSetExtractionOutcome {
                extracted: Vec::new(),
                failed: Vec::new(),
                selected_password: None,
            }),
            None,
        )
    }

    #[test]
    fn collected_result_waits_for_acceptance_and_delivers_once() {
        let mut state = State {
            collected: Some(collected()),
            ..State::default()
        };
        assert!(state.take_ready().is_none());
        state.accepted = true;
        assert!(state.take_ready().is_some());
        assert!(state.take_ready().is_none());
    }

    #[test]
    fn discarded_generation_never_delivers_even_if_accepted() {
        let mut state = State {
            accepted: true,
            discarded: true,
            collected: Some(collected()),
        };
        assert!(state.take_ready().is_none());
    }

    #[test]
    fn archive_selection_keeps_ordinary_payload_on_file_output() {
        assert!(archive_member(std::path::Path::new("inner.part01.rar")));
        assert!(archive_member(std::path::Path::new("inner.7z.001")));
        assert!(!archive_member(std::path::Path::new("video.mkv")));
    }
}
