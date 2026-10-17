//! The blocking decode of one scoped namespace, and the async collector that
//! drains its bridge and reports the verdict to the actor.

use super::*;
use crate::pipeline::completion::finalize::extract::{prepared, sequential};
use crate::pipeline::direct_store::plan::StreamKind;
use crate::pipeline::extraction::input::LiveArchiveInput;
use crate::pipeline::extraction::output::channel::{ChannelOutput, Frame, PrivateTrackedFiles};
use crate::pipeline::extraction::output::select::SelectOutput;
use crate::pipeline::extraction::runtime::RuntimeError;
use crate::pipeline::extraction::runtime::decoded::DecodedState;
use crate::pipeline::extraction::session::SessionLease;

/// How a scoped decode reaches its outputs. Archive formats and live streams
/// go through a bridge so nested members can feed child engines; the rest
/// write directly.
pub(super) enum ScopedDecode {
    Rar(ChannelOutput),
    SevenZip(ChannelOutput),
    Stream(StreamKind, ChannelOutput),
    Simple(SimpleArchiveKind),
}

/// The coordinator half of a streamed decode.
pub(super) struct ScopedStream {
    pub(super) receiver: tokio::sync::mpsc::Receiver<Frame>,
    pub(super) decoded: DecodedState,
}

/// Everything one scoped decode needs, captured on the actor.
pub(super) struct ScopedEngine {
    pub(super) job_id: JobId,
    pub(super) set_name: String,
    pub(super) volumes: Vec<u32>,
    pub(super) input_names: Vec<String>,
    pub(super) inputs: Vec<PathBuf>,
    pub(super) live: Option<LiveArchiveInput>,
    pub(super) passwords: Vec<crate::jobs::ArchivePasswordCandidate>,
    pub(super) policy: Arc<crate::post_processing::model::PostProcessingSettings>,
    pub(super) root: Arc<ExtractionRoot>,
    pub(super) output_dir: PathBuf,
    pub(super) budget: Arc<crate::pipeline::extraction::JobExtractionBudget>,
    pub(super) event_tx: tokio::sync::broadcast::Sender<PipelineEvent>,
    pub(super) counters: Arc<crate::jobs::PhaseCounters>,
    pub(super) sevenz_decode_memory: u64,
    pub(super) threads: usize,
}

impl ScopedEngine {
    pub(super) fn run(
        self,
        decode: ScopedDecode,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        match decode {
            ScopedDecode::Rar(channel) => channel.run(|output| self.run_rar(output)),
            ScopedDecode::SevenZip(channel) => channel.run(|output| self.run_sevenz(output)),
            ScopedDecode::Stream(kind, channel) => {
                channel.run(|output| self.run_stream(kind.into(), output))
            }
            ScopedDecode::Simple(kind) => self.run_simple(kind),
        }
    }

    fn run_rar(
        mut self,
        channel: &ChannelOutput,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        let root = Arc::clone(&self.root);
        let output = SelectOutput::child_archives(
            channel,
            PrivateTrackedFiles {
                output: channel,
                root: &root,
            },
        );
        match self.live.take() {
            Some(input) => self.run_live_rar(&input, &output),
            None => self.prepared_rar().run_to(&output),
        }
    }

    fn run_live_rar<O: crate::pipeline::extraction::output::ArchiveOutput>(
        &self,
        input: &LiveArchiveInput,
        output: &O,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        let password = live_rar::select_password(input, &self.passwords, &self.budget)?;
        crate::pipeline::direct_unpack::rar::extract_from_roster_to(
            crate::pipeline::direct_unpack::rar::RarStreamContext {
                provider: input,
                volume_count: 0,
                budget: &self.budget,
                password,
                counters: &self.counters,
                policy: &self.policy,
            },
            output,
            |index| input.wait_volume(index).map_err(|error| error.to_string()),
            |_| Ok(true),
        )
        .map_err(ExtractionError::recoverable)
    }

    fn prepared_rar(self) -> prepared::PreparedRar {
        prepared::PreparedRar {
            job_id: self.job_id,
            set_name: self.set_name,
            volume_paths: self.volumes.into_iter().zip(self.inputs).collect(),
            cached_headers: None,
            password_candidates: self.passwords,
            shared_kdf_cache: Arc::new(unrar_rs::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,
            already_extracted: HashSet::new(),
            root: self.root,
            budget: self.budget,
            policy: self.policy,
            event_tx: self.event_tx,
            output_dir: self.output_dir,
            phase_counters: self.counters,
        }
    }

    fn run_sevenz(
        mut self,
        channel: &ChannelOutput,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        let root = Arc::clone(&self.root);
        let output = SelectOutput::child_archives(
            channel,
            PrivateTrackedFiles {
                output: channel,
                root: &root,
            },
        );
        let live = self.live.take();
        let prepared = self.prepared_sevenz();
        match live {
            Some(input) => prepared.run_reader_to(&output, || Ok(input.reader())),
            None => prepared.run_to(&output),
        }
    }

    fn prepared_sevenz(self) -> prepared::PreparedSevenZip {
        prepared::PreparedSevenZip {
            job_id: self.job_id,
            set_name: self.set_name,
            part_candidates: self.volumes.into_iter().zip(self.inputs).collect(),
            uu_paths: HashSet::new(),
            passwords: self.passwords,
            root: self.root,
            budget: self.budget,
            event_tx: self.event_tx,
            phase_counters: self.counters,
            sevenz_decode_memory: self.sevenz_decode_memory,
            sevenz_decode_threads: u32::try_from(self.threads).unwrap_or(u32::MAX),
        }
    }

    // A stream's one output is named after its input, so an output that is
    // itself an archive chains to a child engine like any archive member. The
    // live input's bytes never change once placed, so a decode that outgrows
    // its header's reservation reads them again from the start.
    fn run_stream(
        mut self,
        kind: SimpleArchiveKind,
        channel: &ChannelOutput,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        let (Some(input), Some(name)) = (self.live.take(), self.input_names.first()) else {
            return Err(ExtractionError::terminal("a stream decodes one live input"));
        };
        let output = SelectOutput::child_archives(
            channel,
            PrivateTrackedFiles {
                output: channel,
                root: &self.root,
            },
        );
        let extracted = sequential::decode_single_stream_to(
            || Ok(input.reader()),
            &output,
            &sequential::SingleStreamContext {
                kind,
                input: Path::new(name),
                budget: &self.budget,
                event_tx: &self.event_tx,
                job_id: self.job_id,
                set_name: &self.set_name,
            },
        )?;
        Ok(FullSetExtractionOutcome {
            extracted,
            failed: Vec::new(),
            selected_password: None,
        })
    }

    fn run_simple(
        self,
        kind: SimpleArchiveKind,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        prepared::PreparedSimple {
            job_id: self.job_id,
            set_name: self.set_name,
            kind,
            file_paths: self.inputs,
            passwords: self.passwords,
            joined_output_already_present: None,
            root: self.root,
            budget: self.budget,
            event_tx: self.event_tx,
            phase_counters: self.counters,
            xz_worker_threads: self.threads,
        }
        .run()
    }
}

/// Where a scoped decode reports its verdict.
pub(super) struct ScopedReport {
    pub(super) job_id: JobId,
    pub(super) owner: StreamOwner,
    pub(super) request: RequestId,
    pub(super) lease: SessionLease,
    pub(super) done: tokio::sync::mpsc::Sender<ExtractionDone>,
}

impl ScopedReport {
    /// Drains the bridge while the engine runs, settles the namespace once the
    /// engine succeeds, and reports. A stream failure outranks an engine
    /// success, never an engine failure.
    pub(super) async fn collect(
        self,
        engine: tokio::task::JoinHandle<Result<FullSetExtractionOutcome, ExtractionError>>,
        mut stream: Option<ScopedStream>,
    ) {
        let mut stream_error = match &mut stream {
            Some(stream) => self.drain(stream).await.err(),
            None => None,
        };
        let mut result = engine.await.unwrap_or_else(|error| {
            Err(RuntimeError::join("namespace extraction worker")(error).into())
        });
        let engine_succeeded = result
            .as_ref()
            .is_ok_and(|outcome| outcome.failed.is_empty());
        if engine_succeeded
            && stream_error.is_none()
            && let Some(stream) = &mut stream
        {
            stream_error = self.close(&mut stream.decoded).await.err();
        }
        // A bridge failure is the actor's, not the archive's: no repair of
        // the input changes it, so it ends the namespace whatever its cause.
        if let Some(error) = stream_error
            && result.is_ok()
        {
            result = Err(ExtractionError::terminal(error.to_string()));
        }
        let decoded = stream.map(|stream| Box::new(stream.decoded));
        let _ = self
            .lease
            .send_event(
                &self.done,
                ExtractionDone::Scoped {
                    job_id: self.job_id,
                    owner: self.owner,
                    request: self.request.clone(),
                    decoded,
                    result,
                },
            )
            .await;
    }

    // The receiver stays alive after a failure; revocation is what releases
    // a producer blocked on the bridge.
    async fn drain(&self, stream: &mut ScopedStream) -> Result<(), RuntimeError> {
        while let Some(frame) = stream.receiver.recv().await {
            if let Err(error) = self.accept(&mut stream.decoded, frame).await {
                stream.decoded.generation.revoke();
                return Err(error);
            }
        }
        Ok(())
    }

    async fn accept(&self, decoded: &mut DecodedState, frame: Frame) -> Result<(), RuntimeError> {
        decoded.accept(frame).await?;
        self.publish_progress(decoded).await
    }

    async fn close(&self, decoded: &mut DecodedState) -> Result<(), RuntimeError> {
        decoded.close().await?;
        self.publish_progress(decoded).await
    }

    async fn publish_progress(&self, decoded: &mut DecodedState) -> Result<(), RuntimeError> {
        decoded.publish_progress(&self.done, &self.lease).await
    }
}
