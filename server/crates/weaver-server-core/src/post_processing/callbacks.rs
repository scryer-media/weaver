//! What a running script may ask of weaver through the API.
//!
//! Every run is handed a token that is good for as long as that run lasts and
//! no longer. What the script asks for with it is the vocabulary of the
//! `[NZB]` commands it may print, held to the same allow-list and applied by
//! the same code: whatever is reading the script's output also takes these
//! requests, one at a time, in the order they arrive.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::directives::{Directive, ScriptLogLevel};
use super::model::ScriptEventLabel;
use super::runner::{ExecutionDisposition, OutputInjector, RunIdentity, ScriptExecutionResult};
use crate::Database;
use crate::auth::service::{
    ScriptRunClaims, create_script_run_jwt, is_signed_token_shape, verify_script_run_jwt,
};

/// Requests of one run that may wait to be taken.
const WAITING_REQUESTS: usize = 16;
/// How far past the end of its own time limit a run's token is dated. The run
/// ending is what ends the token; the date only bounds a token that outlives
/// the process that issued it.
const TOKEN_MARGIN: Duration = Duration::from_secs(5 * 60);
/// The date on the token of a run that has no time limit.
const UNLIMITED_RUN: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// The longest text one `log` request may carry.
pub const MAX_LOG_TEXT_BYTES: usize = 8 * 1024;
/// The longest reason one `fail` request may carry.
pub const MAX_FAIL_REASON_BYTES: usize = 1024;

/// One thing a running script asked for.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RunAction {
    /// A command, as `[NZB]` would have carried it.
    Command(Directive),
    /// A line for the run's log.
    Log { level: ScriptLogLevel, text: String },
    /// End the run as failed, whatever the script goes on to exit with.
    Fail(String),
}

impl RunAction {
    /// Whether a run for `event` may ask for this at all.
    fn check(&self, event: &ScriptEventLabel) -> Result<(), RunActionError> {
        match self {
            Self::Command(directive) => {
                if !directive.well_formed() {
                    return Err(RunActionError::Refused("Invalid command".into()));
                }
                if !directive.allowed_for(event) {
                    return Err(RunActionError::NotAllowed(format!(
                        "Command {} is not allowed for {event}",
                        directive.command()
                    )));
                }
            }
            Self::Log { text, .. } => {
                if text.len() > MAX_LOG_TEXT_BYTES || text.contains('\0') {
                    return Err(RunActionError::Refused(format!(
                        "a log entry is at most {MAX_LOG_TEXT_BYTES} bytes of text"
                    )));
                }
            }
            Self::Fail(reason) => {
                if reason.trim().is_empty()
                    || reason.len() > MAX_FAIL_REASON_BYTES
                    || reason.contains(['\0', '\n', '\r'])
                {
                    return Err(RunActionError::Refused(format!(
                        "a reason is one line of at most {MAX_FAIL_REASON_BYTES} bytes"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Why a script did not get what it asked for.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum RunActionError {
    #[error("the script run has ended")]
    Ended,
    /// The run's trigger does not allow the command.
    #[error("{0}")]
    NotAllowed(String),
    #[error("{0}")]
    Refused(String),
}

fn ended() -> String {
    RunActionError::Ended.to_string()
}

/// A script run that is going on now.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LiveScriptRun {
    pub run_id: String,
    pub instance_id: String,
    pub instance_name: String,
    /// The download the run is about, when it is about one.
    pub job_id: Option<u64>,
    pub event: ScriptEventLabel,
    /// A test run: what it asks for is reported and never applied.
    pub test: bool,
}

/// A request on its way to the run it is for.
pub struct RunRequest {
    pub action: RunAction,
    /// Answered once the request has been dealt with. Dropping it tells the
    /// script the run has ended.
    pub reply: oneshot::Sender<Result<(), String>>,
}

struct Registered {
    run: LiveScriptRun,
    requests: mpsc::Sender<RunRequest>,
}

/// The runs going on now, and where their scripts reach this server.
#[derive(Default)]
pub(crate) struct LiveRuns {
    runs: Mutex<HashMap<String, Registered>>,
    api_url: Mutex<Option<String>>,
}

impl LiveRuns {
    fn runs(&self) -> std::sync::MutexGuard<'_, HashMap<String, Registered>> {
        self.runs.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn api_url(&self) -> Option<String> {
        self.api_url
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

/// A run's place among the live runs, and what its script asks for while it
/// holds that place. Dropping it ends the run's token.
pub struct RunRequests {
    live: Arc<LiveRuns>,
    run_id: String,
    receiver: mpsc::Receiver<RunRequest>,
    output: OutputInjector,
    failure: Option<String>,
}

impl RunRequests {
    /// The next thing the script asks for. Never resolves when it asks for
    /// nothing more.
    pub async fn next(&mut self) -> RunRequest {
        match self.receiver.recv().await {
            Some(request) => request,
            // The other end is held among the live runs until this is dropped.
            None => std::future::pending().await,
        }
    }

    /// Take a line the script logged. It joins the run's output, and comes
    /// back with the run's secrets taken out for whatever else records it.
    pub(crate) fn log(&self, text: &str) -> Result<String, String> {
        let text = self.output.redact(text).ok_or_else(ended)?;
        if !self.output.push(&text) {
            return Err(ended());
        }
        Ok(text)
    }

    /// Take the script's word that it failed.
    pub(crate) fn fail(&mut self, reason: &str) -> Result<(), String> {
        self.failure = Some(self.output.redact(reason).ok_or_else(ended)?);
        Ok(())
    }

    /// The reason the script gave for failing, when it gave one.
    pub(crate) fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    /// Count the script's own word that it failed into how its run came out.
    /// A run that was stopped is still reported as stopped.
    pub(crate) fn settle(&self, result: &mut ScriptExecutionResult) {
        if let Some(reason) = self.failure()
            && matches!(
                result.disposition,
                ExecutionDisposition::Succeeded
                    | ExecutionDisposition::Skipped
                    | ExecutionDisposition::Failed
            )
        {
            result.disposition = ExecutionDisposition::Failed;
            result.error_message = Some(reason.to_string());
        }
    }
}

impl Drop for RunRequests {
    fn drop(&mut self) {
        self.live.runs().remove(&self.run_id);
    }
}

impl Database {
    /// Where scripts reach this server's API. Set by the server once it is
    /// listening; until then runs are handed no address and no token.
    pub fn set_script_api_url(&self, url: impl Into<String>) {
        *self
            .script_runtime
            .live
            .api_url
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(url.into());
    }

    /// Put a run among the live runs and tell `identity` how its script may
    /// call back. `time_limit` is how long the run is allowed to last.
    ///
    /// Whoever holds what this returns is the run: it must take what the
    /// script asks for, and the run is over when it lets go.
    #[doc(hidden)]
    pub fn open_script_run(
        &self,
        identity: &mut RunIdentity,
        job_id: Option<u64>,
        event: &ScriptEventLabel,
        time_limit: Option<Duration>,
        test: bool,
    ) -> RunRequests {
        let live = self.script_runtime.live.clone();
        let (requests, receiver) = mpsc::channel(WAITING_REQUESTS);
        let run = LiveScriptRun {
            run_id: identity.run_id.clone(),
            instance_id: identity.instance_id.clone(),
            instance_name: identity.instance_name.clone(),
            job_id,
            event: event.clone(),
            test,
        };
        if let Some(url) = live.api_url() {
            match self.get_or_create_jwt_signing_secret() {
                Ok(secret) => {
                    let lasts = time_limit
                        .map_or(UNLIMITED_RUN, |limit| limit.saturating_add(TOKEN_MARGIN));
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default();
                    identity.token = Some(create_script_run_jwt(
                        &ScriptRunClaims {
                            run_id: run.run_id.clone(),
                            instance_id: run.instance_id.clone(),
                            job_id,
                            exp: now.saturating_add(lasts).as_secs(),
                        },
                        &secret,
                    ));
                    identity.api_url = Some(url);
                }
                Err(error) => {
                    tracing::warn!(%error, "could not sign a script run token; the script cannot call back");
                }
            }
        }
        let run_id = run.run_id.clone();
        live.runs()
            .insert(run_id.clone(), Registered { run, requests });
        RunRequests {
            live,
            run_id,
            receiver,
            output: identity.output.clone(),
            failure: None,
        }
    }

    /// The live run `run_id`, or `None` once it has ended.
    pub fn live_script_run(&self, run_id: &str) -> Option<LiveScriptRun> {
        self.script_runtime
            .live
            .runs()
            .get(run_id)
            .map(|registered| registered.run.clone())
    }

    /// The run `token` was issued to, when that run is still going on. A token
    /// is worth nothing once its run has ended, whatever date it carries.
    pub fn script_run_for_token(&self, token: &str) -> Option<LiveScriptRun> {
        if !is_signed_token_shape(token) {
            return None;
        }
        let secret = self.get_or_create_jwt_signing_secret().ok()?;
        let claims = verify_script_run_jwt(token, &secret).ok()?;
        let run = self.live_script_run(&claims.run_id)?;
        (run.instance_id == claims.instance_id && run.job_id == claims.job_id).then_some(run)
    }

    /// Hand `action` to the live run `run_id` and wait for what came of it.
    pub async fn script_run_action(
        &self,
        run_id: &str,
        action: RunAction,
    ) -> Result<(), RunActionError> {
        let (event, requests) = {
            let runs = self.script_runtime.live.runs();
            let registered = runs.get(run_id).ok_or(RunActionError::Ended)?;
            (registered.run.event.clone(), registered.requests.clone())
        };
        action.check(&event)?;
        let (reply, answer) = oneshot::channel();
        requests
            .send(RunRequest { action, reply })
            .await
            .map_err(|_| RunActionError::Ended)?;
        answer
            .await
            .map_err(|_| RunActionError::Ended)?
            .map_err(RunActionError::Refused)
    }
}
