// The API as a running script sees its own run.
//
// A script calling with its run token may read the queue, history and its
// own run. It may only change its run through `Mutation.scriptRun`.
// Other guarded queries and every other mutation are refused with
// `NOT_ALLOWED_FOR_SCRIPT_RUN`. What is here is the part that is about that
// run: the download it is for, and the things it may ask weaver to do with
// it. Those are the commands a script may also print after `[NZB]`, under the
// same rules for which trigger may issue which, and they are applied by the
// same code.

use async_graphql::{Context, Enum, Object, Result};
use weaver_server_core::post_processing::callbacks::{LiveScriptRun, RunAction, RunActionError};
use weaver_server_core::post_processing::directives::{Directive, DupeMode, ScriptLogLevel};
use weaver_server_core::{Database, SchedulerHandle};

use super::types::ScriptKindGql;
use crate::auth::{CallerIdentity, graphql_error};
use crate::jobs::types::Job;

// The run the caller is, or why the caller is none.
pub(crate) fn calling_run(ctx: &Context<'_>) -> Result<LiveScriptRun> {
    let Some(CallerIdentity::ScriptRun(run_id)) = ctx.data_opt::<CallerIdentity>() else {
        return Err(graphql_error(
            "NOT_A_SCRIPT_RUN",
            "scriptRun is for a running script that calls with its run token",
        ));
    };
    ctx.data::<Database>()?
        .live_script_run(run_id)
        .ok_or_else(|| ended(RunActionError::Ended))
}

fn ended(error: RunActionError) -> async_graphql::Error {
    graphql_error("SCRIPT_RUN_ENDED", error.to_string())
}

fn refusal(error: RunActionError) -> async_graphql::Error {
    match error {
        RunActionError::Ended => ended(error),
        RunActionError::NotAllowed(message) => graphql_error("NOT_ALLOWED_FOR_TRIGGER", message),
        RunActionError::Refused(message) => graphql_error("REFUSED", message),
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
#[graphql(name = "ScriptLogLevel")]
pub enum ScriptLogLevelGql {
    Debug,
    Detail,
    Info,
    Warning,
    Error,
}

impl From<ScriptLogLevelGql> for ScriptLogLevel {
    fn from(value: ScriptLogLevelGql) -> Self {
        match value {
            ScriptLogLevelGql::Debug => Self::Debug,
            ScriptLogLevelGql::Detail => Self::Detail,
            ScriptLogLevelGql::Info => Self::Info,
            ScriptLogLevelGql::Warning => Self::Warning,
            ScriptLogLevelGql::Error => Self::Error,
        }
    }
}

/// How a download a scan script is naming is weighed against others like it.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
#[graphql(name = "ScriptDuplicateMode")]
pub enum ScriptDuplicateModeGql {
    Score,
    All,
    Force,
}

impl From<ScriptDuplicateModeGql> for DupeMode {
    fn from(value: ScriptDuplicateModeGql) -> Self {
        match value {
            ScriptDuplicateModeGql::Score => Self::Score,
            ScriptDuplicateModeGql::All => Self::All,
            ScriptDuplicateModeGql::Force => Self::Force,
        }
    }
}

// The run a script is calling from.
pub struct LiveScriptRunGql(pub(crate) LiveScriptRun);

#[Object(name = "LiveScriptRun")]
impl LiveScriptRunGql {
    /// The same value the script was handed as `WEAVER_RUN_ID`.
    async fn run_id(&self) -> &str {
        &self.0.run_id
    }

    /// The script job the run is for.
    async fn instance_id(&self) -> &str {
        &self.0.instance_id
    }

    /// The script job's name.
    async fn instance_name(&self) -> &str {
        &self.0.instance_name
    }

    /// What started the run.
    async fn event(&self) -> String {
        self.0.event.to_string()
    }

    async fn kind(&self) -> ScriptKindGql {
        self.0.event.kind().into()
    }

    /// A test run. What it asks for is reported to whoever is testing and
    /// never applied.
    async fn test(&self) -> bool {
        self.0.test
    }

    async fn job_id(&self) -> Option<u64> {
        self.0.job_id
    }

    /// The download the run is about. Null when it is about none, and when
    /// that download is no longer in the queue.
    async fn job(&self, ctx: &Context<'_>) -> Result<Option<Job>> {
        let Some(job_id) = self.0.job_id else {
            return Ok(None);
        };
        match ctx
            .data::<SchedulerHandle>()?
            .get_job(weaver_server_core::jobs::ids::JobId(job_id))
        {
            Ok(info) => Ok(Some(Job::from(&info))),
            Err(weaver_server_core::SchedulerError::JobNotFound(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

// What a running script may ask weaver to do for its own run. Each answers
// true once it has been done, and is an error when it was not.
pub struct ScriptRunActionsGql(pub(crate) LiveScriptRun);

impl ScriptRunActionsGql {
    async fn ask(&self, ctx: &Context<'_>, action: RunAction) -> Result<bool> {
        ctx.data::<Database>()?
            .script_run_action(&self.0.run_id, action)
            .await
            .map(|()| true)
            .map_err(refusal)
    }

    async fn command(&self, ctx: &Context<'_>, directive: Directive) -> Result<bool> {
        self.ask(ctx, RunAction::Command(directive)).await
    }
}

#[Object(name = "ScriptRunActions", serial)]
impl ScriptRunActionsGql {
    /// `[NZB] CATEGORY=`. For a scan script.
    async fn set_category(&self, ctx: &Context<'_>, category: String) -> Result<bool> {
        self.command(ctx, Directive::Category(category)).await
    }

    /// `[NZB] NZBNAME=`. For a scan script.
    async fn set_name(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        self.command(ctx, Directive::Name(name)).await
    }

    /// `[NZB] DIRECTORY=`. For a post-processing script, and for a queue
    /// script run when a download has finished downloading.
    async fn set_directory(&self, ctx: &Context<'_>, path: String) -> Result<bool> {
        self.command(ctx, Directive::Directory(path)).await
    }

    /// `[NZB] FINALDIR=`. For a post-processing script.
    async fn set_final_directory(&self, ctx: &Context<'_>, path: String) -> Result<bool> {
        self.command(ctx, Directive::FinalDirectory(path)).await
    }

    /// `[NZB] MARK=BAD`. For a post-processing or queue script.
    async fn mark_bad(&self, ctx: &Context<'_>) -> Result<bool> {
        self.command(ctx, Directive::MarkBad).await
    }

    /// `[NZB] NZBPR_<name>=<value>`. An empty value removes the parameter.
    /// For any script that is about a download.
    async fn set_parameter(&self, ctx: &Context<'_>, name: String, value: String) -> Result<bool> {
        self.command(ctx, Directive::Parameter { name, value })
            .await
    }

    /// `[NZB] PRIORITY=`. For a scan script.
    async fn set_priority(&self, ctx: &Context<'_>, priority: i32) -> Result<bool> {
        self.command(ctx, Directive::Priority(priority)).await
    }

    /// `[NZB] PAUSED=`. For a scan script.
    async fn set_paused(&self, ctx: &Context<'_>, paused: bool) -> Result<bool> {
        self.command(ctx, Directive::Paused(paused)).await
    }

    /// `[NZB] TOP=`. For a scan script.
    async fn set_top(&self, ctx: &Context<'_>, top: bool) -> Result<bool> {
        self.command(ctx, Directive::Top(top)).await
    }

    /// `[NZB] DUPEKEY=`, `DUPESCORE=` and `DUPEMODE=`, for whichever are
    /// given. For a scan script.
    async fn set_duplicate(
        &self,
        ctx: &Context<'_>,
        key: Option<String>,
        score: Option<i32>,
        mode: Option<ScriptDuplicateModeGql>,
    ) -> Result<bool> {
        let directives = [
            key.map(Directive::DupeKey),
            score.map(Directive::DupeScore),
            mode.map(|mode| Directive::DupeMode(mode.into())),
        ];
        if directives.iter().all(Option::is_none) {
            return Err(graphql_error(
                "REFUSED",
                "setDuplicate needs a key, a score or a mode",
            ));
        }
        let directives = directives.into_iter().flatten().collect::<Vec<_>>();
        // All of them or none: one the run may not have stops the others
        // before any is sent.
        for directive in &directives {
            RunAction::Command(directive.clone())
                .check(&self.0.event)
                .map_err(refusal)?;
        }
        for directive in directives {
            self.command(ctx, directive).await?;
        }
        Ok(true)
    }

    /// End the run as failed with `reason`, whatever the script goes on to
    /// exit with. For any script.
    async fn fail(&self, ctx: &Context<'_>, reason: String) -> Result<bool> {
        self.ask(ctx, RunAction::Fail(reason)).await
    }

    /// Add `text` to the run's log. For any script.
    async fn log(&self, ctx: &Context<'_>, level: ScriptLogLevelGql, text: String) -> Result<bool> {
        self.ask(
            ctx,
            RunAction::Log {
                level: level.into(),
                text,
            },
        )
        .await
    }
}
