// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `nzb-reports.json`: the support report of every job Weaver is working on.
//
// A job's NZB says what the job is made of (how many recovery sets, whether
// an SFV rides along, how the posts are split), which the rest of the package
// cannot say once its names are stand-ins. The support report says it without
// a name, group, poster, message id or password in it, so the package carries
// that report, as the report code writes it, for the jobs that matter:
// downloading, failed in the queue, or in post-processing. A job merely
// queued or paused is left out, since a queue can hold thousands of them.
//
// Nothing here asks the pipeline. The jobs are read from the queue's shared
// rows, as `queue.json` is, and each report from the database. The reports
// are made one at a time off the async runtime, so no more than one NZB is
// held at once, and the whole step has a time budget: when it is spent, the
// reports made so far are written and the file says it was cut short.

use std::time::Duration;

use serde::Serialize;
use weaver_server_core::support::{
    ActiveReportGroup, ReportEnvironment, SupportReportError, build_job_support_report,
    now_epoch_secs,
};
use weaver_server_core::{Database, SchedulerHandle};

use super::archive::Component;

pub(super) const FILE: &str = "nzb-reports.json";

// At most this many reports. A report parses the job's whole NZB, and the
// jobs that matter to a stall are a handful.
pub(super) const REPORT_LIMIT: usize = 25;

// The time the whole step may take. It runs inside the gap between the two
// metrics samples, so a normal run adds nothing to the package's wait.
pub(super) const REPORT_BUDGET: Duration = Duration::from_secs(30);

// Why the file holds fewer reports than were chosen.
const CUT_SHORT_TIME_BUDGET: &str = "time_budget_spent";

// The reason a job's entry carries when its report could not be made in the
// time left.
const ENTRY_TIME_BUDGET: &str = "time_budget_spent";

#[derive(Debug, Serialize)]
struct NzbReports {
    // Jobs that qualified, before the cap.
    qualified: usize,
    limit: usize,
    // Entries written, reports and failures together.
    written: usize,
    failed: usize,
    // Set when the time budget ran out before every chosen job had an entry.
    cut_short: Option<&'static str>,
    jobs: Vec<JobEntry>,
}

#[derive(Debug, Serialize)]
struct JobEntry {
    // The id `queue.json` lists the job under.
    job_id: u64,
    // The job's stand-in everywhere else in the package.
    job: String,
    group: ActiveReportGroup,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    report: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<EntryError>,
}

#[derive(Debug, Serialize)]
struct EntryError {
    reason: &'static str,
    message: &'static str,
}

// A fixed sentence per reason. The error's own text is not carried: a parse
// error can quote the document it failed on.
fn entry_error(error: &SupportReportError) -> EntryError {
    let message = match error {
        SupportReportError::NotFound(_) => "the job left the queue and history before its report",
        SupportReportError::NzbUnavailable(_) => "the job's NZB is no longer stored",
        SupportReportError::Parse(_) => "the job's stored NZB could not be read as an NZB",
        SupportReportError::State(_) => "the database could not be read for the job's report",
        SupportReportError::Task(_) => "the report task stopped before it finished",
    };
    EntryError {
        reason: error.code(),
        message,
    }
}

pub(super) async fn collect_nzb_reports(db: &Database, handle: &SchedulerHandle) -> Component {
    collect_nzb_reports_within(db, handle, REPORT_LIMIT, REPORT_BUDGET, now_epoch_secs()).await
}

pub(super) async fn collect_nzb_reports_within(
    db: &Database,
    handle: &SchedulerHandle,
    limit: usize,
    budget: Duration,
    // Every report is made as of this moment, the package's own.
    now_epoch_secs: u64,
) -> Component {
    let deadline = tokio::time::Instant::now() + budget;
    let chosen = handle.active_report_jobs(limit);
    let environment = ReportEnvironment::current(db, handle);
    let mut reports = NzbReports {
        qualified: chosen.qualified,
        limit,
        written: 0,
        failed: 0,
        cut_short: None,
        jobs: Vec::with_capacity(chosen.jobs.len()),
    };

    for (group, info) in chosen.jobs {
        if tokio::time::Instant::now() >= deadline {
            reports.cut_short = Some(CUT_SHORT_TIME_BUDGET);
            break;
        }
        let job_id = info.job_id.0;
        let mut entry = JobEntry {
            job_id,
            job: format!("job-{job_id}"),
            group,
            status: info.status.persisted_status(),
            report: None,
            error: None,
        };
        let db = db.clone();
        let task = tokio::task::spawn_blocking(move || {
            // The NZB is read, parsed and dropped inside this call; only the
            // report leaves it.
            build_job_support_report(&db, Some(info), job_id, &environment, now_epoch_secs)
                .and_then(|report| {
                    serde_json::from_str::<serde_json::Value>(&report.json)
                        .map_err(|error| SupportReportError::Task(error.to_string()))
                })
        });
        match tokio::time::timeout_at(deadline, task).await {
            Ok(Ok(Ok(report))) => entry.report = Some(report),
            Ok(Ok(Err(error))) => entry.error = Some(entry_error(&error)),
            Ok(Err(join)) => {
                entry.error = Some(entry_error(&SupportReportError::Task(join.to_string())));
            }
            Err(_) => {
                entry.error = Some(EntryError {
                    reason: ENTRY_TIME_BUDGET,
                    message: "the time budget ran out while this report was being made",
                });
                reports.cut_short = Some(CUT_SHORT_TIME_BUDGET);
            }
        }
        if entry.error.is_some() {
            reports.failed += 1;
        }
        reports.jobs.push(entry);
        if reports.cut_short.is_some() {
            break;
        }
    }

    reports.written = reports.jobs.len();
    Component::json(FILE, &reports)
}
