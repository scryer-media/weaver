// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A copy of the web app's apps/weaver-web/src/next/testids/jobs.ts, between the
// markers. The package does not import across its boundary; the
// testids-pin check keeps the two equal.

// testids:begin
/** The jobs, scripts and scheduling pages. */
export const jobIds = {
  // The scripts settings page.
  scriptDirectory: "script-directory",

  // A job's script runs, keyed by the event that started them and how each ended.
  scriptEventGroup: (event: string) => `script-event-${event}`,
  scriptRun: "script-run",
  scriptRunStatus: (status: string) => `script-run-status-${status}`,

  // Removing a history entry's files: the action, and the mark on an entry whose files are gone.
  historyRemoveFiles: "history-remove-files",
  historyFilesRemoved: (jobId: string | number) => `history-files-removed-${jobId}`,
  // A history row's own actions, keyed by the job's id. Not under the row's
  // `history-row-` prefix, which finds rows.
  historyRowRemoveFiles: (jobId: string | number) => `history-entry-remove-files-${jobId}`,
  historyRowDelete: (jobId: string | number) => `history-entry-delete-${jobId}`,
  jobRemoveFiles: "job-remove-files",
  jobFilesRemoved: "job-files-removed",

  // A job page's lists past one page: the control that loads the next, and a file kept only as a record.
  jobFilesShowMore: "job-files-show-more",
  jobFileRemoved: "job-file-removed",
  jobEventsShowMore: "job-events-show-more",

  // A job page's download panel: the panel, its archive lines keyed by node id, and a password's source group.
  jobDownload: "job-download",
  jobDownloadNode: (nodeId: string | number) => `job-download-node-${nodeId}`,
  jobDownloadPasswordOrigin: (group: string) => `job-download-password-${group}`,
  jobDownloadRecovery: "job-download-recovery",
  jobDownloadPending: "job-download-pending",
  jobDownloadUnrecorded: "job-download-unrecorded",
} as const;
// testids:end
