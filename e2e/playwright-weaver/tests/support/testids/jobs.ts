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

  // A job's tags: its chips, the editor that adds and takes them off, and
  // the rail filter, keyed by the tag ("" is jobs with none).
  jobTags: "job-tags",
  tagEditor: "tag-editor",
  tagInput: "tag-input",
  tagFilter: (tag: string) => `tag-filter-${tag}`,
} as const;
// testids:end
