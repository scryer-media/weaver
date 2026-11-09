// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/**
 * The `data-testid`s of the jobs, scripts and scheduling pages.
 *
 * An ID names the part an element plays, never what it says: a wording change
 * must not move one. Keyed builders take the product's own key (a queue event,
 * a run status), never its caption.
 *
 * The end-to-end suite keeps a copy of everything between the markers in its
 * own package, and a check that the two stay equal.
 */

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
