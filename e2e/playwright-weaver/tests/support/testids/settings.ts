// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A copy of the web app's apps/weaver-web/src/next/testids/settings.ts, between the
// markers. The package does not import across its boundary; the
// testids-pin check keeps the two equal.

// testids:begin
/** The shared controls, dialogs, settings framework and shell. */
export const testIds = {
  // The settings framework: one block, its fields and its rows.
  section: (blockId: string) => `section-${blockId}`,
  field: (fieldId: string) => `field-${fieldId}`,
  row: (blockId: string, rowId: string) => `row-${blockId}-${rowId}`,
  rowPrefix: (blockId: string) => `row-${blockId}-`,
  rowGroup: (blockId: string, groupId: string) => `group-${blockId}-${groupId}`,
  tableAdd: (blockId: string) => `add-${blockId}`,
  tableAction: (blockId: string) => `action-${blockId}`,
  tableEmpty: (blockId: string) => `empty-${blockId}`,
  sectionNote: (blockId: string) => `note-${blockId}`,
  /** A settings field's help line, by the field id. */
  fieldHelp: (fieldId: string) => `help-${fieldId}`,
  /** A history row, keyed by the job's id. */
  historyRow: (jobId: string | number) => `history-row-${jobId}`,
  historyRowPrefix: "history-row-",
  /** A tab of the tab bar `ownerId`, by the tab's id, and the count beside it. */
  tab: (ownerId: string, tabId: string) => `${ownerId}-tab-${tabId}`,
  tabCount: (ownerId: string, tabId: string) => `${ownerId}-count-${tabId}`,
  // The pagination bar under a paged list.
  paginationRange: "pagination-range",
  paginationPrevious: "pagination-previous",
  paginationNext: "pagination-next",
  paginationPageSize: "pagination-page-size",

  // A control's own parts, keyed by the control's ID.
  menu: (ownerId: string) => `${ownerId}-menu`,
  option: (ownerId: string, value: string) => `${ownerId}-option-${value}`,
  /** A Select's trigger showing `value` as the current choice. */
  selected: (ownerId: string, value: string) => `${ownerId}-selected-${value}`,
  browse: (ownerId: string) => `${ownerId}-browse`,

  // Dialogs and their footers.
  dialog: (dialogId: string) => `dialog-${dialogId}`,
  editorSave: "editor-save",
  editorCancel: "editor-cancel",
  editorDelete: "editor-delete",
  editorError: "editor-error",
  confirmAccept: "confirm-accept",
  confirmDismiss: "confirm-dismiss",
  confirmAlternative: "confirm-alternative",

  // The file and folder picker.
  pickerPath: "picker-path",
  pickerPathInput: "picker-path-input",
  pickerGo: "picker-go",
  pickerUse: "picker-use",
  pickerCancel: "picker-cancel",
  pickerEntry: (name: string) => `picker-entry-${name}`,

  // The shell around every screen.
  shellNav: "shell-nav",
  navLink: (screen: string) => `nav-${screen}`,
  shellOpenNavigation: "shell-open-navigation",
  shellSignOut: "shell-sign-out",
  shellFooter: "shell-footer",
  shellConnection: "shell-connection",
  shellStatus: "shell-status",
  /** The footer's connection dot, by the state it shows (connected, reconnecting, polling). */
  shellConnectionState: (state: "connected" | "reconnecting" | "polling") => `shell-connection-${state}`,

  // The queue's pause and resume control, in the Downloads top bar.
  queuePauseAll: "queue-pause-all",
  queueResumeAll: "queue-resume-all",
  queueResumeTimed: "queue-resume-timed",
  /** The Downloads list with no job in the queue at all. */
  queueEmpty: "queue-empty",
} as const;

/** The settings screen and the pages the settings specs drive. */
export const settingsIds = {
  nav: "settings-nav",
  search: "settings-search",
  revert: "settings-revert",
  // The commit button carries the state it is in.
  save: "settings-save",
  saving: "settings-saving",
  saved: "settings-saved",

  // Sign-in and Security.
  loginSubmit: "login-submit",
  loginSetup: "security-login-setup",
  loginOff: "security-login-off",
  passwordChange: "security-password-change",
  signOutEverywhere: "security-sign-out-everywhere",
  keyScope: (scope: string) => `security-key-scope-${scope}`,
  keyRevoke: "security-key-revoke",
  keyCreatedValue: "security-key-created-value",
  keyCreatedDone: "security-key-created-done",

  // General.
  archivePasswordList: "archive-password-list",
  archivePasswordEdit: "archive-password-edit",
  archivePasswordClear: "archive-password-clear",
  profileUnconfirmed: "hardware-profile-unconfirmed",

  // Servers.
  providerEnabled: "provider-enabled",
  providerConnections: "provider-connections",
  providerTransport: (kind: "tls" | "plain") => `provider-transport-${kind}`,

  // Schedules.
  scheduleEnabled: "schedule-enabled",
  // Watch folder: the Scan now button and the last scan's report.
  watchScanNow: "watch-scan-now",
  watchFound: "watch-scan-found",
  watchQueued: "watch-scan-queued",
  /** One problem line of the last scan, by its tone (error, warn, muted). */
  watchProblem: (tone: string) => `watch-scan-problem-${tone}`,
  // RSS: a feed row's poll button and state cell, a rule's action cell, the seen list's clear button.
  rssFeedPoll: "rss-feed-poll",
  rssFeedState: "rss-feed-state",
  /** A rule's action cell, by the product's action value (ACCEPT, REJECT). */
  rssRuleAction: (action: string) => `rss-rule-action-${action}`,
  rssSeenClear: "rss-seen-clear",
  /** A count on a sync's report line, by what it counts (polled, new, queued, ignored). */
  rssReportCount: (count: "polled" | "new" | "queued" | "ignored") => `rss-report-${count}`,
  /** The first classified error on a sync's report line. */
  rssReportError: "rss-report-error",
  // Backup: the export and restore controls and what a read archive holds.
  backupDownload: "backup-download",
  backupChooseFile: "backup-choose-file",
  backupFileName: "backup-file-name",
  /** The restore availability line, while the check is still running. */
  backupStatusChecking: "backup-status-checking",
  /** The restore availability line once the check answered. */
  backupStatus: "backup-status",
  backupReadArchive: "backup-read-archive",
  backupRestore: "backup-restore",
  backupSourceEngine: "backup-source-engine",
  /** Every category destination path field a restore asks for. */
  backupRemap: "backup-remap",
  /** The file name on the status line once an export is saved. */
  backupSavedFile: "backup-saved-file",
  /** A failure on the Backup panel's status line. */
  backupError: "backup-error",
  /** A restore's note on the status line, by what it did (staged, restored). */
  backupRestoreOutcome: (outcome: "staged" | "restored") => `backup-restore-outcome-${outcome}`,
  /** The history count inside a restore's note. */
  backupRestoreHistoryJobs: "backup-restore-history-jobs",
  // Scripts: a post-processing job row's controls and the test dialog.
  ppJobEnabled: "pp-job-enabled",
  ppJobReapply: "pp-job-reapply",
  ppJobMoveUp: "pp-job-move-up",
  ppJobMoveDown: "pp-job-move-down",
  ppJobTest: "pp-job-test",
  ppJobDelete: "pp-job-delete",
  ppTestAgain: "pp-test-again",
  ppTestClose: "pp-test-close",
  ppTestArguments: "pp-test-arguments",
  ppTestLog: "pp-test-log",
  /** A secret's used-by cell, by whether anything uses it (used, unused). */
  secretUsage: (state: "used" | "unused") => `secret-usage-${state}`,
  // Script runs: the trigger filter, the status tabs and an opened run.
  scriptRunsTrigger: "script-runs-trigger",
  scriptRunsStatus: "script-runs-status",
  scriptRunOutput: "script-run-output",
  scriptRunCopy: "script-run-copy",
  scriptRunExitCode: "script-run-exit-code",

  // Backup storage and the stored backups table.
  backupCreate: "backup-create",
  backupStorageSave: "backup-storage-save",
  storedBackupDownload: "stored-backup-download",
  storedBackupDelete: "stored-backup-delete",
  /** A stored backup's status cell, by its status (ready, creating, failed). */
  storedBackupStatus: (status: "ready" | "creating" | "failed") => `stored-backup-status-${status}`,
} as const;

/** The runtime screens: monitoring, logs and the NZB analyzer. */
export const runtimeIds = {
  monitoringPipeline: "monitoring-pipeline",
  monitoringConnections: "monitoring-connections",
  monitoringDepths: "monitoring-depths",
  monitoringProviders: "monitoring-providers",
  logsLevel: "logs-level",
  logsDirectives: "logs-directives",
  // The tail button carries the action it offers.
  logsPauseTail: "logs-pause-tail",
  logsResumeTail: "logs-resume-tail",
  /** The tail's state note (following, paused, disconnected). */
  logsTail: (state: "following" | "paused" | "disconnected") => `logs-tail-${state}`,
  analyzerDrop: "analyzer-drop",
  analyzerReport: "analyzer-report",
  analyzerCopyReport: "analyzer-copy-report",
  analyzerDownloadJson: "analyzer-download-json",
} as const;
// testids:end
