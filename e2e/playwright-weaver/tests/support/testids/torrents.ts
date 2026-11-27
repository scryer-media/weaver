// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A copy of the web app's apps/weaver-web/src/next/testids/torrents.ts, between the
// markers. The package does not import across its boundary; the
// testids-pin check keeps the two equal.

// testids:begin
/** The Add dialog, the queue and a torrent job's screen. */
export const torrentIds = {
  // The Downloads top bar: the Add button, and the hold on every torrent,
  // whose button carries the action it offers.
  addOpen: "add-open",
  pauseTorrents: "torrents-pause",
  resumeTorrents: "torrents-resume",
  resumeTorrentsTimed: "torrents-resume-timed",

  // A torrent's peers: the headings that sort them, and the way between pages.
  peerHeadings: "torrent-peer-headings",
  peerSort: (column: string) => `torrent-peer-sort-${column}`,
  peerRange: "torrent-peer-range",
  peerPrevious: "torrent-peer-previous",
  peerNext: "torrent-peer-next",

  // The Add dialog's drop zone, link box, paused switch and submit.
  addDrop: "add-drop",
  addLink: "add-link",
  addPaused: "add-paused",
  addSubmit: "add-submit",

  // A queue row, keyed by the job's id, and its parts: the protocol and the
  // status by the product's value, the seeding figures, and the pause button,
  // which carries the action it offers. The parts are not under the row's
  // `queue-row-` prefix, which finds rows.
  queueRow: (jobId: string | number) => `queue-row-${jobId}`,
  queueRowPrefix: "queue-row-",
  queueRowName: "queue-entry-name",
  queueRowProtocol: (protocol: string) => `queue-entry-protocol-${protocol}`,
  queueRowStatus: (status: string) => `queue-entry-status-${status}`,
  queueRowSeeding: "queue-entry-seeding",
  // A seeding row's bars: the ratio goal over the finished download, and the seed-time goal.
  queueRowSeedingBars: "queue-entry-seeding-bars",
  queueRowSeedingGoal: "queue-entry-seeding-goal",
  queueRowPause: "queue-entry-pause",
  queueRowResume: "queue-entry-resume",

  // The queue inspector's link to the job's screen and its remove.
  inspectorOpenDetail: "inspector-open-detail",
  inspectorRemove: "inspector-remove",

  // A torrent job's header; each button carries the action it offers.
  jobPause: "torrent-job-pause",
  jobResume: "torrent-job-resume",
  filesMissingDialog: "torrent-files-missing",
  jobForce: "torrent-job-force",
  jobUnforce: "torrent-job-unforce",
  jobRemove: "torrent-job-remove",

  // A torrent's general sections, a row by its key.
  field: (key: string) => `torrent-field-${key}`,
  // A tracker row by its URL, and its status by the daemon's value.
  tracker: (url: string) => `torrent-tracker-${url}`,
  trackerStatus: (status: string) => `torrent-tracker-status-${status}`,
  // A file row, its path and its progress.
  file: "torrent-file",
  filePath: "torrent-file-path",
  fileProgress: "torrent-file-progress",
  // The way between pages of the files.
  fileRange: "torrent-file-range",
  filePrevious: "torrent-file-previous",
  fileNext: "torrent-file-next",
  // How its unpacking came out, a file its unpacking placed beside its own,
  // and the button that reads more of them.
  unpacking: "torrent-unpacking",
  unpackedFile: "torrent-unpacked-file",
  unpackedMore: "torrent-unpacked-more",
  // The seeding block: the override button, a term's value by the term, and
  // the level it comes from by the product's level.
  seedingOverride: "torrent-seeding-override",
  seedingTerm: (term: string) => `torrent-seeding-${term}`,
  seedingLevel: (term: string, level: string) => `torrent-seeding-${term}-level-${level}`,

  // The remove dialog's choice to delete the files too.
  removeDeleteFiles: "torrent-remove-delete-files",
} as const;
// testids:end
