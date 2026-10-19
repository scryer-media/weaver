// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/** The groups the schedule list is broken into, in the order it draws them. */
export const SCHEDULE_TRACKS = [
  { value: "DOWNLOADS", label: "next.schedules.trackDownloads" },
  { value: "TORRENTS", label: "next.schedules.trackTorrents" },
  { value: "POST_PROCESSING", label: "next.schedules.trackPost" },
  { value: "WATCH_FOLDER", label: "next.schedules.trackWatchFolder" },
  { value: "RSS", label: "next.schedules.trackRss" },
  { value: "SPEED", label: "next.schedules.trackSpeed" },
  { value: "SPLIT", label: "next.schedules.trackSplit" },
  { value: "PROFILE", label: "next.schedules.trackProfile" },
  { value: "QUOTA", label: "next.schedules.trackQuota" },
  { value: "SERVER", label: "next.schedules.trackServers" },
  { value: "ONE_SHOT", label: "next.schedules.trackOneShot" },
] as const;

export type ScheduleTrack = (typeof SCHEDULE_TRACKS)[number]["value"];
