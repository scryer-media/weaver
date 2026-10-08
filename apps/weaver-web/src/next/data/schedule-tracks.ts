/** Each group of rules, and the action a rule added from that group starts on. */
export const SCHEDULE_TRACKS = [
  { value: "DOWNLOADS", label: "next.schedules.trackDownloads", action: "pause" },
  { value: "POST_PROCESSING", label: "next.schedules.trackPost", action: "pause_post_processing" },
  { value: "WATCH_FOLDER", label: "next.schedules.trackWatchFolder", action: "pause_watch_folder_scanning" },
  { value: "SPEED", label: "next.schedules.trackSpeed", action: "speed_limit" },
  { value: "PROFILE", label: "next.schedules.trackProfile", action: "hardware_profile" },
  { value: "QUOTA", label: "next.schedules.trackQuota", action: "set_quota_metering" },
  { value: "SERVER", label: "next.schedules.trackServers", action: "set_server_active" },
  { value: "ONE_SHOT", label: "next.schedules.trackOneShot", action: "scan_watch_folder" },
] as const;

export type ScheduleTrack = (typeof SCHEDULE_TRACKS)[number]["value"];
