import type { Translate } from "@/lib/context/translate-context";

export const ADDITIONAL_SCHEDULE_ACTIONS = [
  { value: "pause_all", label: "next.schedules.pauseAll" },
  { value: "pause_post_processing", label: "next.schedules.pausePost" },
  { value: "resume_post_processing", label: "next.schedules.resumePost" },
  { value: "set_server_active", label: "next.schedules.serverActive" },
  { value: "set_quota_metering", label: "next.schedules.quotaMetering" },
  { value: "scan_watch_folder", label: "next.schedules.scanNow" },
  { value: "fetch_rss", label: "next.schedules.fetchRss" },
  { value: "prune_history", label: "next.schedules.pruneHistory" },
];

/** The weekdays a rule may be narrowed to. Labels are translation keys. */
export const SCHEDULE_DAYS = [
  { key: "mon", label: "next.weekday.monShort" },
  { key: "tue", label: "next.weekday.tueShort" },
  { key: "wed", label: "next.weekday.wedShort" },
  { key: "thu", label: "next.weekday.thuShort" },
  { key: "fri", label: "next.weekday.friShort" },
  { key: "sat", label: "next.weekday.satShort" },
  { key: "sun", label: "next.weekday.sunShort" },
];

/** The days a rule runs on; none chosen, like all of them, is every day. */
export function scheduleDaysLabel(t: Translate, days: readonly string[]): string {
  if (days.length === 0 || days.length === SCHEDULE_DAYS.length) {
    return t("next.schedules.everyDay");
  }
  return SCHEDULE_DAYS.filter((day) => days.includes(day.key))
    .map((day) => t(day.label))
    .join(" ");
}

export interface ScheduleOptions {
  times: string[];
  everyHourAtMinute: number | null;
  serverId: number | null;
  serverActive: boolean | null;
  feedId: number | null;
  quotaMeteringEnabled: boolean | null;
  pruneFailed: { deleteFiles: boolean } | null;
  pruneCompleted: { deleteFiles: boolean } | null;
  pruneCancelled: { deleteFiles: boolean } | null;
}

export interface ScheduleOptionsForm extends Omit<ScheduleOptions, "times"> {
  timesText: string;
}

export interface ScheduleTargets {
  servers: { id: number; host: string }[];
  rssFeeds: { id: number; name: string }[];
  /** Every script instance; a rule can only run one whose trigger is the schedule. */
  scriptInstances?: { id: string; name: string; script: string; trigger: string }[];
}

/** The script instances a schedule rule can run. */
export function scheduleInstances(targets: ScheduleTargets | undefined) {
  return (targets?.scriptInstances ?? []).filter((instance) => instance.trigger === "SCHEDULER");
}

export function scheduleActionDetails(
  t: Translate,
  schedule: ScheduleOptions & { actionType: string },
  targets?: ScheduleTargets,
): string | null {
  const state = (enabled: boolean | null) => t(enabled ? "next.common.on" : "next.common.off");
  switch (schedule.actionType) {
    case "set_server_active": {
      const server = targets?.servers.find((entry) => entry.id === schedule.serverId);
      return `${t("next.schedules.serverActive")}: ${server?.host ?? `${t("next.schedules.server")} #${schedule.serverId}`} (${state(schedule.serverActive)})`;
    }
    case "set_quota_metering":
      return `${t("next.schedules.quotaMetering")}: ${state(schedule.quotaMeteringEnabled)}`;
    case "fetch_rss": {
      const feed = targets?.rssFeeds.find((entry) => entry.id === schedule.feedId);
      const target = schedule.feedId === null ? t("next.schedules.allFeeds") : feed?.name ?? `${t("next.schedules.feed")} #${schedule.feedId}`;
      return `${t("next.schedules.fetchRss")}: ${target}`;
    }
    case "prune_history": {
      const choices = (["pruneFailed", "pruneCompleted", "pruneCancelled"] as const)
        .filter((key) => schedule[key] !== null)
        .map((key) => `${t(`next.schedules.${key}`)} (${t("next.schedules.deleteFiles")}: ${state(schedule[key]!.deleteFiles)})`);
      return `${t("next.schedules.pruneHistory")}: ${choices.join(", ")}`;
    }
    default:
      return null;
  }
}

export const NEW_SCHEDULE_OPTIONS: ScheduleOptionsForm = {
  timesText: "", everyHourAtMinute: null,
  serverId: null, serverActive: true, feedId: null, quotaMeteringEnabled: true,
  pruneFailed: null, pruneCompleted: null, pruneCancelled: null,
};

export const isOneShot = (action: string) => ["scan_watch_folder", "fetch_rss", "prune_history"].includes(action);

export function optionsFromSchedule(schedule: ScheduleOptions): ScheduleOptionsForm {
  return {
    ...schedule,
    timesText: (schedule.times ?? []).join(", "),
    serverActive: schedule.serverActive ?? true,
    quotaMeteringEnabled: schedule.quotaMeteringEnabled ?? true,
  };
}

export function optionsInput(form: ScheduleOptionsForm, action: string) {
  const hourly = isOneShot(action) ? form.everyHourAtMinute : null;
  // A script rule lists its times in the time field, and the editor offers it no second list.
  const listed = hourly === null && action !== "run_script";
  return {
    times: listed ? form.timesText.split(",").map((time) => time.trim()).filter(Boolean) : [],
    everyHourAtMinute: hourly,
    serverId: action === "set_server_active" ? form.serverId : null,
    serverActive: action === "set_server_active" ? form.serverActive : null,
    feedId: action === "fetch_rss" ? form.feedId : null,
    quotaMeteringEnabled: action === "set_quota_metering" ? form.quotaMeteringEnabled : null,
    pruneFailed: action === "prune_history" ? form.pruneFailed : null,
    pruneCompleted: action === "prune_history" ? form.pruneCompleted : null,
    pruneCancelled: action === "prune_history" ? form.pruneCancelled : null,
  };
}

export function scheduleTimeLabel(schedule: ScheduleOptions & { time: string }): string {
  return schedule.everyHourAtMinute !== null && schedule.everyHourAtMinute !== undefined
    ? `*:${String(schedule.everyHourAtMinute).padStart(2, "0")}`
    : schedule.times?.length ? schedule.times.join(", ") : schedule.time;
}
