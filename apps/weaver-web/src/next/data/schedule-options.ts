import type { Translate } from "@/lib/context/translate-context";
import { formatRate } from "./format";

/**
 * Every action a rule can take, in the order the action picker offers them:
 * the holds first, each pause beside its resume, then the settings a rule
 * switches, then pruning.
 */
export const SCHEDULE_ACTIONS = [
  { value: "pause", label: "next.schedules.pause" },
  { value: "resume", label: "next.schedules.resume" },
  { value: "pause_all", label: "next.schedules.pauseAll" },
  { value: "pause_post_processing", label: "next.schedules.pausePost" },
  { value: "resume_post_processing", label: "next.schedules.resumePost" },
  { value: "pause_watch_folder_scanning", label: "next.schedules.pauseWatchFolder" },
  { value: "resume_watch_folder_scanning", label: "next.schedules.resumeWatchFolder" },
  { value: "pause_rss", label: "next.schedules.pauseRss" },
  { value: "resume_rss", label: "next.schedules.resumeRss" },
  { value: "speed_limit", label: "next.schedules.setLimit" },
  { value: "hardware_profile", label: "next.schedules.setProfile" },
  { value: "set_server_active", label: "next.schedules.serverActive" },
  { value: "prune_history", label: "next.schedules.pruneHistory" },
  { value: "set_quota_metering", label: "next.schedules.quotaMetering" },
] as const;

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

/** What one change of a speed rule applies to. */
export type SpeedTargetKind = "GLOBAL" | "EGRESS" | "SERVER";

/** One target of a speed rule and the rate it sets; 0 removes the limit there. */
export interface ScheduleSpeedLimit {
  kind: SpeedTargetKind;
  /** The egress or provider; null for the global limit. */
  id: number | null;
  bytesPerSec: number;
}

export interface ScheduleOptions {
  times: string[];
  everyHourAtMinute: number | null;
  serverId: number | null;
  serverActive: boolean | null;
  quotaMeteringEnabled: boolean | null;
  /** The egress a quota rule applies to; null for every egress. */
  quotaEgressId: number | null;
  pruneFailed: { deleteFiles: boolean } | null;
  pruneCompleted: { deleteFiles: boolean } | null;
  pruneCancelled: { deleteFiles: boolean } | null;
  speedLimits: ScheduleSpeedLimit[];
}

export interface ScheduleOptionsForm extends Omit<ScheduleOptions, "times" | "speedLimits"> {
  timesText: string;
  /**
   * The rate typed for each target, in MB/s, keyed by {@link speedKey}. Blank
   * leaves that target as it is.
   */
  speeds: Record<string, string>;
}

export interface ScheduleTargets {
  servers: { id: number; host: string }[];
  egressInterfaces: { id: number; name: string }[];
}

const MIB = 1024 * 1024;

/** The form's key for one speed target. */
export function speedKey(kind: SpeedTargetKind, id: number | null): string {
  return kind === "GLOBAL" ? "GLOBAL" : `${kind}:${id}`;
}

/** A stored rate as its field shows it: MB/s, to two places. */
function rateText(bytesPerSec: number): string {
  return String(Math.round((bytesPerSec / MIB) * 100) / 100);
}

/** Why a typed rate cannot be saved, as a translation key; null when it can. */
export function speedProblem(text: string): string | null {
  const trimmed = text.trim();
  if (trimmed === "") return null;
  const value = Number(trimmed);
  return Number.isFinite(value) && value >= 0 ? null : "next.schedules.speedInvalid";
}

/**
 * The changes a speed form makes, global first, then each egress, then each
 * provider. A target left blank is left out. A rate the field only rounded for
 * display goes back exactly as it was stored.
 */
export function speedLimitsInput(
  speeds: Record<string, string>,
  stored: readonly ScheduleSpeedLimit[] = [],
): ScheduleSpeedLimit[] {
  const order = (kind: SpeedTargetKind) => (kind === "GLOBAL" ? 0 : kind === "EGRESS" ? 1 : 2);
  return Object.entries(speeds)
    .filter(([, text]) => text.trim() !== "" && speedProblem(text) === null)
    .map(([key, text]): ScheduleSpeedLimit => {
      const [kind, id] = key.split(":") as [SpeedTargetKind, string | undefined];
      const target = { kind, id: id === undefined ? null : Number(id) };
      const saved = stored.find((limit) => limit.kind === target.kind && limit.id === target.id);
      const bytesPerSec =
        saved && rateText(saved.bytesPerSec) === String(Number(text.trim()))
          ? saved.bytesPerSec
          : Math.round(Number(text.trim()) * MIB);
      return { ...target, bytesPerSec };
    })
    .sort((left, right) => order(left.kind) - order(right.kind) || (left.id ?? 0) - (right.id ?? 0));
}

/** A speed rule in the table's words: "Global 5 MB/s, wan 2 MB/s", with "unlimited" for a removed limit. */
export function speedLimitsLabel(
  t: Translate,
  limits: readonly ScheduleSpeedLimit[],
  targets?: ScheduleTargets,
): string {
  return limits
    .map((limit) => {
      const name =
        limit.kind === "GLOBAL"
          ? t("next.schedules.speedGlobal")
          : limit.kind === "EGRESS"
            ? targets?.egressInterfaces.find((egress) => egress.id === limit.id)?.name ??
              `${t("next.schedules.egress")} #${limit.id}`
            : targets?.servers.find((server) => server.id === limit.id)?.host ??
              `${t("next.schedules.server")} #${limit.id}`;
      const rate = limit.bytesPerSec > 0 ? formatRate(limit.bytesPerSec) : t("next.schedules.unlimited");
      return `${name} ${rate}`;
    })
    .join(", ");
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
    case "set_quota_metering": {
      const egress =
        schedule.quotaEgressId === null
          ? null
          : targets?.egressInterfaces.find((entry) => entry.id === schedule.quotaEgressId)?.name ??
            `${t("next.schedules.egress")} #${schedule.quotaEgressId}`;
      const action = t(schedule.quotaMeteringEnabled ? "next.schedules.quotaCountOn" : "next.schedules.quotaCountOff");
      return egress ? `${action}: ${egress}` : action;
    }
    case "speed_limit":
      return schedule.speedLimits.length > 0
        ? speedLimitsLabel(t, schedule.speedLimits, targets)
        : t("next.schedules.speedNothing");
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
  serverId: null, serverActive: true, quotaMeteringEnabled: true, quotaEgressId: null,
  pruneFailed: null, pruneCompleted: null, pruneCancelled: null,
  speeds: {},
};

/** Only pruning runs once each time rather than holding until the next rule. */
export const isOneShot = (action: string) => action === "prune_history";

export function optionsFromSchedule(schedule: ScheduleOptions): ScheduleOptionsForm {
  const { times, speedLimits, ...rest } = schedule;
  return {
    ...rest,
    timesText: (times ?? []).join(", "),
    serverActive: schedule.serverActive ?? true,
    quotaMeteringEnabled: schedule.quotaMeteringEnabled ?? true,
    speeds: Object.fromEntries(
      (speedLimits ?? []).map((limit) => [speedKey(limit.kind, limit.id), rateText(limit.bytesPerSec)]),
    ),
  };
}

export function optionsInput(form: ScheduleOptionsForm, action: string, stored: readonly ScheduleSpeedLimit[] = []) {
  const hourly = isOneShot(action) ? form.everyHourAtMinute : null;
  // A speed rule takes the one time of day.
  const listed = hourly === null && action !== "speed_limit";
  return {
    times: listed ? form.timesText.split(",").map((time) => time.trim()).filter(Boolean) : [],
    everyHourAtMinute: hourly,
    serverId: action === "set_server_active" ? form.serverId : null,
    serverActive: action === "set_server_active" ? form.serverActive : null,
    quotaMeteringEnabled: action === "set_quota_metering" ? form.quotaMeteringEnabled : null,
    quotaEgressId: action === "set_quota_metering" ? form.quotaEgressId : null,
    pruneFailed: action === "prune_history" ? form.pruneFailed : null,
    pruneCompleted: action === "prune_history" ? form.pruneCompleted : null,
    pruneCancelled: action === "prune_history" ? form.pruneCancelled : null,
    speedLimits: action === "speed_limit" ? speedLimitsInput(form.speeds, stored) : null,
  };
}

export function scheduleTimeLabel(schedule: ScheduleOptions & { time: string }): string {
  return schedule.everyHourAtMinute !== null && schedule.everyHourAtMinute !== undefined
    ? `*:${String(schedule.everyHourAtMinute).padStart(2, "0")}`
    : schedule.times?.length ? schedule.times.join(", ") : schedule.time;
}
