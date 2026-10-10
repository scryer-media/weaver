import type { Translate } from "@/lib/context/translate-context";
import type { DownloadQuota, QuotaPeriod, Weekday } from "@/lib/networking";
import { formatSize } from "../../data/format";
import type { FieldSpec } from "./framework";

/**
 * A download allowance in an editor. Providers and egresses carry the same
 * quota, so both edit it with these fields.
 */

const GIB = 1024 ** 3;
const TIB = 1024 ** 4;

export const EMPTY_QUOTA: DownloadQuota = {
  enabled: false,
  period: "MONTHLY",
  limitBytes: 0,
  resetTimeMinutesLocal: 0,
  weeklyResetWeekday: "MON",
  monthlyResetDay: 1,
};

/** A quota as its editor holds it: the allowance is a number plus a unit, the reset a clock time. */
export interface QuotaDraft {
  quota: DownloadQuota;
  limit: string;
  unit: "GB" | "TB";
  resetTime: string;
}

/** Labels are translation keys, resolved when the fields render. */
const QUOTA_PERIODS: { value: QuotaPeriod; label: string }[] = [
  { value: "ONE_TIME", label: "next.providers.oneBlock" },
  { value: "DAILY", label: "next.bandwidth.daily" },
  { value: "WEEKLY", label: "next.bandwidth.weekly" },
  { value: "MONTHLY", label: "next.bandwidth.monthly" },
];

const WEEKDAYS: { value: Weekday; label: string }[] = [
  { value: "MON", label: "next.weekday.mon" },
  { value: "TUE", label: "next.weekday.tue" },
  { value: "WED", label: "next.weekday.wed" },
  { value: "THU", label: "next.weekday.thu" },
  { value: "FRI", label: "next.weekday.fri" },
  { value: "SAT", label: "next.weekday.sat" },
  { value: "SUN", label: "next.weekday.sun" },
];

export function minutesToTime(minutes: number): string {
  const clamped = Math.max(0, Math.min(23 * 60 + 59, Math.round(minutes)));
  return `${String(Math.floor(clamped / 60)).padStart(2, "0")}:${String(clamped % 60).padStart(2, "0")}`;
}

export function timeToMinutes(raw: string): number {
  const [hours, minutes] = raw.split(":").map(Number);
  if (!Number.isInteger(hours) || !Number.isInteger(minutes)) {
    return 0;
  }
  return Math.max(0, Math.min(23 * 60 + 59, hours * 60 + minutes));
}

export function trimNumber(value: number): string {
  return String(Number(value.toFixed(4)));
}

export function quotaDraft(quota: DownloadQuota | null | undefined): QuotaDraft {
  const saved = quota ?? EMPTY_QUOTA;
  const unit = saved.limitBytes >= TIB ? "TB" : "GB";
  return {
    quota: saved,
    limit: saved.limitBytes === 0 ? "" : trimNumber(saved.limitBytes / (unit === "TB" ? TIB : GIB)),
    unit,
    resetTime: minutesToTime(saved.resetTimeMinutesLocal),
  };
}

/** The quota as the API takes it. */
export function quotaInput(draft: QuotaDraft): DownloadQuota {
  const unitBytes = draft.unit === "TB" ? TIB : GIB;
  return {
    enabled: draft.quota.enabled,
    limitBytes: Math.max(0, Math.round(Number(draft.limit || 0) * unitBytes)),
    period: draft.quota.period,
    resetTimeMinutesLocal: timeToMinutes(draft.resetTime),
    weeklyResetWeekday: draft.quota.weeklyResetWeekday,
    monthlyResetDay: Math.min(31, Math.max(1, Math.trunc(draft.quota.monthlyResetDay))),
  };
}

/** Why a switched-on quota cannot be saved, or null when it can. */
export function quotaProblem(t: Translate, draft: QuotaDraft): string | null {
  return draft.quota.enabled && quotaInput(draft).limitBytes <= 0 ? t("next.quota.sizeRequired") : null;
}

/**
 * The switch and, while it is on, the window, allowance and reset fields.
 * `usedBytes` is what the current window has spent, when it is known.
 */
export function quotaFields(
  t: Translate,
  draft: QuotaDraft,
  onChange: (next: QuotaDraft) => void,
  options: { label: string; help: string; usedBytes?: number },
): FieldSpec[] {
  const patchQuota = (next: Partial<DownloadQuota>) => onChange({ ...draft, quota: { ...draft.quota, ...next } });
  const toggle: FieldSpec = {
    id: "quotaEnabled",
    label: options.label,
    help: options.help,
    keywords: "quota allowance metered cap",
    control: { kind: "toggle", value: draft.quota.enabled, onChange: (enabled) => patchQuota({ enabled }) },
  };
  if (!draft.quota.enabled) {
    return [toggle];
  }
  return [
    toggle,
    {
      id: "quotaPeriod",
      label: t("next.providers.quotaWindow"),
      control: {
        kind: "select",
        value: draft.quota.period,
        options: QUOTA_PERIODS.map((option) => ({ ...option, label: t(option.label) })),
        onChange: (next) => patchQuota({ period: next as QuotaPeriod }),
      },
    },
    {
      id: "quotaLimit",
      label: t("next.bandwidth.allowance"),
      help:
        options.usedBytes === undefined
          ? undefined
          : t("next.providers.usedSoFar", { size: formatSize(options.usedBytes) }),
      control: {
        kind: "custom",
        control: (
          <div className="flex items-center gap-[10px]">
            <input
              type="text"
              inputMode="decimal"
              aria-label={t("next.bandwidth.allowance")}
              value={draft.limit}
              placeholder="0"
              onChange={(event) => onChange({ ...draft, limit: event.target.value })}
              className="h-[34px] w-[110px] border border-wv-control bg-wv-input px-3 font-wv-mono text-[12px] text-wv-fg outline-none focus:border-wv-control-focus"
            />
            <div className="flex border border-wv-control bg-wv-input">
              {(["GB", "TB"] as const).map((unit, index) => (
                <button
                  key={unit}
                  type="button"
                  aria-pressed={draft.unit === unit}
                  onClick={() => onChange({ ...draft, unit })}
                  className={`flex h-8 cursor-pointer items-center px-[13px] font-wv-mono text-[12px] ${
                    index > 0 ? "border-l border-wv-control " : ""
                  }${
                    draft.unit === unit
                      ? "bg-wv-segment-active font-medium text-wv-strong"
                      : "text-wv-muted hover:text-wv-strong"
                  }`}
                >
                  {unit}
                </button>
              ))}
            </div>
          </div>
        ),
      },
    },
    ...(draft.quota.period === "ONE_TIME"
      ? []
      : [
          {
            id: "quotaResetTime",
            label: t("next.bandwidth.resetAt"),
            control: {
              kind: "time" as const,
              value: draft.resetTime,
              onChange: (resetTime: string) => onChange({ ...draft, resetTime }),
            },
          },
        ]),
    ...(draft.quota.period === "WEEKLY"
      ? [
          {
            id: "quotaWeekday",
            label: t("next.bandwidth.resetDay"),
            control: {
              kind: "select" as const,
              value: draft.quota.weeklyResetWeekday,
              options: WEEKDAYS.map((option) => ({ ...option, label: t(option.label) })),
              onChange: (next: string) => patchQuota({ weeklyResetWeekday: next as Weekday }),
            },
          },
        ]
      : []),
    ...(draft.quota.period === "MONTHLY"
      ? [
          {
            id: "quotaMonthDay",
            label: t("next.bandwidth.resetDayOfMonth"),
            control: {
              kind: "number" as const,
              value: draft.quota.monthlyResetDay,
              min: 1,
              max: 31,
              onChange: (monthlyResetDay: number) => patchQuota({ monthlyResetDay }),
            },
          },
        ]
      : []),
  ];
}
