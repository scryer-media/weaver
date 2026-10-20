// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import { usePasswordManagerIgnore } from "@/lib/password-manager";
import type { DownloadQuota, QuotaPeriod, Weekday } from "@/lib/networking";
import { UnitStrip } from "../../components/controls";
import { formatSize } from "../../data/format";
import type { FieldSpec } from "./framework";

/**
 * A download allowance in an editor. Providers and egresses carry the same
 * quota, so both edit it with these fields.
 */

/** The allowance as typed: a decimal number, kept as text until it is saved. */
function AllowanceInput({
  label,
  value,
  onChange,
}: {
  label: string;
  value: string;
  onChange: (next: string) => void;
}) {
  const ignore = usePasswordManagerIgnore({ type: "text" });
  return (
    <input
      {...ignore}
      type="text"
      inputMode="decimal"
      aria-label={label}
      value={value}
      placeholder="0"
      onChange={(event) => onChange(event.target.value)}
      className="h-[34px] w-[110px] border border-wv-control bg-wv-input px-3 font-wv-mono text-[12px] text-wv-fg outline-none focus:border-wv-control-focus"
    />
  );
}

const ALLOWANCE_UNITS = [
  { value: "GB", label: "GB" },
  { value: "TB", label: "TB" },
] as const;

/** An allowance as typed, and the unit it is typed in. */
export function AllowanceControl({
  label,
  limit,
  unit,
  onChange,
}: {
  label: string;
  limit: string;
  unit: AllowanceUnit;
  onChange: (next: { limit: string; unit: AllowanceUnit }) => void;
}) {
  return (
    <div className="flex items-center gap-[10px]">
      <AllowanceInput label={label} value={limit} onChange={(next) => onChange({ limit: next, unit })} />
      <UnitStrip options={ALLOWANCE_UNITS} value={unit} onChange={(next) => onChange({ limit, unit: next })} />
    </div>
  );
}

export type AllowanceUnit = "GB" | "TB";

const GIB = 1024 ** 3;
const TIB = 1024 ** 4;

/** A size in bytes as an allowance is typed: a number, and the unit it reads best in. */
export function allowanceText(bytes: number): { limit: string; unit: AllowanceUnit } {
  const unit = bytes >= TIB ? "TB" : "GB";
  return { limit: trimNumber(bytes / (unit === "TB" ? TIB : GIB)), unit };
}

/**
 * An allowance as typed, in bytes. Text that is not a number, or a number too
 * large to send exactly, is null: no allowance at all.
 */
export function allowanceBytes(limit: string, unit: AllowanceUnit): number | null {
  if (!limit.trim()) {
    return null;
  }
  const bytes = Math.round(Number(limit) * (unit === "TB" ? TIB : GIB));
  return Number.isSafeInteger(bytes) && bytes >= 0 ? bytes : null;
}

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
  unit: AllowanceUnit;
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
  // Text that is not a number, or a number too large to send exactly, is no
  // allowance at all: a switched-on quota then reports it as missing, and a
  // switched-off one still saves.
  const bytes = Math.round(Number(draft.limit || 0) * unitBytes);
  return {
    enabled: draft.quota.enabled,
    limitBytes: Number.isSafeInteger(bytes) ? Math.max(0, bytes) : 0,
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
    periodField(t, draft.quota.period, (period) => patchQuota({ period })),
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
          <AllowanceControl
            label={t("next.bandwidth.allowance")}
            limit={draft.limit}
            unit={draft.unit}
            onChange={(next) => onChange({ ...draft, ...next })}
          />
        ),
      },
    },
    ...windowFields(
      t,
      {
        period: draft.quota.period,
        resetTime: draft.resetTime,
        weeklyResetWeekday: draft.quota.weeklyResetWeekday,
        monthlyResetDay: draft.quota.monthlyResetDay,
      },
      ({ resetTime, ...quota }) =>
        onChange({ ...draft, resetTime: resetTime ?? draft.resetTime, quota: { ...draft.quota, ...quota } }),
    ),
  ];
}

/** When an allowance's window resets, as its editor holds it. */
export interface QuotaWindow {
  period: QuotaPeriod;
  resetTime: string;
  weeklyResetWeekday: Weekday;
  monthlyResetDay: number;
}

/** The window's period picker, as a field. */
export function periodField(t: Translate, period: QuotaPeriod, onChange: (period: QuotaPeriod) => void): FieldSpec {
  return {
    id: "quotaPeriod",
    label: t("next.providers.quotaWindow"),
    control: {
      kind: "select",
      value: period,
      options: QUOTA_PERIODS.map((option) => ({ ...option, label: t(option.label) })),
      onChange: (next) => onChange(next as QuotaPeriod),
    },
  };
}

/** The fields that say when a window of `window.period` resets. A one-time block never does. */
export function windowFields(
  t: Translate,
  window: QuotaWindow,
  patch: (next: Partial<QuotaWindow>) => void,
): FieldSpec[] {
  return [
    ...(window.period === "ONE_TIME"
      ? []
      : [
          {
            id: "quotaResetTime",
            label: t("next.bandwidth.resetAt"),
            control: {
              kind: "time" as const,
              value: window.resetTime,
              onChange: (resetTime: string) => patch({ resetTime }),
            },
          },
        ]),
    ...(window.period === "WEEKLY"
      ? [
          {
            id: "quotaWeekday",
            label: t("next.bandwidth.resetDay"),
            control: {
              kind: "select" as const,
              value: window.weeklyResetWeekday,
              options: WEEKDAYS.map((option) => ({ ...option, label: t(option.label) })),
              onChange: (next: string) => patch({ weeklyResetWeekday: next as Weekday }),
            },
          },
        ]
      : []),
    ...(window.period === "MONTHLY"
      ? [
          {
            id: "quotaMonthDay",
            label: t("next.bandwidth.resetDayOfMonth"),
            control: {
              kind: "number" as const,
              value: window.monthlyResetDay,
              min: 1,
              max: 31,
              onChange: (monthlyResetDay: number) => patch({ monthlyResetDay }),
            },
          },
        ]
      : []),
  ];
}
