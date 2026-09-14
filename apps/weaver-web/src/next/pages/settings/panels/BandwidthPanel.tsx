import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import { SETTINGS_QUERY, UPDATE_SETTINGS_MUTATION } from "@/graphql/queries";
import type { DownloadBlockState } from "@/lib/context/live-data-context";
import { useTranslate } from "@/lib/context/translate-context";
import { Bar, KeyValueRow } from "../../../components/chrome";
import { WV } from "../../../data/palette";
import { formatDate, formatSize } from "../../../data/format";
import {
  SettingsBlocks,
  useDraft,
  usePanelState,
  type FieldSpec,
  type SettingsBlock,
} from "../framework";

/**
 * Bandwidth: the global download ceiling and the ISP cap window.
 *
 * The current-window block reads `globalQueueState.downloadBlock`, which is
 * the same counter the status bar and the rail watch, so what this panel shows
 * and what the daemon is enforcing can never disagree.
 */

const MIB = 1024 ** 2;
const GIB = 1024 ** 3;
const TIB = 1024 ** 4;

type CapPeriod = "DAILY" | "WEEKLY" | "MONTHLY";
type Weekday = "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN";

interface IspBandwidthCap {
  enabled: boolean;
  period: CapPeriod;
  limitBytes: number;
  resetTimeMinutesLocal: number;
  weeklyResetWeekday: Weekday;
  monthlyResetDay: number;
}

interface BandwidthDraft {
  maxDownloadSpeed: number;
  cap: IspBandwidthCap;
  /** The limit is edited as a number plus a unit, then folded back to bytes. */
  limitValue: string;
  limitUnit: "GB" | "TB";
  resetTime: string;
}

const DEFAULT_CAP: IspBandwidthCap = {
  enabled: false,
  period: "MONTHLY",
  limitBytes: 0,
  resetTimeMinutesLocal: 0,
  weeklyResetWeekday: "MON",
  monthlyResetDay: 1,
};

/** Option labels are translation keys, resolved when the panel renders. */
const PERIODS: { value: string; label: string }[] = [
  { value: "DAILY", label: "next.bandwidth.daily" },
  { value: "WEEKLY", label: "next.bandwidth.weekly" },
  { value: "MONTHLY", label: "next.bandwidth.monthly" },
];

const WEEKDAYS: { value: string; label: string }[] = [
  { value: "MON", label: "next.weekday.mon" },
  { value: "TUE", label: "next.weekday.tue" },
  { value: "WED", label: "next.weekday.wed" },
  { value: "THU", label: "next.weekday.thu" },
  { value: "FRI", label: "next.weekday.fri" },
  { value: "SAT", label: "next.weekday.sat" },
  { value: "SUN", label: "next.weekday.sun" },
];

const UNITS: { value: string; label: string }[] = [
  { value: "GB", label: "GB" },
  { value: "TB", label: "TB" },
];

function minutesToTime(minutes: number): string {
  const clamped = Math.max(0, Math.min(23 * 60 + 59, Math.round(minutes)));
  return `${String(Math.floor(clamped / 60)).padStart(2, "0")}:${String(clamped % 60).padStart(2, "0")}`;
}

function timeToMinutes(raw: string): number {
  const [hours, minutes] = raw.split(":").map(Number);
  if (!Number.isInteger(hours) || !Number.isInteger(minutes)) {
    return 0;
  }
  return Math.max(0, Math.min(23 * 60 + 59, hours * 60 + minutes));
}

function trimNumber(value: number): string {
  return String(Number(value.toFixed(4)));
}

export function BandwidthPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{
    settings: { maxDownloadSpeed: number; ispBandwidthCap: IspBandwidthCap | null };
    globalState: { downloadBlock: DownloadBlockState } | null;
  }>({ query: SETTINGS_QUERY });
  const [updateState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const source = useMemo<BandwidthDraft | null>(() => {
    const settings = data?.settings;
    if (!settings) {
      return null;
    }
    const cap = settings.ispBandwidthCap ?? DEFAULT_CAP;
    const unit = cap.limitBytes >= TIB ? "TB" : "GB";
    return {
      maxDownloadSpeed: settings.maxDownloadSpeed ?? 0,
      cap,
      limitValue: cap.limitBytes === 0 ? "" : trimNumber(cap.limitBytes / (unit === "TB" ? TIB : GIB)),
      limitUnit: unit,
      resetTime: minutesToTime(cap.resetTimeMinutesLocal),
    };
  }, [data?.settings]);

  const draft = useDraft(source, () => {
    setStatus(null);
    setError(null);
  });
  const values = draft.value;

  usePanelState({
    dirty: draft.dirty,
    busy: updateState.fetching,
    status: error ?? status,
    failed: error !== null,
    revert: () => {
      setError(null);
      draft.revert();
    },
    save: () => {
      if (!values) {
        return;
      }
      const unitBytes = values.limitUnit === "TB" ? TIB : GIB;
      const limitBytes = Math.max(0, Math.round(Number(values.limitValue || 0) * unitBytes));
      if (values.cap.enabled && limitBytes <= 0) {
        setError(t("next.bandwidth.capSizeRequired"));
        return;
      }
      setError(null);
      void updateSettings({
        input: {
          maxDownloadSpeed: values.maxDownloadSpeed,
          ispBandwidthCap: {
            enabled: values.cap.enabled,
            period: values.cap.period,
            limitBytes,
            resetTimeMinutesLocal: timeToMinutes(values.resetTime),
            weeklyResetWeekday: values.cap.weeklyResetWeekday,
            monthlyResetDay: Math.min(31, Math.max(1, Math.trunc(values.cap.monthlyResetDay))),
          },
        },
      }).then((result) => {
        if (result.error || !result.data?.updateSettings) {
          setError(result.error?.message ?? t("next.bandwidth.saveFailed"));
          return;
        }
        draft.markSaved();
        setStatus(t("next.settings.saved"));
        void reexecute({ requestPolicy: "network-only" });
      });
    },
  });

  // A server quota block carries placeholder ISP counters, so it must not be
  // presented as this window's usage.
  const block = data?.globalState?.downloadBlock;
  const ispBlock = block && block.kind !== "SERVER_QUOTA" ? block : undefined;

  const limitFields: FieldSpec[] = values
    ? [
        {
          id: "maxDownloadSpeed",
          label: t("next.bandwidth.ceiling"),
          help: t("next.bandwidth.ceilingHelp"),
          keywords: "speed limit throttle rate",
          // The same field as the speed-limit dialog, so the ceiling reads and
          // edits identically wherever it is set.
          control: {
            kind: "number",
            value: Math.round((values.maxDownloadSpeed / MIB) * 10) / 10,
            min: 0,
            onChange: (next) => draft.set({ maxDownloadSpeed: Math.round(Math.max(0, next) * MIB) }),
            suffix: "MB/s",
          },
        },
        {
          id: "capEnabled",
          label: t("next.bandwidth.enforceCap"),
          help: t("next.bandwidth.enforceCapHelp"),
          keywords: "isp quota allowance metered",
          control: {
            kind: "toggle",
            value: values.cap.enabled,
            onChange: (next) => draft.set({ cap: { ...values.cap, enabled: next } }),
          },
        },
        {
          id: "capPeriod",
          collapsed: !values.cap.enabled,
          label: t("next.bandwidth.capWindow"),
          help: t("next.bandwidth.capWindowHelp"),
          keywords: "daily weekly monthly period",
          control: {
            kind: "segmented",
            value: values.cap.period,
            options: PERIODS.map((option) => ({ ...option, label: t(option.label) })),
            onChange: (next) => draft.set({ cap: { ...values.cap, period: next as CapPeriod } }),
          },
        },
        {
          id: "capLimit",
          collapsed: !values.cap.enabled,
          label: t("next.bandwidth.allowance"),
          help: t("next.bandwidth.allowanceHelp"),
          keywords: "cap size gb tb limit",
          control: {
            kind: "custom",
            control: (
              <div className="flex items-center gap-[10px]">
                <input
                  type="text"
                  inputMode="decimal"
                  aria-label={t("next.bandwidth.allowance")}
                  value={values.limitValue}
                  placeholder="0"
                  onChange={(event) => draft.set({ limitValue: event.target.value })}
                  className="h-[34px] w-[110px] border border-wv-control bg-wv-input px-3 font-wv-mono text-[12px] text-wv-fg outline-none focus:border-wv-control-focus"
                />
                <div className="flex border border-wv-control bg-wv-input">
                  {UNITS.map((unit, index) => (
                    <button
                      key={unit.value}
                      type="button"
                      aria-pressed={values.limitUnit === unit.value}
                      onClick={() => draft.set({ limitUnit: unit.value as "GB" | "TB" })}
                      className={`flex h-8 cursor-pointer items-center px-[13px] font-wv-mono text-[12px] ${
                        index > 0 ? "border-l border-wv-control " : ""
                      }${
                        values.limitUnit === unit.value
                          ? "bg-wv-segment-active font-medium text-wv-strong"
                          : "text-wv-muted hover:text-wv-strong"
                      }`}
                    >
                      {unit.label}
                    </button>
                  ))}
                </div>
              </div>
            ),
          },
        },
        {
          id: "resetTime",
          collapsed: !values.cap.enabled,
          label: t("next.bandwidth.resetAt"),
          help: t("next.bandwidth.resetAtHelp"),
          keywords: "clock hour",
          control: {
            kind: "time",
            value: values.resetTime,
            onChange: (next) => draft.set({ resetTime: next }),
          },
        },
        ...(values.cap.period === "WEEKLY"
          ? [
              {
                id: "weeklyResetWeekday",
                collapsed: !values.cap.enabled,
                label: t("next.bandwidth.resetDay"),
                help: t("next.bandwidth.resetDayHelp"),
                control: {
                  kind: "select" as const,
                  value: values.cap.weeklyResetWeekday,
                  options: WEEKDAYS.map((option) => ({ ...option, label: t(option.label) })),
                  onChange: (next: string) =>
                    draft.set({ cap: { ...values.cap, weeklyResetWeekday: next as Weekday } }),
                },
              },
            ]
          : []),
        ...(values.cap.period === "MONTHLY"
          ? [
              {
                id: "monthlyResetDay",
                collapsed: !values.cap.enabled,
                label: t("next.bandwidth.resetDayOfMonth"),
                help: t("next.bandwidth.resetDayOfMonthHelp"),
                control: {
                  kind: "number" as const,
                  value: values.cap.monthlyResetDay,
                  min: 1,
                  max: 31,
                  onChange: (next: number) =>
                    draft.set({ cap: { ...values.cap, monthlyResetDay: next } }),
                },
              },
            ]
          : []),
      ]
    : [];

  const usedPercent =
    ispBlock && ispBlock.limitBytes > 0 ? (ispBlock.usedBytes / ispBlock.limitBytes) * 100 : 0;

  const blocks: (SettingsBlock | null)[] = [
    values ? { kind: "section", id: "limits", title: t("next.bandwidth.limits"), fields: limitFields } : null,
    ispBlock?.capEnabled
      ? {
          kind: "custom",
          id: "window",
          title: t("next.bandwidth.currentWindow"),
          note: ispBlock.timezoneName || undefined,
          searchText: "current window usage cap remaining resets",
          body: (
            <>
              <div className="flex flex-col gap-2 border-b border-wv-hairline px-4 sm:px-6 py-[14px]">
                <Bar
                  percent={usedPercent}
                  color={usedPercent >= 90 ? WV.error : usedPercent >= 70 ? WV.warn : WV.accent}
                />
                <div className="flex items-baseline justify-between font-wv-mono text-[11.5px] text-wv-muted">
                  <span>{t("next.bandwidth.used", { size: formatSize(ispBlock.usedBytes) })}</span>
                  <span>{t("next.monitoring.allowance", { size: formatSize(ispBlock.limitBytes) })}</span>
                </div>
              </div>
              <KeyValueRow label={t("next.monitoring.remaining")} value={formatSize(ispBlock.remainingBytes)} />
              <KeyValueRow
                label={t("next.monitoring.reserved")}
                value={formatSize(ispBlock.reservedBytes)}
              />
              <KeyValueRow
                label={t("next.monitoring.windowResets")}
                value={formatDate(ispBlock.windowEndsAtEpochMs)}
              />
              <KeyValueRow
                label={t("next.monitoring.heldByCap")}
                value={ispBlock.kind === "ISP_CAP" ? t("next.common.yes") : t("next.common.no")}
              />
            </>
          ),
        }
      : null,
  ];

  return <SettingsBlocks blocks={blocks} loading={fetching && !data} />;
}
