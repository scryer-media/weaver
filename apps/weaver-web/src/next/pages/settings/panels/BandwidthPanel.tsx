import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import { SETTINGS_QUERY, UPDATE_SETTINGS_MUTATION } from "@/graphql/queries";
import type { DownloadBlockState } from "@/lib/context/live-data-context";
import { Bar, KeyValueRow } from "../../../components/chrome";
import { WV } from "../../../data/palette";
import { formatDate, formatRate, formatSize } from "../../../data/format";
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

const MAX_SPEED = 10 * 1024 * 1024 * 1024;
const SPEED_STEP = 1024 * 1024;
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

const PERIODS: { value: string; label: string }[] = [
  { value: "DAILY", label: "Daily" },
  { value: "WEEKLY", label: "Weekly" },
  { value: "MONTHLY", label: "Monthly" },
];

const WEEKDAYS: { value: string; label: string }[] = [
  { value: "MON", label: "Monday" },
  { value: "TUE", label: "Tuesday" },
  { value: "WED", label: "Wednesday" },
  { value: "THU", label: "Thursday" },
  { value: "FRI", label: "Friday" },
  { value: "SAT", label: "Saturday" },
  { value: "SUN", label: "Sunday" },
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
  const [{ data }, reexecute] = useQuery<{
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
        setError("Set a cap size before switching the cap on.");
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
          setError(result.error?.message ?? "Could not save the bandwidth settings.");
          return;
        }
        draft.markSaved();
        setStatus("Saved");
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
          label: "Download ceiling",
          help: "Applies immediately and persists across restarts. Zero means unlimited.",
          keywords: "speed limit throttle rate",
          control: {
            kind: "slider",
            value: Math.min(values.maxDownloadSpeed, MAX_SPEED),
            min: 0,
            max: MAX_SPEED,
            step: SPEED_STEP,
            onChange: (next) => draft.set({ maxDownloadSpeed: next }),
            display:
              values.maxDownloadSpeed === 0 ? "Unlimited" : formatRate(values.maxDownloadSpeed),
          },
        },
        {
          id: "capEnabled",
          label: "Enforce a data cap",
          help: "Stop downloading once the window's allowance is spent, and resume when it resets.",
          keywords: "isp quota allowance metered",
          control: {
            kind: "toggle",
            value: values.cap.enabled,
            onChange: (next) => draft.set({ cap: { ...values.cap, enabled: next } }),
          },
        },
        {
          id: "capPeriod",
          label: "Cap window",
          help: "How often the allowance resets.",
          keywords: "daily weekly monthly period",
          control: {
            kind: "segmented",
            value: values.cap.period,
            options: PERIODS,
            onChange: (next) => draft.set({ cap: { ...values.cap, period: next as CapPeriod } }),
          },
        },
        {
          id: "capLimit",
          label: "Allowance",
          help: "How much may be downloaded inside one window.",
          keywords: "cap size gb tb limit",
          control: {
            kind: "custom",
            control: (
              <div className="flex items-center gap-[10px]">
                <input
                  type="text"
                  inputMode="decimal"
                  aria-label="Allowance"
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
                          ? "bg-wv-segment-active font-semibold text-wv-strong"
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
          label: "Reset at",
          help: "Local time the window rolls over.",
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
                label: "Reset day",
                help: "Which day of the week the allowance rolls over.",
                control: {
                  kind: "select" as const,
                  value: values.cap.weeklyResetWeekday,
                  options: WEEKDAYS,
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
                label: "Reset day of month",
                help: "Months shorter than this day roll over on their last day.",
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
    values ? { kind: "section", id: "limits", title: "Limits", fields: limitFields } : null,
    ispBlock?.capEnabled
      ? {
          kind: "custom",
          id: "window",
          title: "Current window",
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
                  <span>{formatSize(ispBlock.usedBytes)} used</span>
                  <span>{formatSize(ispBlock.limitBytes)} allowance</span>
                </div>
              </div>
              <KeyValueRow label="Remaining" value={formatSize(ispBlock.remainingBytes)} />
              <KeyValueRow label="Reserved by running transfers" value={formatSize(ispBlock.reservedBytes)} />
              <KeyValueRow label="Window resets" value={formatDate(ispBlock.windowEndsAtEpochMs)} />
              <KeyValueRow
                label="Downloads held by the cap"
                value={ispBlock.kind === "ISP_CAP" ? "Yes" : "No"}
              />
            </>
          ),
        }
      : null,
  ];

  return <SettingsBlocks blocks={blocks} />;
}
