// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useQuery } from "urql";
import { EGRESS_QUOTAS_QUERY } from "@/graphql/networking";
import type { Translate } from "@/lib/context/translate-context";
import type { Egress, EgressQuota, QuotaDirectionUsage, QuotaPeriod, QuotaState, Weekday } from "@/lib/networking";
import { Bar } from "../../components/chrome";
import { Toggle } from "../../components/controls";
import { Cell } from "../../components/rows";
import { formatDayClock, formatSize } from "../../data/format";
import { WV } from "../../data/palette";
import type { FieldSpec } from "../../pages/settings/framework";
import {
  AllowanceControl,
  allowanceBytes,
  allowanceText,
  minutesToTime,
  periodField,
  timeToMinutes,
  windowFields,
  type AllowanceUnit,
} from "../../pages/settings/quota";

/** The egress weaver falls back to; it always exists, so it is always listed. */
const SYSTEM_EGRESS_ID = 0;

export type EgressQuotaRow = Pick<Egress, "id" | "name" | "quota" | "quotaUsage">;

export type Direction = "down" | "up";

/** The cap a direction is measured against: the download cap, or the upload's hard cap, else its soft one. */
export function directionCap(quota: EgressQuota | undefined, direction: Direction): number | null {
  if (!quota) {
    return null;
  }
  return direction === "down" ? quota.downloadBytes : (quota.uploadHardBytes ?? quota.uploadSoftBytes);
}

/** The directions this egress has a cap on, download first. */
export function cappedDirections(egress: EgressQuotaRow): Direction[] {
  return (["down", "up"] as const).filter((direction) => directionCap(egress.quota, direction) !== null);
}

/** Whether this egress has a cap being counted in either direction. */
export function metered(egress: EgressQuotaRow): boolean {
  return cappedDirections(egress).length > 0;
}

const ARROW: Record<Direction, string> = { down: "↓", up: "↑" };

export function usageOf(egress: EgressQuotaRow, direction: Direction): QuotaDirectionUsage | null {
  return egress.quotaUsage?.[direction] ?? null;
}

/** "used / cap" for one direction. */
function directionSummary(egress: EgressQuotaRow, direction: Direction): string {
  return `${formatSize(usageOf(egress, direction)?.usedBytes ?? 0)} / ${formatSize(directionCap(egress.quota, direction) ?? 0)}`;
}

/** "↓ used / cap · ↑ used / cap" over the capped directions, or a dash for an egress with no cap. */
export function quotaSummary(egress: EgressQuotaRow): string {
  const directions = cappedDirections(egress);
  return directions.length
    ? directions.map((direction) => `${ARROW[direction]} ${directionSummary(egress, direction)}`).join(" · ")
    : "—";
}

/** A direction that is held back: slowed to the trickle, or stopped. */
export const quotaHolding = (state: QuotaState | null | undefined) => state === "TRICKLE" || state === "STOPPED";

/** The strongest hold the caps put on this egress: Stopped over Trickle, else null. */
export function quotaHeldState(egress: EgressQuotaRow): QuotaState | null {
  const states = cappedDirections(egress).map((direction) => usageOf(egress, direction)?.state ?? "NORMAL");
  return states.includes("STOPPED") ? "STOPPED" : states.includes("TRICKLE") ? "TRICKLE" : null;
}

export function quotaStateLabel(t: Translate, state: QuotaState): string {
  return state === "STOPPED" ? t("next.quota.stopped") : state === "TRICKLE" ? t("next.quota.trickle") : "";
}

/** When the current period resets, for a metered egress. */
function quotaDetail(t: Translate, egress: EgressQuotaRow): string {
  const end = egress.quotaUsage?.periodEndEpochMs;
  return metered(egress) && end ? t("next.monitoring.resetsAt", { time: formatDayClock(end) }) : "";
}

/** Everything the quota cell says, for the settings search. */
export function quotaSearchText(t: Translate, egress: EgressQuotaRow): string {
  return [
    quotaSummary(egress),
    quotaDetail(t, egress),
    ...cappedDirections(egress).map((direction) => quotaStateLabel(t, usageOf(egress, direction)?.state ?? "NORMAL")),
  ].join(" ");
}

/**
 * Every egress with its caps and what the current period has spent, System
 * first. The usage is the counter that holds transfers back, so this list and
 * what weaver enforces cannot disagree.
 */
export function useEgressQuotas(t: Translate) {
  const [{ data, fetching, error }] = useQuery<{ egressInterfaces: EgressQuotaRow[] }>({
    query: EGRESS_QUOTAS_QUERY,
    requestPolicy: "cache-and-network",
  });
  const listed = data?.egressInterfaces ?? [];
  const rows: EgressQuotaRow[] = listed.some((egress) => egress.id === SYSTEM_EGRESS_ID)
    ? listed
    : [{ id: SYSTEM_EGRESS_ID, name: t("next.networking.binding.system"), quota: undefined, quotaUsage: null }, ...listed];
  return {
    rows: [...rows].sort((left, right) => left.id - right.id),
    loading: fetching && !data,
    error: error?.message ?? null,
  };
}

/** A direction held back by its cap: Trickle, or Stopped. */
export function QuotaStateChip({ t, state }: { t: Translate; state: QuotaState }) {
  return (
    <span className="inline-flex h-[18px] flex-none items-center border border-wv-warn/60 px-[6px] font-wv-mono text-[10px] leading-none font-medium tracking-[0.1em] whitespace-nowrap text-wv-warn uppercase">
      {quotaStateLabel(t, state)}
    </span>
  );
}

/** How full a direction's cap is, coloured as it nears the cap. */
export function directionPercent(egress: EgressQuotaRow, direction: Direction): number {
  const cap = directionCap(egress.quota, direction);
  const used = usageOf(egress, direction)?.usedBytes ?? 0;
  return cap === null ? 0 : cap === 0 ? 100 : Math.min(100, (used / cap) * 100);
}

export function usageColor(state: QuotaState | undefined, percent: number): string {
  return state === "STOPPED" || percent >= 90 ? WV.error : state === "TRICKLE" || percent >= 70 ? WV.warn : WV.accent;
}

/**
 * The egress table's quota cell: for each capped direction, a bar of the
 * spend against the cap, the figures, and whether the cap is holding that
 * direction back; then when the period resets. A dash for an egress with no cap.
 */
export function QuotaCell({ t, egress }: { t: Translate; egress: EgressQuotaRow }) {
  const directions = cappedDirections(egress);
  const detail = quotaDetail(t, egress);
  const zone = egress.quotaUsage?.timezoneName;
  if (!directions.length) {
    return (
      <Cell mono className="text-wv-muted">
        —
      </Cell>
    );
  }
  return (
    <div className="flex min-w-0 flex-col gap-[3px]">
      {directions.map((direction) => {
        const state = usageOf(egress, direction)?.state ?? "NORMAL";
        const percent = directionPercent(egress, direction);
        const label = t(direction === "down" ? "next.quota.download" : "next.quota.upload");
        return (
          <div key={direction} className="flex min-w-0 items-center gap-2" title={label}>
            <span aria-hidden="true" className="flex-none font-wv-mono text-[11px] text-wv-muted">
              {ARROW[direction]}
            </span>
            <Bar percent={percent} color={usageColor(state, percent)} height={10} className="w-[56px] flex-none" label={label} />
            <Cell mono className={quotaHolding(state) ? "text-wv-warn" : undefined}>
              {directionSummary(egress, direction)}
            </Cell>
            {quotaHolding(state) ? <QuotaStateChip t={t} state={state} /> : null}
          </div>
        );
      })}
      {detail ? (
        <Cell mono className="text-[11px] text-wv-muted" title={zone ? `${detail} · ${zone}` : detail}>
          {detail}
        </Cell>
      ) : null}
    </div>
  );
}

/* --------------------------------------------------------------- the editor */

/** One cap as its editor holds it: off, or an allowance typed in a unit. */
interface CapDraft {
  on: boolean;
  limit: string;
  unit: AllowanceUnit;
}

/** An egress's quota as its editor holds it. */
export interface EgressQuotaDraft {
  download: CapDraft;
  uploadSoft: CapDraft;
  uploadHard: CapDraft;
  /** The rate a soft-capped upload slows to, in KiB/s. */
  trickleKib: number;
  period: QuotaPeriod;
  resetTime: string;
  weeklyResetWeekday: Weekday;
  monthlyResetDay: number;
}

const KIB = 1024;

const capDraft = (bytes: number | null | undefined): CapDraft =>
  bytes === null || bytes === undefined ? { on: false, limit: "", unit: "GB" } : { on: true, ...allowanceText(bytes) };

export function egressQuotaDraft(quota: EgressQuota | undefined): EgressQuotaDraft {
  return {
    download: capDraft(quota?.downloadBytes),
    uploadSoft: capDraft(quota?.uploadSoftBytes),
    uploadHard: capDraft(quota?.uploadHardBytes),
    trickleKib: Math.round((quota?.uploadTrickleBps ?? 2048) / KIB),
    period: quota?.period ?? "MONTHLY",
    resetTime: minutesToTime(quota?.resetTimeMinutesLocal ?? 0),
    weeklyResetWeekday: quota?.weeklyResetWeekday ?? "MON",
    monthlyResetDay: quota?.monthlyResetDay ?? 1,
  };
}

const capBytes = (cap: CapDraft) => (cap.on ? allowanceBytes(cap.limit, cap.unit) : null);

/** The quota as `setEgressQuota` takes it. */
export function egressQuotaInput(draft: EgressQuotaDraft): EgressQuota {
  return {
    downloadBytes: capBytes(draft.download),
    uploadSoftBytes: capBytes(draft.uploadSoft),
    uploadTrickleBps: Math.max(0, Math.round(draft.trickleKib)) * KIB,
    uploadHardBytes: capBytes(draft.uploadHard),
    period: draft.period,
    resetTimeMinutesLocal: timeToMinutes(draft.resetTime),
    weeklyResetWeekday: draft.weeklyResetWeekday,
    monthlyResetDay: Math.min(31, Math.max(1, Math.trunc(draft.monthlyResetDay))),
  };
}

/** Why the quota cannot be saved: a cap switched on with no allowance typed. Null when it can. */
export function egressQuotaProblem(t: Translate, draft: EgressQuotaDraft): string | null {
  return [draft.download, draft.uploadSoft, draft.uploadHard].some((cap) => cap.on && capBytes(cap) === null)
    ? t("next.quota.sizeRequired")
    : null;
}

/** Whether the draft saves something other than `saved`. */
export function egressQuotaChanged(draft: EgressQuotaDraft, saved: EgressQuota | undefined): boolean {
  return JSON.stringify(egressQuotaInput(draft)) !== JSON.stringify(egressQuotaInput(egressQuotaDraft(saved)));
}

/** A cap's switch, and the allowance beside it while it is on. */
function CapControl({ label, cap, onChange }: { label: string; cap: CapDraft; onChange: (next: CapDraft) => void }) {
  return (
    <div className="flex items-center gap-[10px]">
      {cap.on ? (
        <AllowanceControl label={label} limit={cap.limit} unit={cap.unit} onChange={(next) => onChange({ ...cap, ...next })} />
      ) : null}
      <Toggle checked={cap.on} label={label} onChange={(on) => onChange({ ...cap, on })} />
    </div>
  );
}

/**
 * The quota's fields: a cap on downloads, a soft and a hard cap on uploads
 * with the rate the soft one slows uploads to, and, once any cap is on, the
 * period they count over and when it resets.
 */
export function egressQuotaFields(
  t: Translate,
  draft: EgressQuotaDraft,
  onChange: (next: EgressQuotaDraft) => void,
  usage: { down?: number; up?: number },
): FieldSpec[] {
  const patch = (next: Partial<EgressQuotaDraft>) => onChange({ ...draft, ...next });
  const used = (bytes: number | undefined) =>
    bytes === undefined ? undefined : t("next.providers.usedSoFar", { size: formatSize(bytes) });
  const cap = (id: "download" | "uploadSoft" | "uploadHard", label: string, help: string | undefined): FieldSpec => ({
    id: `quota-${id}`,
    label,
    help,
    keywords: "quota allowance metered cap",
    control: {
      kind: "custom",
      control: <CapControl label={label} cap={draft[id]} onChange={(next) => patch({ [id]: next })} />,
    },
  });
  const anyCap = draft.download.on || draft.uploadSoft.on || draft.uploadHard.on;
  return [
    cap("download", t("next.quota.downloadCap"), used(usage.down)),
    cap("uploadSoft", t("next.quota.uploadSoftCap"), used(usage.up)),
    ...(draft.uploadSoft.on
      ? [
          {
            id: "quota-trickle",
            label: t("next.quota.trickleRate"),
            control: {
              kind: "number" as const,
              value: draft.trickleKib,
              min: 0,
              suffix: "KiB/s",
              onChange: (trickleKib: number) => patch({ trickleKib }),
            },
          },
        ]
      : []),
    cap("uploadHard", t("next.quota.uploadHardCap"), undefined),
    ...(anyCap
      ? [
          periodField(t, draft.period, (period) => patch({ period })),
          ...windowFields(
            t,
            {
              period: draft.period,
              resetTime: draft.resetTime,
              weeklyResetWeekday: draft.weeklyResetWeekday,
              monthlyResetDay: draft.monthlyResetDay,
            },
            patch,
          ),
        ]
      : []),
  ];
}
