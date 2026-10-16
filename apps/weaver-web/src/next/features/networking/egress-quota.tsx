// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useQuery } from "urql";
import { EGRESS_QUOTAS_QUERY } from "@/graphql/networking";
import type { Translate } from "@/lib/context/translate-context";
import type { Egress } from "@/lib/networking";
import { formatDayClock, formatSize } from "../../data/format";
import { Cell } from "../../components/rows";

/** The egress weaver falls back to; it always exists, so it is always listed. */
const SYSTEM_EGRESS_ID = 0;

export type EgressQuotaRow = Pick<Egress, "id" | "name" | "downloadQuota" | "downloadQuotaUsage">;

/** Whether this egress has an allowance being counted. */
export function metered(egress: EgressQuotaRow): boolean {
  return Boolean(egress.downloadQuota?.enabled && egress.downloadQuota.limitBytes > 0);
}

/** "used / allowance", or a dash for an egress with no allowance. */
export function quotaSummary(egress: EgressQuotaRow): string {
  if (!metered(egress)) {
    return "—";
  }
  const used = egress.downloadQuotaUsage?.usedBytes ?? 0;
  return `${formatSize(used)} / ${formatSize(egress.downloadQuota?.limitBytes ?? 0)}`;
}

/** What the current window has left and when it resets, for a metered egress. */
function quotaDetail(t: Translate, egress: EgressQuotaRow): string {
  const usage = egress.downloadQuotaUsage;
  if (!metered(egress) || !usage) {
    return "";
  }
  return [
    ...(usage.remainingBytes != null ? [t("next.quota.remaining", { size: formatSize(usage.remainingBytes) })] : []),
    ...(usage.windowEndsAtEpochMs
      ? [t("next.monitoring.resetsAt", { time: formatDayClock(usage.windowEndsAtEpochMs) })]
      : []),
  ].join(" · ");
}

/** Everything the quota cell says, for the settings search. */
export function quotaSearchText(t: Translate, egress: EgressQuotaRow): string {
  return [
    quotaSummary(egress),
    quotaDetail(t, egress),
    egress.downloadQuotaUsage?.blocked ? t("next.quota.blocked") : "",
  ].join(" ");
}

/**
 * Every egress with its allowance and what the current window has spent,
 * System first. The usage is the counter that refuses downloads, so this list
 * and what weaver enforces cannot disagree.
 */
export function useEgressQuotas(t: Translate) {
  const [{ data, fetching, error }] = useQuery<{ egressInterfaces: EgressQuotaRow[] }>({
    query: EGRESS_QUOTAS_QUERY,
    requestPolicy: "cache-and-network",
  });
  const listed = data?.egressInterfaces ?? [];
  const rows: EgressQuotaRow[] = listed.some((egress) => egress.id === SYSTEM_EGRESS_ID)
    ? listed
    : [{ id: SYSTEM_EGRESS_ID, name: t("next.networking.binding.system"), downloadQuota: undefined, downloadQuotaUsage: null }, ...listed];
  return {
    rows: [...rows].sort((left, right) => left.id - right.id),
    loading: fetching && !data,
    error: error?.message ?? null,
  };
}

function BlockedChip({ t }: { t: Translate }) {
  return (
    <span className="inline-flex h-[18px] flex-none items-center border border-wv-warn/60 px-[6px] font-wv-mono text-[10px] leading-none font-medium tracking-[0.1em] whitespace-nowrap text-wv-warn uppercase">
      {t("next.quota.blocked")}
    </span>
  );
}

/**
 * The egress table's quota cell: used against the allowance, then what the
 * window has left and when it resets, and whether it is refusing downloads.
 * A dash for an egress with no allowance.
 */
export function QuotaCell({ t, egress }: { t: Translate; egress: EgressQuotaRow }) {
  const blocked = Boolean(egress.downloadQuotaUsage?.blocked);
  const detail = quotaDetail(t, egress);
  const zone = egress.downloadQuotaUsage?.timezoneName;
  return (
    <div className="flex min-w-0 flex-col gap-[3px]">
      <div className="flex min-w-0 items-center gap-2">
        <Cell mono className={blocked ? "text-wv-warn" : metered(egress) ? undefined : "text-wv-muted"}>
          {quotaSummary(egress)}
        </Cell>
        {blocked ? <BlockedChip t={t} /> : null}
      </div>
      {detail ? (
        <Cell mono className="text-[11px] text-wv-muted" title={zone ? `${detail} · ${zone}` : detail}>
          {detail}
        </Cell>
      ) : null}
    </div>
  );
}
