import { useQuery } from "urql";
import { EGRESS_QUOTAS_QUERY } from "@/graphql/networking";
import type { Translate } from "@/lib/context/translate-context";
import type { Egress } from "@/lib/networking";
import { formatDayClock, formatSize } from "../../data/format";
import type { SettingsTableModel } from "../../pages/settings/framework";
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
    <span className="inline-flex h-[18px] flex-none items-center self-start border border-wv-warn/60 px-[6px] font-wv-mono text-[10px] leading-none font-medium tracking-[0.1em] whitespace-nowrap text-wv-warn uppercase">
      {t("next.quota.blocked")}
    </span>
  );
}

/** One usage row per egress: name, used, allowance, remaining, when the window resets, and whether it is refusing. */
export function egressQuotaTable(t: Translate, rows: EgressQuotaRow[]): SettingsTableModel {
  return {
    kind: "table",
    id: "egressQuotas",
    title: t("next.quota.usageTitle"),
    note: t("next.quota.usageNote"),
    columns: "minmax(0, 1.2fr) minmax(0, 0.8fr) minmax(0, 0.8fr) minmax(0, 0.8fr) minmax(0, 1fr) 84px",
    headers: [
      t("next.networking.egress.name"),
      t("next.quota.used"),
      t("next.bandwidth.allowance"),
      t("next.monitoring.remaining"),
      t("next.quota.resets"),
      t("next.quota.state"),
    ],
    rows: rows.map((egress) => {
      const usage = egress.downloadQuotaUsage;
      const on = metered(egress);
      const dash = (key: string) => (
        <Cell key={key} mono className="text-wv-muted">
          —
        </Cell>
      );
      return {
        id: String(egress.id),
        searchText: `${egress.name} quota allowance used remaining`,
        cells: [
          <Cell key="name" title={egress.name}>
            {egress.name}
          </Cell>,
          <Cell key="used" mono>
            {formatSize(usage?.usedBytes ?? 0)}
          </Cell>,
          on ? (
            <Cell key="allowance" mono>
              {formatSize(egress.downloadQuota?.limitBytes ?? 0)}
            </Cell>
          ) : (
            dash("allowance")
          ),
          on && usage?.remainingBytes != null ? (
            <Cell key="remaining" mono>
              {formatSize(usage.remainingBytes)}
            </Cell>
          ) : (
            dash("remaining")
          ),
          on && usage?.windowEndsAtEpochMs ? (
            <Cell key="resets" mono title={usage.timezoneName || undefined}>
              {formatDayClock(usage.windowEndsAtEpochMs)}
            </Cell>
          ) : (
            dash("resets")
          ),
          usage?.blocked ? (
            <BlockedChip key="state" t={t} />
          ) : (
            <Cell key="state" mono className="text-wv-muted">
              {on ? t("next.quota.open") : "—"}
            </Cell>
          ),
        ],
      };
    }),
  };
}
