import { useState, type ReactNode } from "react";
import { Navigate, useLocation } from "react-router";
import { UPDATE_EGRESS } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import type { Egress } from "@/lib/networking";
import { EmptyState, MetricCell, MetricStrip } from "../../components/chrome";
import { NumberField, SecondaryButton } from "../../components/controls";
import { countLabel } from "../../i18n/labels";
import { FieldRows, SettingsBlocks, usePanelStatus, type FieldSpec } from "../../pages/settings/framework";
import {
  egressInput,
  NETWORKING_PAGES,
  useNetworkingWorkspace,
  type NetworkingAction,
  type NetworkingPage,
} from "./data";
import { EgressPage } from "./EgressPage";
import { NetworkFlow } from "./NetworkFlow";
import { PoolsPage } from "./PoolsPage";
import { RoutesPage } from "./RoutesPage";
import { formatRate, Hint, MIB, NoticeBand } from "./presentation";

export type { NetworkingData } from "./data";

/** Publishes the last action's outcome to the settings status bar. */
function ActionStatus({ message, failed }: { message: string | null; failed: boolean }) {
  usePanelStatus(message, failed);
  return null;
}

/**
 * Networking: where connections leave, how each server and feed reaches the
 * outside, and what each path is doing right now.
 *
 * The settings rail already lists the five pages and the top bar names the
 * one that is open, so this draws only the page itself.
 */
export function NetworkingWorkspace({ proxies, bandwidth }: { proxies: ReactNode; bandwidth: ReactNode }) {
  const t = useTranslate();
  const location = useLocation();
  const page = location.pathname.split("/networking/")[1] ?? "overview";
  const workspace = useNetworkingWorkspace(page);
  const { data, error, fetching, flow, consumers, egresses, measured, action, busy, message, failed } = workspace;

  if (!NETWORKING_PAGES.includes(page as NetworkingPage)) {
    return <Navigate to="/settings/networking/overview" replace />;
  }

  return (
    <>
      {/* The bandwidth page's own settings own the status bar there. */}
      {page === "bandwidth" ? null : <ActionStatus message={message} failed={failed} />}
      {error ? (
        <EmptyState
          title={t("next.networking.loadFailed")}
          body={error.message}
          action={
            <SecondaryButton icon="refresh" onClick={workspace.retry}>
              {t("next.networking.retry")}
            </SecondaryButton>
          }
        />
      ) : null}
      {!data && fetching ? (
        <EmptyState loading title={t("next.common.loading")} body={t("next.networking.loading")} />
      ) : null}
      {data ? (
        <>
          {data.platformNetworking.bridgeNetworkSuspected ? (
            <NoticeBand tone="warn" role="status">
              {t("next.networking.bridgeNotice")}
            </NoticeBand>
          ) : null}
          {page === "overview" ? (
            <>
              <MetricStrip>
                <MetricCell
                  variant="strip"
                  eyebrow={t("next.networking.overview.egresses")}
                  value={egresses.length}
                  note={t("next.networking.overview.egressesUp", {
                    count: egresses.filter((egress) => egress.health === "UP").length,
                  })}
                />
                <MetricCell
                  variant="strip"
                  eyebrow={t("next.networking.overview.proxies")}
                  value={data.proxyProfiles.length}
                  note={countLabel(t, "next.networking.pools.count", data.proxyPools.length)}
                />
                <MetricCell
                  variant="strip"
                  eyebrow={t("next.networking.overview.legs")}
                  value={flow.legs.length}
                  note={countLabel(t, "next.networking.overview.consumers", consumers.length)}
                />
                <MetricCell
                  variant="strip"
                  eyebrow={t("next.networking.overview.open")}
                  value={flow.legs.reduce((sum, leg) => sum + leg.open, 0)}
                  note={formatRate(flow.legs.reduce((sum, leg) => sum + (leg.bytesPerSecond ?? 0), 0))}
                />
              </MetricStrip>
              {workspace.liveError ? (
                <NoticeBand tone="warn" role="status">
                  {t("next.networking.liveUnavailable")}
                </NoticeBand>
              ) : null}
              <SettingsBlocks
                blocks={[
                  {
                    kind: "custom",
                    id: "flow",
                    title: t("next.networking.overview.flow"),
                    note: t("next.networking.overview.flowNote"),
                    searchText: [...consumers.map((consumer) => consumer.name), ...egresses.map((egress) => egress.name)].join(" "),
                    body: (
                      <NetworkFlow
                        flow={flow}
                        egresses={egresses}
                        profiles={measured.proxies ?? data.proxyProfiles}
                        pools={measured.proxyPools ?? data.proxyPools}
                        consumers={consumers}
                      />
                    ),
                  },
                ]}
              />
            </>
          ) : null}
          {page === "egress" ? <EgressPage data={data} egresses={egresses} action={action} busy={busy} /> : null}
          {page === "proxies" ? <PoolsPage data={data} action={action} busy={busy} proxies={proxies} /> : null}
          {page === "routes" ? (
            <RoutesPage key={location.search} data={data} consumers={consumers} flow={flow} action={action} busy={busy} />
          ) : null}
          {page === "bandwidth" ? (
            <>
              {bandwidth}
              <EgressLimits egresses={egresses} action={action} busy={busy} message={message} failed={failed} />
            </>
          ) : null}
        </>
      ) : null}
    </>
  );
}

/** Each egress's own ceiling, on top of the global and per-server ones above it. */
function EgressLimits({
  egresses,
  action,
  busy,
  message,
  failed,
}: {
  egresses: Egress[];
  action: NetworkingAction;
  busy: boolean;
  message: string | null;
  failed: boolean;
}) {
  const t = useTranslate();
  const fields: FieldSpec[] = egresses.map((egress) => ({
    id: `egress-${egress.id}`,
    label: egress.name,
    help: t("next.networking.egress.limitHelp"),
    control: {
      kind: "custom",
      control: <EgressLimit key={`${egress.id}:${egress.maxDownloadSpeed}`} egress={egress} action={action} busy={busy} />,
    },
  }));
  return (
    <SettingsBlocks
      blocks={[
        {
          kind: "custom",
          id: "egress-limits",
          title: t("next.networking.limits.title"),
          searchText: `${t("next.networking.limits.title")} ${egresses.map((egress) => egress.name).join(" ")}`,
          body: (
            <>
              <Hint>{t("next.networking.limits.note")}</Hint>
              <FieldRows fields={fields} />
              {message ? (
                <div
                  role={failed ? "alert" : "status"}
                  className={
                    failed
                      ? "px-4 py-3 text-[12.5px] text-wv-error-text sm:px-6"
                      : "px-4 py-3 text-[12.5px] text-wv-muted sm:px-6"
                  }
                >
                  {message}
                </div>
              ) : null}
            </>
          ),
        },
      ]}
    />
  );
}

function EgressLimit({ egress, action, busy }: { egress: Egress; action: NetworkingAction; busy: boolean }) {
  const t = useTranslate();
  const saved = egress.maxDownloadSpeed / MIB;
  const [limit, setLimit] = useState(saved);
  return (
    <div className="flex items-center gap-[10px]">
      <NumberField
        label={t("next.networking.limits.limitFor", { name: egress.name })}
        value={limit}
        min={0}
        step={0.1}
        precision={1}
        suffix="MiB/s"
        onChange={setLimit}
      />
      <SecondaryButton
        size="compact"
        disabled={busy || limit === saved}
        onClick={() =>
          void action(UPDATE_EGRESS, {
            id: egress.id,
            input: egressInput({ ...egress, maxDownloadSpeed: Math.round(limit * MIB) }),
          })
        }
      >
        {t("next.networking.limits.save")}
      </SecondaryButton>
    </div>
  );
}
