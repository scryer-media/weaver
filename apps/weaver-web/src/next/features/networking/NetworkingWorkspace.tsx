import { useState, type ReactNode } from "react";
import { Navigate, useLocation } from "react-router";
import { useTranslate } from "@/lib/context/translate-context";
import { EmptyState } from "../../components/chrome";
import { SecondaryButton } from "../../components/controls";
import {
  SettingsBlocks,
  usePanelStatus,
  useSearchReport,
  useSettingsPanelSlug,
} from "../../pages/settings/framework";
import { NETWORKING_PAGES, useNetworkingWorkspace, type NetworkingPage } from "./data";
import { EgressPage } from "./EgressPage";
import { FlowEditor } from "./FlowEditor";
import { NetworkFlow, type FlowTarget } from "./NetworkFlow";
import { PoolsPage } from "./PoolsPage";
import { RoutesPage } from "./RoutesPage";
import { SpeedPage } from "./SpeedPage";
import { NoticeBand } from "./presentation";

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
export function NetworkingWorkspace({
  proxies,
  proxyEditor,
}: {
  /** The stored proxies' table. It calls back when one is saved or removed. */
  proxies: (onChanged: () => void) => ReactNode;
  /** One stored proxy's editor, for a proxy picked in the flow. */
  proxyEditor: (id: number, onClose: () => void, onChanged: () => void) => ReactNode;
}) {
  const t = useTranslate();
  const location = useLocation();
  // Every networking page shares this component. While the settings search
  // mounts all of them at once, each must draw the page it was mounted for,
  // not the one the address names.
  const panelSlug = useSettingsPanelSlug();
  const page = panelSlug?.startsWith("networking/")
    ? panelSlug.slice("networking/".length)
    : (location.pathname.split("/networking/")[1] ?? "overview");
  const workspace = useNetworkingWorkspace(page);
  const [editing, setEditing] = useState<FlowTarget | null>(null);
  const { data, error, fetching, flow, consumers, egresses, measured, action, busy, message, failed } = workspace;
  // The pages draw their blocks only once the data is here.
  useSearchReport(0, !data && fetching);

  if (!NETWORKING_PAGES.includes(page as NetworkingPage)) {
    return <Navigate to="/settings/networking/overview" replace />;
  }

  return (
    <>
      {/* The egress page's download ceiling owns the status bar there, and reports these actions with its own. */}
      {page === "egress" ? null : <ActionStatus message={message} failed={failed} />}
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
                    searchText: [...consumers.map((consumer) => consumer.name), ...egresses.map((egress) => egress.name)].join(" "),
                    body: (
                      <NetworkFlow
                        flow={flow}
                        egresses={egresses}
                        profiles={measured.proxies ?? data.proxyProfiles}
                        pools={measured.proxyPools ?? data.proxyPools}
                        consumers={consumers}
                        onEdit={setEditing}
                        trace
                      />
                    ),
                  },
                ]}
              />
              <FlowEditor
                target={editing}
                onClose={() => setEditing(null)}
                data={data}
                egresses={egresses}
                consumers={consumers}
                flow={flow}
                action={action}
                busy={busy}
                proxyEditor={(id, onClose) => proxyEditor(id, onClose, workspace.retry)}
              />
            </>
          ) : null}
          {page === "egress" ? (
            <EgressPage data={data} egresses={egresses} action={action} busy={busy} message={message} failed={failed} />
          ) : null}
          {page === "proxies" ? <PoolsPage data={data} action={action} busy={busy} proxies={proxies(workspace.retry)} /> : null}
          {page === "routes" ? (
            <RoutesPage key={location.search} data={data} consumers={consumers} flow={flow} action={action} busy={busy} />
          ) : null}
          {page === "speed" ? <SpeedPage key={location.search} data={data} consumers={consumers} /> : null}
        </>
      ) : null}
    </>
  );
}
