// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { createContext, memo, useContext, useMemo, useState, type ReactNode } from "react";
import { Navigate, useLocation } from "react-router";
import { useTranslate } from "@/lib/context/translate-context";
import type { Egress, NetworkFlow as Flow, ProxyPool } from "@/lib/networking";
import type { ProxyProfile } from "@/lib/proxies";
import { EmptyState } from "../../components/chrome";
import { SecondaryButton } from "../../components/controls";
import {
  SettingsBlocks,
  usePanelStatus,
  useSearchReport,
  useSettingsPanelSlug,
  type SettingsBlock,
} from "../../pages/settings/framework";
import {
  EMPTY_FLOW,
  NETWORKING_PAGES,
  useNetworkingWorkspace,
  type Consumer,
  type NetworkingPage,
} from "./data";
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

/** The live flow, apart, so a sample draws the overview's diagram and not the blocks around it. */
const LiveFlowContext = createContext<Flow>(EMPTY_FLOW);

/** The overview's diagram, the one part of the page each sample changes. */
const OverviewFlow = memo(function OverviewFlow({
  egresses,
  profiles,
  pools,
  consumers,
  onEdit,
}: {
  egresses: Egress[];
  profiles: ProxyProfile[];
  pools: ProxyPool[];
  consumers: Consumer[];
  onEdit: (target: FlowTarget) => void;
}) {
  const flow = useContext(LiveFlowContext);
  return (
    <NetworkFlow
      flow={flow}
      egresses={egresses}
      profiles={profiles}
      pools={pools}
      consumers={consumers}
      onEdit={onEdit}
      traffic
      trace
    />
  );
});

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
  const { data, error, fetching, flow, consumers, egresses, profiles, pools, action, busy, message, failed } = workspace;
  // The pages draw their blocks only once the data is here.
  useSearchReport(0, !data && fetching);
  // Held while what it names is the same: the diagram reads each sample itself.
  const overviewBlocks = useMemo<SettingsBlock[]>(
    () => [
      {
        kind: "custom",
        id: "flow",
        title: t("next.networking.overview.flow"),
        searchText: [...consumers.map((consumer) => consumer.name), ...egresses.map((egress) => egress.name)].join(" "),
        body: (
          <OverviewFlow egresses={egresses} profiles={profiles} pools={pools} consumers={consumers} onEdit={setEditing} />
        ),
      },
    ],
    [consumers, egresses, pools, profiles, t],
  );

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
              <LiveFlowContext.Provider value={flow}>
                <SettingsBlocks blocks={overviewBlocks} />
              </LiveFlowContext.Provider>
              {/* Nothing is drawn until a box is picked, so nothing is drawn again for each sample either. */}
              {editing ? (
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
              ) : null}
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
