import { useState } from "react";
import { useLocation } from "react-router";
import { SAVE_ROUTE } from "@/graphql/networking";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { routeInput, routeProblem, type NetworkFlow, type NetworkRoute } from "@/lib/networking";
import { Dialog } from "../../components/Dialog";
import { PrimaryButton, SecondaryButton } from "../../components/controls";
import { Cell } from "../../components/rows";
import { countLabel } from "../../i18n/labels";
import { SettingsBlocks, type SettingsBlock } from "../../pages/settings/framework";
import { defaultRoute, type Consumer, type NetworkingAction, type NetworkingData } from "./data";
import { RouteEditor } from "./RouteEditor";
import { stateLabel, StatusMark } from "./presentation";

/** "20 NNTP connections · 2 legs": what a consumer is, and how its route splits it. */
export function consumerSummary(t: Translate, consumer: Consumer): string {
  const legs = countLabel(t, "next.networking.legCount", consumer.route?.legs.length ?? 1);
  return consumer.kind === "SERVER"
    ? `${countLabel(t, "next.networking.routes.connections", consumer.cap)} · ${legs}`
    : `${t("next.networking.routes.rssFeed")} · ${legs}`;
}

/** The state a consumer's legs add up to: carrying, broken, or waiting. */
function consumerState(flow: NetworkFlow, consumer: Consumer): string {
  const legs = flow.legs.filter((leg) => leg.consumer === consumer.key);
  if (legs.some((leg) => leg.open > 0)) {
    return "ACTIVE";
  }
  if (legs.length > 0 && legs.every((leg) => leg.state === "DOWN" || leg.state === "BLOCKED")) {
    return "DOWN";
  }
  return "IDLE";
}

export function RoutesPage({
  data,
  consumers,
  flow,
  action,
  busy,
}: {
  data: NetworkingData;
  consumers: Consumer[];
  flow: NetworkFlow;
  action: NetworkingAction;
  busy: boolean;
}) {
  const t = useTranslate();
  const location = useLocation();
  const requested = new URLSearchParams(location.search).get("consumer");
  const preselected = consumers.find((consumer) => consumer.key === requested) ?? null;
  const [editing, setEditing] = useState<Consumer | null>(preselected);
  const [route, setRoute] = useState<NetworkRoute>(preselected?.route ?? defaultRoute());
  const [error, setError] = useState<string | null>(null);

  const open = (consumer: Consumer) => {
    setError(null);
    setEditing(consumer);
    setRoute(consumer.route ?? defaultRoute());
  };

  const save = async () => {
    if (!editing) {
      return;
    }
    const failure = await action(SAVE_ROUTE, { kind: editing.kind, id: editing.id, input: routeInput(route) });
    if (failure) {
      setError(failure);
    } else {
      setEditing(null);
    }
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "routes",
      title: t("next.networking.routes.title"),
      note: t("next.networking.routes.note"),
      columns: "minmax(0, 1.2fr) minmax(0, 1.4fr) minmax(0, 1fr) 112px",
      headers: [
        t("next.networking.routes.consumer"),
        t("next.networking.routes.route"),
        t("next.networking.routes.live"),
        "",
      ],
      empty: t("next.networking.routes.empty"),
      rows: consumers.map((consumer) => {
        const legs = flow.legs.filter((leg) => leg.consumer === consumer.key);
        const openCount = legs.reduce((sum, leg) => sum + leg.open, 0);
        const state = consumerState(flow, consumer);
        return {
          id: consumer.key,
          searchText: `${consumer.name} ${consumerSummary(t, consumer)}`,
          cells: [
            <Cell key="name" title={consumer.name}>
              {consumer.name}
            </Cell>,
            <Cell key="route" mono className="text-wv-secondary">
              {consumerSummary(t, consumer)}
            </Cell>,
            <StatusMark
              key="live"
              state={state}
              label={stateLabel(t, state)}
              detail={consumer.kind === "SERVER" ? `${openCount} / ${consumer.cap}` : undefined}
            />,
            <SecondaryButton key="edit" size="compact" onClick={() => open(consumer)} className="ml-auto">
              {t("next.networking.routes.edit")}
            </SecondaryButton>,
          ],
        };
      }),
    },
  ];

  const problem = routeProblem(route);

  return (
    <>
      <SettingsBlocks blocks={blocks} />
      {editing ? (
        <Dialog
          open
          title={t("next.networking.routes.editorTitle", { name: editing.name })}
          note={consumerSummary(t, { ...editing, route })}
          width={720}
          onDismiss={() => setEditing(null)}
          footer={
            <>
              <SecondaryButton onClick={() => setEditing(null)}>{t("action.cancel")}</SecondaryButton>
              <PrimaryButton icon="save" disabled={busy || problem !== null} onClick={() => void save()}>
                {busy ? t("settings.saving") : t("next.networking.routes.save")}
              </PrimaryButton>
            </>
          }
        >
          <div className="px-4 py-5 sm:px-6">
            <RouteEditor
              value={route}
              onChange={setRoute}
              egresses={data.egressInterfaces}
              profiles={data.proxyProfiles}
              pools={data.proxyPools}
              cap={editing.cap}
              rss={editing.kind === "RSS"}
              status={flow.legs.filter((leg) => leg.consumer === editing.key)}
            />
          </div>
          {error ? (
            <div role="alert" className="flex-none border-t border-wv-hairline px-4 py-4 text-[12.5px] text-wv-error-text sm:px-6">
              {error}
            </div>
          ) : null}
        </Dialog>
      ) : null}
    </>
  );
}
