// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router";
import { SAVE_ROUTE } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import { routeInput, routeProblem, type ConsumerKind, type NetworkFlow, type NetworkRoute } from "@/lib/networking";
import { cn } from "@/lib/utils";
import { Dialog } from "../../components/Dialog";
import { PrimaryButton, SecondaryButton } from "../../components/controls";
import { Icon } from "../../components/icons";
import { Cell } from "../../components/rows";
import { SettingsBlocks, type SettingsBlock } from "../../pages/settings/framework";
import { defaultRoute, speedPage, type Consumer, type NetworkingAction, type NetworkingData } from "./data";
import { RouteEditor } from "./RouteEditor";
import { stateLabel, StatusMark } from "./presentation";
import { TestViaDialog } from "./TestViaDialog";

/** What a consumer carries: HTTP (Default outbound, the feeds), Usenet (the servers) or torrents. */
export type ConsumerType = "HTTP" | "USENET" | "TORRENT";

export function consumerType(kind: ConsumerKind): ConsumerType {
  switch (kind) {
    case "SERVER":
      return "USENET";
    case "RSS":
    case "OUTBOUND":
      return "HTTP";
    case "TORRENT_DEFAULT":
    case "TORRENT_CATEGORY":
      return "TORRENT";
  }
}

const TYPE_KEYS: Record<ConsumerType, string> = {
  HTTP: "next.networking.routes.typeHttp",
  USENET: "next.networking.routes.typeUsenet",
  TORRENT: "next.networking.routes.typeTorrent",
};

type SortColumn = "name" | "type";
type Sort = { column: SortColumn; descending: boolean };

/** A header the table sorts by: a click sorts by it, a second click turns it around. */
function SortHeader({ label, column, sort, onSort }: { label: string; column: SortColumn; sort: Sort; onSort: (column: SortColumn) => void }) {
  const active = sort.column === column;
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={() => onSort(column)}
      className={cn("flex cursor-pointer items-center gap-1 uppercase hover:text-wv-strong", active && "text-wv-strong")}
    >
      {label}
      <Icon name={active ? (sort.descending ? "moveDown" : "moveUp") : "priority"} size={11} className={cn("flex-none", !active && "opacity-50")} />
    </button>
  );
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
  const navigate = useNavigate();
  const requested = new URLSearchParams(location.search).get("consumer");
  const preselected = consumers.find((consumer) => consumer.key === requested) ?? null;
  const [editing, setEditing] = useState<Consumer | null>(preselected);
  const [sort, setSort] = useState<Sort>({ column: "name", descending: false });
  const typeLabel = (consumer: Pick<Consumer, "kind">) => t(TYPE_KEYS[consumerType(consumer.kind)]);
  const sorted = useMemo(() => {
    const byName = (a: Consumer, b: Consumer) => a.name.localeCompare(b.name);
    const byType = (a: Consumer, b: Consumer) =>
      t(TYPE_KEYS[consumerType(a.kind)]).localeCompare(t(TYPE_KEYS[consumerType(b.kind)])) || byName(a, b);
    const order = [...consumers].sort(sort.column === "name" ? byName : byType);
    return sort.descending ? order.reverse() : order;
  }, [consumers, sort, t]);
  const onSort = (column: SortColumn) =>
    setSort((current) => ({ column, descending: current.column === column && !current.descending }));

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "routes",
      title: t("next.networking.routes.title"),
      note: t("next.networking.routes.note"),
      columns: "minmax(0, 1.6fr) minmax(0, 0.8fr) minmax(0, 1fr) 208px",
      headers: [
        <SortHeader key="name" label={t("next.networking.routes.consumer")} column="name" sort={sort} onSort={onSort} />,
        <SortHeader key="type" label={t("next.networking.routes.type")} column="type" sort={sort} onSort={onSort} />,
        t("next.networking.routes.live"),
        "",
      ],
      empty: t("next.networking.routes.empty"),
      onRowClick: (key) => {
        const consumer = consumers.find((candidate) => candidate.key === key);
        if (consumer) {
          setEditing(consumer);
        }
      },
      rows: sorted.map((consumer) => {
        const legs = flow.legs.filter((leg) => leg.consumer === consumer.key);
        const openCount = legs.reduce((sum, leg) => sum + leg.open, 0);
        const state = consumerState(flow, consumer);
        return {
          id: consumer.key,
          searchText: `${consumer.name} ${typeLabel(consumer)}`,
          cells: [
            <Cell key="name" title={consumer.name}>
              {consumer.name}
            </Cell>,
            <Cell key="type" mono className="text-wv-secondary">
              {typeLabel(consumer)}
            </Cell>,
            <StatusMark
              key="live"
              state={state}
              label={stateLabel(t, state)}
              detail={consumer.kind === "SERVER" ? `${openCount} / ${consumer.cap}` : undefined}
            />,
            <span key="edit" className="ml-auto flex gap-2" onClick={(event) => event.stopPropagation()}>
              <SecondaryButton
                size="compact"
                icon="speedTest"
                onClick={() => void navigate(speedPage(consumer.key))}
              >
                {t("next.networking.speed.title")}
              </SecondaryButton>
              <SecondaryButton size="compact" icon="edit" onClick={() => setEditing(consumer)}>
                {t("next.common.edit")}
              </SecondaryButton>
            </span>,
          ],
        };
      }),
    },
  ];

  return (
    <>
      <SettingsBlocks blocks={blocks} />
      {editing ? (
        <RouteDialog
          key={editing.key}
          data={data}
          consumer={editing}
          flow={flow}
          action={action}
          busy={busy}
          onClose={() => setEditing(null)}
        />
      ) : null}
    </>
  );
}

/**
 * One consumer's route in its editor, mounted while it is open. The
 * routes table opens it, and so does a route leg or an endpoint picked in the
 * network flow.
 */
export function RouteDialog({
  data,
  consumer,
  flow,
  action,
  busy,
  onClose,
}: {
  data: NetworkingData;
  consumer: Consumer;
  flow: NetworkFlow;
  action: NetworkingAction;
  busy: boolean;
  onClose: () => void;
}) {
  const t = useTranslate();
  const [route, setRoute] = useState<NetworkRoute>(consumer.route ?? defaultRoute());
  const [error, setError] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);

  const save = async () => {
    const failure = await action(SAVE_ROUTE, { kind: consumer.kind, id: consumer.id, input: routeInput(route) });
    if (failure) {
      setError(failure);
    } else {
      onClose();
    }
  };

  const problem = routeProblem(route, { egresses: data.egressInterfaces, pools: data.proxyPools, kind: consumer.kind });

  return (
    <Dialog
      open
      title={t("next.networking.routes.editorTitle", { name: consumer.name })}
      note={t(TYPE_KEYS[consumerType(consumer.kind)])}
      width={720}
      onDismiss={onClose}
      footer={
        <>
          {consumer.kind === "TORRENT_DEFAULT" || consumer.kind === "TORRENT_CATEGORY" ? (
            <SecondaryButton icon="test" disabled={problem !== null} onClick={() => setTesting(true)}>
              {t("next.networking.testVia.title")}
            </SecondaryButton>
          ) : null}
          <SecondaryButton onClick={onClose}>{t("action.cancel")}</SecondaryButton>
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
          cap={consumer.cap}
          kind={consumer.kind}
          status={flow.legs.filter((leg) => leg.consumer === consumer.key)}
        />
      </div>
      {error ? (
        <div role="alert" className="flex-none border-t border-wv-hairline px-4 py-4 text-[12.5px] text-wv-error-text sm:px-6">
          {error}
        </div>
      ) : null}
      {testing && (consumer.kind === "TORRENT_DEFAULT" || consumer.kind === "TORRENT_CATEGORY") ? (
        <TestViaDialog subject={null} route={route} torrent={consumer.kind} onClose={() => setTesting(false)} />
      ) : null}
    </Dialog>
  );
}

