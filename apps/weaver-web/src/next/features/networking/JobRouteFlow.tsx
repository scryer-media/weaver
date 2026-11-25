// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import type { NetworkFlow as Flow } from "@/lib/networking";
import { DetailBlock } from "../../components/chrome";
import { useNetworkingWorkspace } from "./data";
import { useFlowDns } from "./dns-choices";
import { type FlowTarget } from "./NetworkFlow";
import { FlowPreview } from "./FlowPreview";

/**
 * A job's own routes: the live connection flow cut down to the routes the job
 * leaves on, drawn small in its panel. Picking the drawing opens it full size,
 * where every box opens its editor as on the networking overview; with one
 * route, Edit route opens that route's editor straight away.
 *
 * Nothing is read until the job's routes are known, and a reader who may not
 * see the networking settings gets no panel.
 */
export function JobRouteFlow({
  routes,
  title,
  onReport,
  proxyEditor,
}: {
  /**
   * The consumer keys the job leaves on: a torrent's route, or the servers
   * that served a usenet job. Empty for a usenet job none has served yet:
   * every server's route is drawn.
   */
  routes: string[];
  /** The job's name, as the enlarged view's note. */
  title: string;
  onReport: (message: string) => void;
  proxyEditor: (
    id: number,
    onClose: () => void,
    onChanged: () => void,
  ) => ReactNode;
}) {
  const t = useTranslate();
  const workspace = useNetworkingWorkspace("overview");
  const { data, error, flow, consumers, message } = workspace;
  // Where each leg's names are looked up, and what answers them. Read once the
  // networking settings are, so a reader who may not see them asks for nothing.
  const flowDns = useFlowDns(!data);
  const [editing, setEditing] = useState<FlowTarget | null>(null);

  const routeKey = routes.join("\n");
  // A usenet job that no server has served yet may leave on any of them.
  const shown = useMemo(() => {
    if (routeKey === "")
      return consumers.filter((consumer) => consumer.kind === "SERVER");
    const wanted = new Set(routeKey.split("\n"));
    return consumers.filter((consumer) => wanted.has(consumer.key));
  }, [consumers, routeKey]);
  const shownFlow = useMemo<Flow>(() => {
    const wanted = new Set(shown.map((consumer) => consumer.key));
    return {
      ...flow,
      legs: flow.legs.filter((leg) => wanted.has(leg.consumer)),
    };
  }, [flow, shown]);

  // What an edit made of it goes to the page's status bar.
  useEffect(() => {
    if (message !== null) onReport(message);
  }, [message, onReport]);

  if (error || !data || shown.length === 0) {
    return null;
  }
  const only = shown.length === 1 ? shown[0]! : null;

  return (
    <DetailBlock
      id="network"
      title={t("next.job.network")}
      tone="panel"
      bodyClassName="gap-[11px]"
      right={
        only ? (
          <button
            type="button"
            className="cursor-pointer text-wv-dim hover:text-wv-fg"
            onClick={() => setEditing({ kind: "route", consumer: only.key })}
          >
            {t("next.job.editRoute")}
          </button>
        ) : undefined
      }
    >
      <FlowPreview
        workspace={{ ...workspace, data }}
        dns={flowDns}
        flow={shownFlow}
        consumers={shown}
        note={title}
        editing={editing}
        onEdit={setEditing}
        proxyEditor={proxyEditor}
      />
    </DetailBlock>
  );
}
