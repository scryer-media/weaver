// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { ReactNode } from "react";
import type { Egress, NetworkFlow as Flow } from "@/lib/networking";
import type { Consumer, NetworkingAction, NetworkingData } from "./data";
import { DnsFlowEditor } from "./DnsPage";
import { EgressEditor } from "./EgressPage";
import type { FlowTarget } from "./NetworkFlow";
import { PoolEditor } from "./PoolsPage";
import { RouteDialog } from "./RoutesPage";

/**
 * The editor for whatever was picked in the network flow: the same dialog its
 * own page opens for it.
 *
 * A proxy's editor belongs to the proxies panel, so whoever mounts the flow
 * hands it in. A DNS profile's or list's editor is the DNS page's;
 * `onDnsChanged` hears what it changed.
 */
export function FlowEditor({
  target,
  onClose,
  data,
  egresses,
  consumers,
  flow,
  action,
  busy,
  proxyEditor,
  onDnsChanged,
}: {
  target: FlowTarget | null;
  onClose: () => void;
  data: NetworkingData;
  egresses: Egress[];
  consumers: Consumer[];
  flow: Flow;
  action: NetworkingAction;
  busy: boolean;
  proxyEditor: (id: number, onClose: () => void) => ReactNode;
  onDnsChanged?: () => void;
}) {
  if (!target) {
    return null;
  }
  if (target.kind === "proxy") {
    return proxyEditor(target.id, onClose);
  }
  if (target.kind === "dnsProfile" || target.kind === "dnsList") {
    return (
      <DnsFlowEditor
        key={`${target.kind}:${target.id}`}
        target={target}
        onClose={onClose}
        onChanged={onDnsChanged}
      />
    );
  }
  if (target.kind === "egress") {
    const egress = egresses.find((candidate) => candidate.id === target.id);
    return egress ? (
      <EgressEditor
        key={egress.id}
        data={data}
        egress={egress}
        action={action}
        busy={busy}
        onClose={onClose}
      />
    ) : null;
  }
  if (target.kind === "pool") {
    const pool = data.proxyPools.find(
      (candidate) => candidate.id === target.id,
    );
    return pool ? (
      <PoolEditor
        key={pool.id}
        data={data}
        pool={pool}
        action={action}
        busy={busy}
        onClose={onClose}
      />
    ) : null;
  }
  const consumer = consumers.find(
    (candidate) => candidate.key === target.consumer,
  );
  return consumer ? (
    <RouteDialog
      key={consumer.key}
      data={data}
      consumer={consumer}
      flow={flow}
      action={action}
      busy={busy}
      onClose={onClose}
    />
  ) : null;
}
