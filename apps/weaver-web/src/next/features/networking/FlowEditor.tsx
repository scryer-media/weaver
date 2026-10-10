import type { ReactNode } from "react";
import type { Egress, NetworkFlow as Flow } from "@/lib/networking";
import type { Consumer, NetworkingAction, NetworkingData } from "./data";
import { EgressEditor } from "./EgressPage";
import type { FlowTarget } from "./NetworkFlow";
import { PoolEditor } from "./PoolsPage";
import { RouteDialog } from "./RoutesPage";

/**
 * The editor for whatever was picked in the network flow: the same dialog its
 * own page opens for it.
 *
 * A proxy's editor belongs to the proxies panel, so whoever mounts the flow
 * hands it in.
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
}) {
  if (!target) {
    return null;
  }
  if (target.kind === "proxy") {
    return proxyEditor(target.id, onClose);
  }
  if (target.kind === "egress") {
    const egress = egresses.find((candidate) => candidate.id === target.id);
    return egress ? (
      <EgressEditor key={egress.id} data={data} egress={egress} action={action} busy={busy} onClose={onClose} />
    ) : null;
  }
  if (target.kind === "pool") {
    const pool = data.proxyPools.find((candidate) => candidate.id === target.id);
    return pool ? (
      <PoolEditor key={pool.id} data={data} pool={pool} action={action} busy={busy} onClose={onClose} />
    ) : null;
  }
  const consumer = consumers.find((candidate) => candidate.key === target.consumer);
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
