// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState, type ReactNode } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import type { NetworkFlow as Flow } from "@/lib/networking";
import { SecondaryButton } from "../../components/controls";
import { Dialog } from "../../components/Dialog";
import type { Consumer, useNetworkingWorkspace } from "./data";
import type { useFlowDns } from "./dns-choices";
import { FlowEditor } from "./FlowEditor";
import { NetworkFlow, type FlowTarget } from "./NetworkFlow";

type Workspace = ReturnType<typeof useNetworkingWorkspace>;

/**
 * The live connection flow drawn small, and full size once picked, where every
 * box opens its editor as on the networking overview. A job's panel shows its
 * own routes this way; the rail shows every route.
 *
 * `editing` is the parent's, so a control beside the drawing can open an
 * editor too.
 */
export function FlowPreview({
  workspace,
  dns,
  flow,
  consumers,
  note,
  editing,
  onEdit,
  proxyEditor,
  className,
}: {
  workspace: Workspace & { data: NonNullable<Workspace["data"]> };
  dns: ReturnType<typeof useFlowDns>;
  /** What is drawn: the whole flow, or the legs of `consumers` only. */
  flow: Flow;
  consumers: Consumer[];
  /** The enlarged view's note. */
  note?: string;
  editing: FlowTarget | null;
  onEdit: (target: FlowTarget | null) => void;
  proxyEditor: (
    id: number,
    onClose: () => void,
    onChanged: () => void,
  ) => ReactNode;
  /** The small drawing's frame; by default a hairline box. */
  className?: string;
}) {
  const t = useTranslate();
  const {
    data,
    flow: whole,
    consumers: every,
    egresses,
    profiles,
    pools,
    action,
    busy,
    retry,
  } = workspace;
  const [enlarged, setEnlarged] = useState(false);
  return (
    <>
      <div
        role="button"
        tabIndex={0}
        title={t("next.job.enlargeFlow")}
        aria-label={t("next.job.enlargeFlow")}
        className={
          className ??
          "min-w-0 cursor-zoom-in border border-wv-hairline hover:border-wv-control"
        }
        onClick={() => setEnlarged(true)}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            setEnlarged(true);
          }
        }}
      >
        <NetworkFlow
          flow={flow}
          egresses={egresses}
          profiles={profiles}
          pools={pools}
          consumers={consumers}
          traffic
          fitWidth
          dns={dns.dns}
        />
      </div>
      <Dialog
        open={enlarged}
        title={t("next.networking.overview.flow")}
        note={note}
        // As wide as the window allows: the drawing scales up to fill it.
        width={2400}
        onDismiss={() => setEnlarged(false)}
        footer={
          <SecondaryButton onClick={() => setEnlarged(false)}>
            {t("next.networking.close")}
          </SecondaryButton>
        }
      >
        <NetworkFlow
          flow={flow}
          egresses={egresses}
          profiles={profiles}
          pools={pools}
          consumers={consumers}
          onEdit={onEdit}
          traffic
          trace
          fitWidth
          dns={dns.dns}
        />
      </Dialog>
      {editing ? (
        <FlowEditor
          target={editing}
          onClose={() => onEdit(null)}
          data={data}
          egresses={egresses}
          consumers={every}
          flow={whole}
          action={action}
          busy={busy}
          proxyEditor={(id, onClose) => proxyEditor(id, onClose, retry)}
          onDnsChanged={dns.refresh}
        />
      ) : null}
    </>
  );
}
