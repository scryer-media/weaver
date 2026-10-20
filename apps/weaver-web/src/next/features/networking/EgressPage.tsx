// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { memo, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  BANDWIDTH_QUERY,
  BANDWIDTH_SPLIT_QUERY,
  CREATE_EGRESS,
  DELETE_EGRESS,
  RESET_EGRESS_USAGE,
  SET_BANDWIDTH,
  SET_EGRESS_QUOTA,
  UPDATE_EGRESS,
} from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import type { Bandwidth, BandwidthSplitState, Egress } from "@/lib/networking";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import { RecordEditor } from "../../components/RecordEditor";
import { SecondaryButton, TextField } from "../../components/controls";
import { Cell } from "../../components/rows";
import {
  SettingsBlocks,
  useDraft,
  usePanelState,
  type FieldSpec,
  type SettingsBlock,
  type SettingsSectionModel,
} from "../../pages/settings/framework";
import { egressInput, liveAddresses, type NetworkingAction, type NetworkingData } from "./data";
import {
  egressQuotaChanged,
  egressQuotaDraft,
  egressQuotaFields,
  egressQuotaInput,
  egressQuotaProblem,
  metered,
  QuotaCell,
  quotaSearchText,
} from "./egress-quota";
import { LimitControl, SplitControl } from "./limits";
import { bindingLabel, formatLimit, stateLabel, StatusMark } from "./presentation";
import { TestViaDialog } from "./TestViaDialog";

const NEW_EGRESS: Egress = {
  id: -1,
  name: "",
  bindingKind: "INTERFACE",
  interfaceName: "",
  sourceAddress: "",
  enabled: true,
  downloadBps: null,
  uploadBps: null,
  health: "UNKNOWN",
  reason: null,
};

/** How often the rates the split holds each side to are read again. */
const SPLIT_REFRESH_MS = 2000;

/** The system egress is the operating system's own routing; it can be limited, never renamed or rebound. */
const isSystem = (egress: Egress) => egress.id === 0;

/**
 * The global limits, over every egress together, one per direction. They save
 * through `setBandwidth`, which replaces both at once, and own the top bar's
 * Save and Revert. The status line they publish also carries what the egress
 * table last wrote, unless the limits have an edit pending.
 */
function useGlobalBandwidth(busy: boolean, message: string | null, failed: boolean) {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{ networking: { bandwidth: Bandwidth } }>({
    query: BANDWIDTH_QUERY,
  });
  const [{ data: splitData }, refreshSplit] = useQuery<{ networking: { bandwidthSplit: BandwidthSplitState } }>({
    query: BANDWIDTH_SPLIT_QUERY,
  });
  const [updateState, setBandwidth] = useMutation(SET_BANDWIDTH);
  const [status, setStatus] = useState<string | null>(null);

  useEffect(() => {
    const timer = window.setInterval(() => refreshSplit({ requestPolicy: "network-only" }), SPLIT_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [refreshSplit]);
  const [error, setError] = useState<string | null>(null);

  // A write to an egress takes the status line over from the limits' last save.
  useEffect(() => {
    if (busy) {
      setStatus(null);
      setError(null);
    }
  }, [busy]);

  const source = useMemo<Bandwidth | null>(() => {
    const bandwidth = data?.networking?.bandwidth;
    return bandwidth
      ? {
          downloadBps: bandwidth.downloadBps ?? null,
          uploadBps: bandwidth.uploadBps ?? null,
          split: bandwidth.split ? { usenetPct: bandwidth.split.usenetPct } : null,
        }
      : null;
  }, [data?.networking?.bandwidth]);

  const draft = useDraft(source, () => {
    setStatus(null);
    setError(null);
  });
  const values = draft.value;
  const actionMessage = draft.dirty ? null : message;

  usePanelState({
    dirty: draft.dirty,
    busy: updateState.fetching,
    status: error ?? status ?? actionMessage,
    failed: error !== null || (status === null && actionMessage !== null && failed),
    revert: () => {
      setError(null);
      draft.revert();
    },
    save: () => {
      if (!values) {
        return;
      }
      setError(null);
      const input = { downloadBps: values.downloadBps, uploadBps: values.uploadBps, split: values.split };
      void setBandwidth({ input }).then((result) => {
        if (result.error || !result.data?.setBandwidth) {
          setError(result.error?.message ?? t("next.bandwidth.saveFailed"));
          return;
        }
        draft.markSaved();
        setStatus(t("next.settings.saved"));
        void reexecute({ requestPolicy: "network-only" });
        refreshSplit({ requestPolicy: "network-only" });
      });
    },
  });

  const limit = (id: "downloadBps" | "uploadBps", label: string): FieldSpec => ({
    id,
    label,
    keywords: "speed limit throttle rate bandwidth",
    control: {
      kind: "custom",
      control: values ? (
        <LimitControl
          label={label}
          value={values[id]}
          suffix="MB/s"
          onChange={(next) => draft.set({ ...values, [id]: next })}
        />
      ) : null,
    },
  });

  // The rates each side is held to, while a split is in force over a download limit.
  const live = splitData?.networking?.bandwidthSplit;
  const rates =
    live?.inForce && !live.needsDownloadLimit
      ? `${t("next.bandwidth.splitUsenet")} ↓ ${formatLimit(t, live.usenetBps)} · ${t("next.bandwidth.splitTorrentsRate")} ↓ ${formatLimit(t, live.torrentBps)}`
      : undefined;
  const split: FieldSpec = {
    id: "split",
    label: t("next.bandwidth.split"),
    help: rates,
    keywords: "split share usenet torrents bandwidth",
    control: {
      kind: "custom",
      control: values ? (
        <SplitControl
          value={values.split}
          needsDownloadLimit={values.downloadBps === null}
          onChange={(next) => draft.set({ ...values, split: next })}
        />
      ) : null,
    },
  };

  const block: SettingsSectionModel | null = values
    ? {
        kind: "section",
        id: "limits",
        title: t("next.bandwidth.limits"),
        fields: [
          limit("downloadBps", t("next.bandwidth.downloadLimit")),
          split,
          limit("uploadBps", t("next.bandwidth.uploadLimit")),
        ],
      }
    : null;

  return { block, loading: fetching && !data };
}

/** The egress table's limit cell: one line per direction. */
function LimitCell({ egress }: { egress: Egress }) {
  const t = useTranslate();
  return (
    <div className="flex min-w-0 flex-col gap-[3px]">
      {(
        [
          ["↓", egress.downloadBps, "next.quota.download"],
          ["↑", egress.uploadBps, "next.quota.upload"],
        ] as const
      ).map(([arrow, value, label]) => (
        <Cell key={arrow} mono className={value === null ? "text-wv-muted" : undefined} title={t(label)}>
          {`${arrow} ${formatLimit(t, value)}`}
        </Cell>
      ))}
    </div>
  );
}

export const EgressPage = memo(function EgressPage({
  data,
  egresses,
  action,
  busy,
  message = null,
  failed = false,
}: {
  data: NetworkingData;
  egresses: Egress[];
  action: NetworkingAction;
  busy: boolean;
  /** The outcome of the last write, for the status line. */
  message?: string | null;
  failed?: boolean;
}) {
  const t = useTranslate();
  const [editing, setEditing] = useState<Egress | null>(null);
  const ceiling = useGlobalBandwidth(busy, message, failed);
  const kinds = data.platformNetworking.egressBindingKinds;
  const listed = [...egresses.filter(isSystem), ...egresses.filter((egress) => !isSystem(egress))];

  const open = (egress: Egress | null) =>
    setEditing(
      egress ?? { ...NEW_EGRESS, bindingKind: kinds.includes("INTERFACE") ? "INTERFACE" : "SOURCE_ADDRESS" },
    );

  const blocks: (SettingsBlock | null)[] = [
    ceiling.block,
    {
      kind: "table",
      id: "egress",
      title: t("next.networking.egress.title"),
      columns: "minmax(0, 1fr) minmax(0, 1.2fr) minmax(0, 0.8fr) 120px minmax(0, 1.6fr) 80px",
      headers: [
        t("next.networking.egress.name"),
        t("next.networking.egress.bindingColumn"),
        t("next.networking.egress.health"),
        t("next.networking.egress.limit"),
        t("next.networking.egress.quota"),
        "",
      ],
      empty: t("next.networking.egress.empty"),
      add: { label: t("next.networking.egress.add"), onClick: () => open(null) },
      onRowClick: (id) => {
        const egress = egresses.find((candidate) => String(candidate.id) === id);
        if (egress) {
          open(egress);
        }
      },
      rows: listed.map((egress) => {
        const addresses = liveAddresses(egress, data).join(", ");
        const binding = egress.interfaceName ?? egress.sourceAddress ?? bindingLabel(t, "SYSTEM");
        return {
          id: String(egress.id),
          searchText: [
            egress.name,
            binding,
            addresses,
            egress.health,
            t("next.networking.egress.quota"),
            quotaSearchText(t, egress),
          ].join(" "),
          cells: [
            <Cell key="name" title={egress.name}>
              {egress.name}
            </Cell>,
            <div key="binding" className="flex min-w-0 flex-col gap-[3px]">
              <Cell mono title={binding}>
                {binding}
              </Cell>
              {addresses ? (
                <Cell mono className="text-[11px] text-wv-muted" title={addresses}>
                  {addresses}
                </Cell>
              ) : null}
            </div>,
            <span key="health" title={egress.reason ?? undefined} className="flex min-w-0 flex-col gap-[3px]">
              <StatusMark state={egress.health} label={stateLabel(t, egress.health)} />
              {egress.enabled ? null : (
                <span className="font-wv-mono text-[11px] text-wv-muted">{t("next.networking.disabled")}</span>
              )}
            </span>,
            <LimitCell key="limit" egress={egress} />,
            <QuotaCell key="quota" t={t} egress={egress} />,
            <span key="edit" className="ml-auto" onClick={(event) => event.stopPropagation()}>
              <SecondaryButton size="compact" icon="edit" onClick={() => open(egress)}>
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
      <SettingsBlocks blocks={blocks} loading={ceiling.loading} />
      {editing ? (
        <EgressEditor
          key={editing.id}
          data={data}
          egress={editing}
          action={action}
          busy={busy}
          onClose={() => setEditing(null)}
        />
      ) : null}
    </>
  );
});

/**
 * One egress in its editor, mounted while it is open. The egress table opens
 * it, and so does an egress picked in the network flow. An id below zero is an
 * egress that has not been saved yet.
 */
export function EgressEditor({
  data,
  egress,
  action,
  busy,
  onClose,
}: {
  data: NetworkingData;
  egress: Egress;
  action: NetworkingAction;
  busy: boolean;
  onClose: () => void;
}) {
  const t = useTranslate();
  const [editing, setEditing] = useState(egress);
  const [quota, setQuota] = useState(() => egressQuotaDraft(egress.quota));
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<Egress | null>(null);
  const [testing, setTesting] = useState<Egress | null>(null);
  const kinds = data.platformNetworking.egressBindingKinds;
  const patch = (next: Partial<Egress>) => setEditing((current) => ({ ...current, ...next }));

  const save = async () => {
    const missing = !editing.name.trim()
      ? t("next.networking.egress.nameRequired")
      : editing.bindingKind === "INTERFACE" && !editing.interfaceName
        ? t("next.networking.egress.interfaceRequired")
        : editing.bindingKind === "SOURCE_ADDRESS" && !editing.sourceAddress?.trim()
          ? t("next.networking.egress.sourceRequired")
          : egressQuotaProblem(t, quota);
    if (missing) {
      setError(missing);
      return;
    }
    let id = editing.id;
    const failure = await action(
      editing.id < 0 ? CREATE_EGRESS : UPDATE_EGRESS,
      { id: editing.id, input: egressInput(editing) },
      (saved) => {
        const created = (saved as { createEgressInterface?: { id: number } } | undefined)?.createEgressInterface;
        if (created) {
          id = created.id;
        }
      },
    );
    // The quota saves on its own, once the egress it caps exists.
    const quotaFailure =
      failure ?? (egressQuotaChanged(quota, egress.quota) && id >= 0
        ? await action(SET_EGRESS_QUOTA, { id, input: egressQuotaInput(quota) })
        : null);
    if (quotaFailure) {
      setError(quotaFailure);
    } else {
      onClose();
    }
  };

  const remove = async () => {
    if (!deleting) {
      return;
    }
    const failure = await action(DELETE_EGRESS, { id: deleting.id });
    setDeleting(null);
    if (!failure) {
      onClose();
    }
  };

  const system = isSystem(editing);
  const discovered = data.discoverNetworkInterfaces;
  const fields: FieldSpec[] = [
    {
      id: "name",
      label: t("next.networking.egress.name"),
      control: system
        ? { kind: "static", value: editing.name }
        : { kind: "text", mono: false, value: editing.name, onChange: (name) => patch({ name }) },
    },
    {
      id: "binding",
      label: t("next.networking.egress.binding"),
      help: t("next.networking.egress.bindingHelp"),
      control: system
        ? { kind: "static", value: bindingLabel(t, "SYSTEM") }
        : {
            kind: "select",
            value: editing.bindingKind,
            options: kinds
              .filter((kind) => kind !== "SYSTEM")
              .map((kind) => ({ value: kind, label: bindingLabel(t, kind) })),
            onChange: (kind) => patch({ bindingKind: kind as Egress["bindingKind"] }),
          },
    },
    ...(editing.bindingKind === "INTERFACE"
      ? [
          {
            id: "interface",
            label: t("next.networking.egress.interface"),
            control: {
              kind: "select" as const,
              value: editing.interfaceName ?? "",
              className: "w-[268px] max-w-full",
              options: [
                { value: "", label: t("next.networking.egress.chooseInterface") },
                ...(editing.interfaceName && !discovered.some((candidate) => candidate.name === editing.interfaceName)
                  ? [
                      {
                        value: editing.interfaceName,
                        label: t("next.networking.egress.missingInterface", { name: editing.interfaceName }),
                      },
                    ]
                  : []),
                ...discovered.map((candidate) => ({
                  value: candidate.name,
                  label: [
                    candidate.name,
                    candidate.up ? t("next.networking.egress.linkUp") : t("next.networking.egress.linkDown"),
                    ...(candidate.addresses.length ? [candidate.addresses.join(", ")] : []),
                  ].join(" · "),
                })),
              ],
              onChange: (interfaceName: string) => patch({ interfaceName }),
            },
          },
        ]
      : []),
    ...(editing.bindingKind === "SOURCE_ADDRESS"
      ? [
          {
            id: "source",
            label: t("next.networking.egress.sourceAddress"),
            help: data.platformNetworking.sourceAddressHint || undefined,
            control: {
              kind: "custom" as const,
              control: (
                <>
                  <TextField
                    label={t("next.networking.egress.sourceAddress")}
                    list="network-source-addresses"
                    value={editing.sourceAddress ?? ""}
                    onChange={(sourceAddress) => patch({ sourceAddress })}
                    className="w-[268px] max-w-full"
                  />
                  <datalist id="network-source-addresses">
                    {discovered.flatMap((candidate) =>
                      candidate.addresses.map((address) => (
                        <option key={`${candidate.name}:${address}`} value={address}>
                          {candidate.name}
                        </option>
                      )),
                    )}
                  </datalist>
                </>
              ),
            },
          },
        ]
      : []),
    {
      id: "downloadLimit",
      label: t("next.networking.egress.downloadLimit"),
      keywords: "speed limit throttle rate",
      control: {
        kind: "custom",
        control: (
          <LimitControl
            label={t("next.networking.egress.downloadLimit")}
            value={editing.downloadBps}
            onChange={(downloadBps) => patch({ downloadBps })}
          />
        ),
      },
    },
    {
      id: "uploadLimit",
      label: t("next.networking.egress.uploadLimit"),
      keywords: "speed limit throttle rate",
      control: {
        kind: "custom",
        control: (
          <LimitControl
            label={t("next.networking.egress.uploadLimit")}
            value={editing.uploadBps}
            onChange={(uploadBps) => patch({ uploadBps })}
          />
        ),
      },
    },
    {
      id: "enabled",
      label: t("next.networking.egress.enabled"),
      help: system ? t("next.networking.egress.systemHelp") : undefined,
      control: { kind: "toggle", value: editing.enabled, disabled: system, onChange: (enabled) => patch({ enabled }) },
    },
  ];

  return (
    <>
      <RecordEditor
        open
        title={editing.id >= 0 ? editing.name : t("next.networking.egress.add")}
        note={editing.id >= 0 ? bindingLabel(t, editing.bindingKind) : t("next.networking.egress.newNote")}
        error={error}
        busy={busy}
        saveLabel={t("next.networking.egress.save")}
        onSave={() => void save()}
        onDismiss={onClose}
        onDelete={editing.id > 0 ? () => setDeleting(editing) : undefined}
        deleteLabel={t("next.networking.egress.delete")}
        extraActions={
          editing.id >= 0 ? (
            <>
              {metered(egress) ? (
                <SecondaryButton
                  icon="reset"
                  disabled={busy}
                  onClick={() => {
                    // `action` re-executes the networking and flow queries on success.
                    void action(RESET_EGRESS_USAGE, { id: editing.id }).then((failure) => {
                      setError(failure ?? null);
                    });
                  }}
                >
                  {t("next.providers.resetUsage")}
                </SecondaryButton>
              ) : null}
              <SecondaryButton icon="test" onClick={() => setTesting(editing)}>
                {t("next.networking.egress.test")}
              </SecondaryButton>
            </>
          ) : null
        }
        sections={[
          { id: "egress", title: t("next.networking.egress.section"), fields },
          {
            id: "quota",
            title: t("next.networking.egress.quota"),
            fields: egressQuotaFields(t, quota, setQuota, {
              down: egress.quotaUsage?.down.usedBytes,
              up: egress.quotaUsage?.up.usedBytes,
            }),
          },
        ]}
      />

      <ConfirmDialog
        open={deleting !== null}
        title={t("next.networking.egress.delete")}
        note={deleting?.name}
        busy={busy}
        confirmLabel={t("next.networking.egress.delete")}
        body={t("next.networking.egress.deleteBody", { name: deleting?.name ?? "" })}
        onConfirm={() => void remove()}
        onDismiss={() => setDeleting(null)}
      />

      {testing ? (
        <TestViaDialog key={testing.id} subject={null} egressId={testing.id} onClose={() => setTesting(null)} />
      ) : null}
    </>
  );
}
