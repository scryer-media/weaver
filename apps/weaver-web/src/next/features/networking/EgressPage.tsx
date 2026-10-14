import { memo, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import { CREATE_EGRESS, DELETE_EGRESS, RESET_EGRESS_USAGE, UPDATE_EGRESS } from "@/graphql/networking";
import { SETTINGS_QUERY, UPDATE_SETTINGS_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import type { Egress } from "@/lib/networking";
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
import { quotaDraft, quotaFields, quotaInput, quotaProblem } from "../../pages/settings/quota";
import { egressInput, liveAddresses, type NetworkingAction, type NetworkingData } from "./data";
import { QuotaCell, quotaSearchText } from "./egress-quota";
import { bindingLabel, formatLimit, Hint, MIB, stateLabel, StatusMark } from "./presentation";
import { TestViaDialog } from "./TestViaDialog";

const NEW_EGRESS: Egress = {
  id: -1,
  name: "",
  bindingKind: "INTERFACE",
  interfaceName: "",
  sourceAddress: "",
  enabled: true,
  maxDownloadSpeed: 0,
  health: "UNKNOWN",
  reason: null,
};

/** The system egress is the operating system's own routing; it can be limited, never renamed or rebound. */
const isSystem = (egress: Egress) => egress.id === 0;

/**
 * The global download ceiling, over every egress together. It is a daemon
 * setting rather than an egress's, so it saves through the settings and owns
 * the top bar's Save and Revert. The status line it publishes also carries
 * what the egress table last wrote, unless the ceiling has an edit pending.
 */
function useDownloadCeiling(busy: boolean, message: string | null, failed: boolean) {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{ settings: { maxDownloadSpeed: number } }>({
    query: SETTINGS_QUERY,
  });
  const [updateState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  // A write to an egress takes the status line over from the ceiling's last save.
  useEffect(() => {
    if (busy) {
      setStatus(null);
      setError(null);
    }
  }, [busy]);

  const source = useMemo<{ maxDownloadSpeed: number } | null>(() => {
    const settings = data?.settings;
    return settings ? { maxDownloadSpeed: settings.maxDownloadSpeed ?? 0 } : null;
  }, [data?.settings]);

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
      void updateSettings({ input: { maxDownloadSpeed: values.maxDownloadSpeed } }).then((result) => {
        if (result.error || !result.data?.updateSettings) {
          setError(result.error?.message ?? t("next.bandwidth.saveFailed"));
          return;
        }
        draft.markSaved();
        setStatus(t("next.settings.saved"));
        void reexecute({ requestPolicy: "network-only" });
      });
    },
  });

  const block: SettingsSectionModel | null = values
    ? {
        kind: "section",
        id: "limits",
        title: t("next.bandwidth.limits"),
        fields: [
          {
            id: "maxDownloadSpeed",
            label: t("next.bandwidth.ceiling"),
            help: t("next.bandwidth.ceilingHelp"),
            keywords: "speed limit throttle rate",
            // The same field as the speed-limit dialog, so the ceiling reads and
            // edits identically wherever it is set.
            control: {
              kind: "number",
              value: Math.round((values.maxDownloadSpeed / MIB) * 10) / 10,
              min: 0,
              onChange: (next) => draft.set({ maxDownloadSpeed: Math.round(Math.max(0, next) * MIB) }),
              suffix: "MB/s",
            },
          },
        ],
      }
    : null;

  return { block, loading: fetching && !data };
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
  const ceiling = useDownloadCeiling(busy, message, failed);
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
      columns: "minmax(0, 1fr) minmax(0, 1.2fr) minmax(0, 0.8fr) 104px minmax(0, 1.4fr) 124px",
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
            t("next.networking.egress.quotaSection"),
            t("next.bandwidth.allowance"),
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
            <Cell key="limit" mono className={egress.maxDownloadSpeed ? undefined : "text-wv-muted"}>
              {formatLimit(t, egress.maxDownloadSpeed)}
            </Cell>,
            <QuotaCell key="quota" t={t} egress={egress} />,
            <span key="edit" className="ml-auto" onClick={(event) => event.stopPropagation()}>
              <SecondaryButton size="compact" onClick={() => open(egress)}>
                {t("next.networking.egress.edit")}
              </SecondaryButton>
            </span>,
          ],
        };
      }),
    },
  ];

  return (
    <>
      {data.platformNetworking.sourceAddressHint ? <Hint>{data.platformNetworking.sourceAddressHint}</Hint> : null}
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
  const [quota, setQuota] = useState(() => quotaDraft(egress.downloadQuota));
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
          : quotaProblem(t, quota);
    if (missing) {
      setError(missing);
      return;
    }
    const failure = await action(editing.id < 0 ? CREATE_EGRESS : UPDATE_EGRESS, {
      id: editing.id,
      input: { ...egressInput(editing), downloadQuota: quotaInput(quota) },
    });
    if (failure) {
      setError(failure);
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
      id: "limit",
      label: t("next.networking.egress.downloadLimit"),
      help: t("next.networking.egress.limitHelp"),
      control: {
        kind: "number",
        value: editing.maxDownloadSpeed / MIB,
        min: 0,
        step: 0.1,
        precision: 1,
        suffix: "MiB/s",
        onChange: (limit) => patch({ maxDownloadSpeed: Math.round(limit * MIB) }),
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
              {editing.downloadQuota?.enabled ? (
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
            title: t("next.networking.egress.quotaSection"),
            fields: quotaFields(t, quota, setQuota, {
              label: t("next.networking.egress.quotaEnabled"),
              help: t("next.networking.egress.quotaHelp"),
              usedBytes: egress.downloadQuotaUsage?.usedBytes,
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
