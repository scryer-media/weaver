import { useState } from "react";
import { useClient } from "urql";
import { CREATE_EGRESS, DELETE_EGRESS, TEST_EGRESS, UPDATE_EGRESS } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import type { Egress } from "@/lib/networking";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import { Dialog } from "../../components/Dialog";
import { RecordEditor } from "../../components/RecordEditor";
import { PrimaryButton, SecondaryButton, TextField } from "../../components/controls";
import { Cell } from "../../components/rows";
import { FieldRows, PanelControls, SettingsBlocks, type FieldSpec, type SettingsBlock } from "../../pages/settings/framework";
import { egressInput, liveAddresses, type NetworkingAction, type NetworkingData } from "./data";
import { bindingLabel, formatLimit, Hint, MIB, stateLabel, StatusMark } from "./presentation";

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

export function EgressPage({
  data,
  egresses,
  action,
  busy,
}: {
  data: NetworkingData;
  egresses: Egress[];
  action: NetworkingAction;
  busy: boolean;
}) {
  const t = useTranslate();
  const [editing, setEditing] = useState<Egress | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<Egress | null>(null);
  const [testing, setTesting] = useState<Egress | null>(null);
  const kinds = data.platformNetworking.egressBindingKinds;

  const open = (egress: Egress | null) => {
    setError(null);
    setEditing(
      egress ?? { ...NEW_EGRESS, bindingKind: kinds.includes("INTERFACE") ? "INTERFACE" : "SOURCE_ADDRESS" },
    );
  };
  const patch = (next: Partial<Egress>) => setEditing((current) => (current ? { ...current, ...next } : current));

  const save = async () => {
    if (!editing) {
      return;
    }
    const missing = !editing.name.trim()
      ? t("next.networking.egress.nameRequired")
      : editing.bindingKind === "INTERFACE" && !editing.interfaceName
        ? t("next.networking.egress.interfaceRequired")
        : editing.bindingKind === "SOURCE_ADDRESS" && !editing.sourceAddress?.trim()
          ? t("next.networking.egress.sourceRequired")
          : null;
    if (missing) {
      setError(missing);
      return;
    }
    const failure = await action(editing.id < 0 ? CREATE_EGRESS : UPDATE_EGRESS, {
      id: editing.id,
      input: egressInput(editing),
    });
    if (failure) {
      setError(failure);
    } else {
      setEditing(null);
    }
  };

  const remove = async () => {
    if (!deleting) {
      return;
    }
    if (!(await action(DELETE_EGRESS, { id: deleting.id }))) {
      setEditing(null);
    }
    setDeleting(null);
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "egress",
      title: t("next.networking.egress.title"),
      columns: "minmax(0, 1fr) minmax(0, 1.4fr) minmax(0, 0.9fr) 104px",
      headers: [
        t("next.networking.egress.name"),
        t("next.networking.egress.bindingColumn"),
        t("next.networking.egress.health"),
        t("next.networking.egress.limit"),
      ],
      empty: t("next.networking.egress.empty"),
      emptyAction: { label: t("next.networking.egress.add"), onClick: () => open(null) },
      onRowClick: (id) => {
        const egress = egresses.find((candidate) => String(candidate.id) === id);
        if (egress) {
          open(egress);
        }
      },
      rows: egresses.map((egress) => {
        const addresses = liveAddresses(egress, data).join(", ");
        const binding = egress.interfaceName ?? egress.sourceAddress ?? bindingLabel(t, "SYSTEM");
        return {
          id: String(egress.id),
          searchText: `${egress.name} ${binding} ${addresses} ${egress.health}`,
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
          ],
        };
      }),
    },
  ];

  const system = editing ? isSystem(editing) : false;
  const discovered = data.discoverNetworkInterfaces;
  const fields: FieldSpec[] = editing
    ? [
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
      ]
    : [];

  return (
    <>
      <PanelControls>
        <PrimaryButton icon="add" onClick={() => open(null)}>
          {t("next.networking.egress.add")}
        </PrimaryButton>
      </PanelControls>
      {data.platformNetworking.sourceAddressHint ? <Hint>{data.platformNetworking.sourceAddressHint}</Hint> : null}
      <SettingsBlocks blocks={blocks} />

      <RecordEditor
        open={editing !== null}
        title={editing && editing.id >= 0 ? editing.name : t("next.networking.egress.add")}
        note={editing && editing.id >= 0 ? bindingLabel(t, editing.bindingKind) : t("next.networking.egress.newNote")}
        error={error}
        busy={busy}
        saveLabel={t("next.networking.egress.save")}
        onSave={() => void save()}
        onDismiss={() => setEditing(null)}
        onDelete={editing && editing.id > 0 ? () => setDeleting(editing) : undefined}
        deleteLabel={t("next.networking.egress.delete")}
        extraActions={
          editing && editing.id >= 0 ? (
            <SecondaryButton icon="test" onClick={() => setTesting(editing)}>
              {t("next.networking.egress.test")}
            </SecondaryButton>
          ) : null
        }
        sections={[{ id: "egress", title: t("next.networking.egress.section"), fields }]}
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

      {testing ? <EgressTest key={testing.id} egress={testing} onClose={() => setTesting(null)} /> : null}
    </>
  );
}

/** Connect out through one egress and report the source address the far end saw. */
function EgressTest({ egress, onClose }: { egress: Egress; onClose: () => void }) {
  const t = useTranslate();
  const client = useClient();
  const [host, setHost] = useState("");
  const [port, setPort] = useState(443);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; text: string } | null>(null);

  const run = async () => {
    setRunning(true);
    try {
      const response = await client.mutation(TEST_EGRESS, { id: egress.id, host: host.trim(), port }).toPromise();
      const outcome = response.data?.testEgressInterface as
        | { success: boolean; message: string; sourceAddress: string | null; connectMillis: number | null }
        | undefined;
      if (response.error || !outcome) {
        setResult({ ok: false, text: response.error?.message ?? t("next.networking.egress.testFailed") });
      } else {
        setResult({
          ok: outcome.success,
          text: [
            outcome.message,
            ...(outcome.sourceAddress ? [t("next.networking.egress.source", { address: outcome.sourceAddress })] : []),
            ...(outcome.connectMillis != null ? [`${outcome.connectMillis} ms`] : []),
          ].join(" · "),
        });
      }
    } finally {
      setRunning(false);
    }
  };

  return (
    <Dialog
      open
      title={t("next.networking.egress.testTitle", { name: egress.name })}
      onDismiss={onClose}
      footer={
        <>
          <SecondaryButton onClick={onClose}>{t("next.networking.close")}</SecondaryButton>
          <PrimaryButton icon="test" disabled={running || !host.trim()} onClick={() => void run()}>
            {running ? t("next.networking.egress.connecting") : t("next.networking.egress.connect")}
          </PrimaryButton>
        </>
      }
    >
      <Hint>{t("next.networking.egress.testNote")}</Hint>
      <FieldRows
        fields={[
          {
            id: "host",
            label: t("next.networking.test.host"),
            control: { kind: "text", value: host, onChange: setHost, placeholder: "news.example.invalid" },
          },
          {
            id: "port",
            label: t("next.networking.test.port"),
            control: { kind: "number", value: port, min: 1, max: 65535, onChange: setPort },
          },
        ]}
      />
      {result ? (
        <div role="status" className="px-4 py-4 sm:px-6">
          <StatusMark
            state={result.ok ? "UP" : "DOWN"}
            label={result.ok ? t("next.networking.test.passed") : t("next.networking.test.failed")}
            detail={result.text}
            wrap
          />
        </div>
      ) : null}
    </Dialog>
  );
}
