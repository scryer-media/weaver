import { useState, type ReactNode } from "react";
import { useClient } from "urql";
import { CREATE_POOL, DELETE_POOL, TEST_POOL, UPDATE_POOL } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import type { ProxyPool } from "@/lib/networking";
import { proxyLabels, type ProxyKind } from "@/lib/proxies";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import { Dialog } from "../../components/Dialog";
import { RecordEditor } from "../../components/RecordEditor";
import { PrimaryButton, SecondaryButton } from "../../components/controls";
import { Cell } from "../../components/rows";
import { countLabel } from "../../i18n/labels";
import { FieldRows, SettingsBlocks, type SettingsBlock } from "../../pages/settings/framework";
import type { NetworkingAction, NetworkingData } from "./data";
import { CheckRow, Hint, StatusMark } from "./presentation";

const NEW_POOL: ProxyPool = { id: -1, name: "", kind: "SOCKS5", memberIds: [], enabled: true };

/** WireGuard keeps a tunnel per member per egress, and each reserves this much memory. */
const WIREGUARD_BUDGET_MIB = 516;

/**
 * Proxy pools, then the proxies themselves.
 *
 * A pool races proxies of one type and settles on the members with delivery
 * evidence behind them. The proxy list below is the proxies panel, unchanged;
 * this page only adds the pools that group its entries.
 */
export function PoolsPage({
  data,
  action,
  busy,
  proxies,
}: {
  data: NetworkingData;
  action: NetworkingAction;
  busy: boolean;
  proxies: ReactNode;
}) {
  const t = useTranslate();
  const [editing, setEditing] = useState<ProxyPool | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<ProxyPool | null>(null);
  const [testing, setTesting] = useState<ProxyPool | null>(null);
  const profileName = (id: number) =>
    data.proxyProfiles.find((profile) => profile.id === id)?.name ?? t("next.networking.proxyNumber", { id });

  const open = (pool: ProxyPool | null) => {
    setError(null);
    setEditing(pool ?? NEW_POOL);
  };
  const patch = (next: Partial<ProxyPool>) => setEditing((current) => (current ? { ...current, ...next } : current));

  const save = async () => {
    if (!editing) {
      return;
    }
    if (!editing.name.trim()) {
      setError(t("next.networking.pools.nameRequired"));
      return;
    }
    const { id, ...input } = editing;
    const failure = await action(id < 0 ? CREATE_POOL : UPDATE_POOL, { id, input });
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
    if (!(await action(DELETE_POOL, { id: deleting.id }))) {
      setEditing(null);
    }
    setDeleting(null);
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "pools",
      title: t("next.networking.pools.title"),
      note: countLabel(t, "next.networking.pools.count", data.proxyPools.length),
      columns: "minmax(0, 1fr) 132px minmax(0, 1.6fr) 104px",
      headers: [
        t("next.networking.pools.name"),
        t("next.networking.pools.type"),
        t("next.networking.pools.members"),
        t("next.networking.pools.state"),
      ],
      empty: t("next.networking.pools.empty"),
      emptyAction: { label: t("next.networking.pools.add"), onClick: () => open(null) },
      footer: data.proxyPools.length ? (
        <SecondaryButton icon="add" size="compact" onClick={() => open(null)}>
          {t("next.networking.pools.add")}
        </SecondaryButton>
      ) : undefined,
      onRowClick: (id) => {
        const pool = data.proxyPools.find((candidate) => String(candidate.id) === id);
        if (pool) {
          open(pool);
        }
      },
      rows: data.proxyPools.map((pool) => {
        const members = pool.memberIds.map(profileName).join(", ");
        return {
          id: String(pool.id),
          searchText: `${pool.name} ${proxyLabels[pool.kind]} ${members}`,
          cells: [
            <Cell key="name" title={pool.name}>
              {pool.name}
            </Cell>,
            <Cell key="kind" mono>
              {proxyLabels[pool.kind]}
            </Cell>,
            <Cell key="members" mono className="text-wv-secondary" title={members}>
              {members || "—"}
            </Cell>,
            <StatusMark
              key="state"
              state={pool.enabled ? "UP" : "IDLE"}
              label={pool.enabled ? t("next.networking.enabled") : t("next.networking.disabled")}
            />,
          ],
        };
      }),
    },
  ];

  const candidates = editing ? data.proxyProfiles.filter((profile) => profile.kind === editing.kind) : [];

  return (
    <>
      <Hint>
        {t("next.networking.pools.note", {
          count: data.platformNetworking.maxWireguardInstances,
          budget: WIREGUARD_BUDGET_MIB,
        })}
      </Hint>
      <SettingsBlocks blocks={blocks} />
      {proxies}

      <RecordEditor
        open={editing !== null}
        title={editing && editing.id >= 0 ? editing.name : t("next.networking.pools.add")}
        note={editing ? proxyLabels[editing.kind] : undefined}
        error={error}
        busy={busy}
        saveLabel={t("next.networking.pools.save")}
        saveDisabled={(editing?.memberIds.length ?? 0) < 2}
        onSave={() => void save()}
        onDismiss={() => setEditing(null)}
        onDelete={editing && editing.id >= 0 ? () => setDeleting(editing) : undefined}
        deleteLabel={t("next.networking.pools.delete")}
        extraActions={
          editing && editing.id >= 0 ? (
            <SecondaryButton icon="test" onClick={() => setTesting(editing)}>
              {t("next.networking.pools.test")}
            </SecondaryButton>
          ) : null
        }
        sections={
          editing
            ? [
                {
                  id: "pool",
                  title: t("next.networking.pools.section"),
                  fields: [
                    {
                      id: "name",
                      label: t("next.networking.pools.name"),
                      control: { kind: "text", mono: false, value: editing.name, onChange: (name) => patch({ name }) },
                    },
                    {
                      id: "kind",
                      label: t("next.networking.pools.type"),
                      help: t("next.networking.pools.typeHelp"),
                      control: {
                        kind: "select",
                        value: editing.kind,
                        options: Object.entries(proxyLabels).map(([value, label]) => ({ value, label })),
                        onChange: (kind) => patch({ kind: kind as ProxyKind, memberIds: [] }),
                      },
                    },
                    {
                      id: "members",
                      label: t("next.networking.pools.members"),
                      help: t("next.networking.pools.membersHelp"),
                      control: {
                        kind: "custom",
                        control: candidates.length ? (
                          <div role="group" aria-label={t("next.networking.pools.members")} className="flex w-[268px] max-w-full flex-col gap-[10px]">
                            {candidates.map((profile) => (
                              <CheckRow
                                key={profile.id}
                                label={profile.name}
                                detail={profile.enabled ? undefined : t("next.networking.disabled")}
                                checked={editing.memberIds.includes(profile.id)}
                                onChange={(checked) =>
                                  patch({
                                    memberIds: checked
                                      ? [...editing.memberIds, profile.id]
                                      : editing.memberIds.filter((id) => id !== profile.id),
                                  })
                                }
                              />
                            ))}
                          </div>
                        ) : (
                          <span className="w-[268px] max-w-full text-[12.5px] text-wv-muted">
                            {t("next.networking.pools.noCandidates", { kind: proxyLabels[editing.kind] })}
                          </span>
                        ),
                      },
                    },
                    {
                      id: "enabled",
                      label: t("next.networking.pools.enabled"),
                      control: { kind: "toggle", value: editing.enabled, onChange: (enabled) => patch({ enabled }) },
                    },
                  ],
                },
              ]
            : []
        }
      />

      <ConfirmDialog
        open={deleting !== null}
        title={t("next.networking.pools.delete")}
        note={deleting?.name}
        busy={busy}
        confirmLabel={t("next.networking.pools.delete")}
        body={t("next.networking.pools.deleteBody", { name: deleting?.name ?? "" })}
        onConfirm={() => void remove()}
        onDismiss={() => setDeleting(null)}
      />

      {testing ? <PoolTest key={testing.id} pool={testing} data={data} onClose={() => setTesting(null)} /> : null}
    </>
  );
}

type ProbeResult = {
  proxyId: number;
  success: boolean;
  message: string;
  sourceAddress: string | null;
  connectMillis: number | null;
};

/** Probe every member in an isolated session; the live selection is left alone. */
function PoolTest({ pool, data, onClose }: { pool: ProxyPool; data: NetworkingData; onClose: () => void }) {
  const t = useTranslate();
  const client = useClient();
  const usable = data.egressInterfaces.filter((egress) => egress.enabled);
  const [egress, setEgress] = useState(usable[0]?.id ?? 0);
  const [host, setHost] = useState("");
  const [port, setPort] = useState(443);
  const [running, setRunning] = useState(false);
  const [results, setResults] = useState<ProbeResult[]>([]);
  const [error, setError] = useState("");
  const destinationRequired = pool.kind === "SOCKS5" || pool.kind === "HTTP_CONNECT";

  const run = async () => {
    setRunning(true);
    setError("");
    try {
      const result = await client
        .mutation(TEST_POOL, { id: pool.id, egressId: egress, host: host.trim() || null, port: host.trim() ? port : null })
        .toPromise();
      setError(result.error?.message ?? "");
      setResults(result.data?.testProxyPool ?? []);
    } catch (caught) {
      setError(String(caught));
    } finally {
      setRunning(false);
    }
  };

  return (
    <Dialog
      open
      title={t("next.networking.pools.testTitle", { name: pool.name })}
      note={proxyLabels[pool.kind]}
      onDismiss={onClose}
      footer={
        <>
          <SecondaryButton onClick={onClose}>{t("next.networking.close")}</SecondaryButton>
          <PrimaryButton
            icon="test"
            disabled={running || (destinationRequired && !host.trim())}
            onClick={() => void run()}
          >
            {running ? t("next.networking.pools.testing") : t("next.networking.pools.testMembers")}
          </PrimaryButton>
        </>
      }
    >
      <Hint>{t("next.networking.pools.testNote")}</Hint>
      <FieldRows
        fields={[
          {
            id: "egress",
            label: t("next.networking.route.egress"),
            control: {
              kind: "select",
              value: String(egress),
              options: usable.map((candidate) => ({ value: String(candidate.id), label: candidate.name })),
              onChange: (next) => setEgress(Number(next)),
            },
          },
          {
            id: "host",
            label: t("next.networking.test.host"),
            help: destinationRequired ? undefined : t("next.networking.pools.hostOptional"),
            control: { kind: "text", value: host, onChange: setHost, placeholder: "news.example.invalid" },
          },
          {
            id: "port",
            label: t("next.networking.test.port"),
            control: { kind: "number", value: port, min: 1, max: 65535, onChange: setPort },
          },
        ]}
      />
      {error ? (
        <div role="alert" className="px-4 py-4 text-[12.5px] text-wv-error-text sm:px-6">
          {error}
        </div>
      ) : null}
      <div role="status" className="flex flex-col">
        {results.map((result) => (
          <div key={result.proxyId} className="border-b border-wv-hairline px-4 py-3 sm:px-6">
            <StatusMark
              state={result.success ? "UP" : "DOWN"}
              label={data.proxyProfiles.find((profile) => profile.id === result.proxyId)?.name ?? t("next.networking.proxyNumber", { id: result.proxyId })}
              detail={[
                result.message,
                ...(result.sourceAddress ? [t("next.networking.egress.source", { address: result.sourceAddress })] : []),
                ...(result.connectMillis != null ? [`${result.connectMillis} ms`] : []),
              ].join(" · ")}
              wrap
            />
          </div>
        ))}
      </div>
    </Dialog>
  );
}
