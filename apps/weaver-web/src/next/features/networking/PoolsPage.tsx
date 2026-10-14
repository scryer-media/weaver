import { useState, type ReactNode } from "react";
import { CREATE_POOL, DELETE_POOL, UPDATE_POOL } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import type { ProxyPool } from "@/lib/networking";
import { proxyLabels, type ProxyKind } from "@/lib/proxies";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import { RecordEditor } from "../../components/RecordEditor";
import { SecondaryButton } from "../../components/controls";
import { Cell } from "../../components/rows";
import { countLabel } from "../../i18n/labels";
import { SettingsBlocks, type SettingsBlock } from "../../pages/settings/framework";
import type { NetworkingAction, NetworkingData } from "./data";
import { CheckRow, Hint, StatusMark } from "./presentation";
import { TestViaDialog } from "./TestViaDialog";

/** The daemon pools up to sixteen proxies. */
const MAX_POOL_MEMBERS = 16;

const NEW_POOL: ProxyPool = { id: -1, name: "", kind: "WIRE_GUARD", memberIds: [], enabled: true };

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
  const profileName = (id: number) =>
    data.proxyProfiles.find((profile) => profile.id === id)?.name ?? t("next.networking.proxyNumber", { id });

  const open = (pool: ProxyPool | null) => setEditing(pool ?? NEW_POOL);

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "pools",
      title: t("next.networking.pools.title"),
      note: countLabel(t, "next.networking.pools.count", data.proxyPools.length),
      columns: "minmax(0, 1fr) 132px minmax(0, 1.6fr) 104px 112px",
      headers: [
        t("next.networking.pools.name"),
        t("next.networking.pools.type"),
        t("next.networking.pools.members"),
        t("next.networking.pools.state"),
        "",
      ],
      empty: t("next.networking.pools.empty"),
      add: { label: t("next.networking.pools.add"), onClick: () => open(null) },
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
            <span key="edit" className="ml-auto" onClick={(event) => event.stopPropagation()}>
              <SecondaryButton size="compact" onClick={() => open(pool)}>
                {t("next.networking.pools.edit")}
              </SecondaryButton>
            </span>,
          ],
        };
      }),
    },
  ];

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
      {editing ? (
        <PoolEditor
          key={editing.id}
          data={data}
          pool={editing}
          action={action}
          busy={busy}
          onClose={() => setEditing(null)}
        />
      ) : null}
    </>
  );
}

/**
 * One pool in its editor, mounted while it is open. The pools table opens it,
 * and so does a pool picked in the network flow. An id below zero is a pool
 * that has not been saved yet.
 */
export function PoolEditor({
  data,
  pool,
  action,
  busy,
  onClose,
}: {
  data: NetworkingData;
  pool: ProxyPool;
  action: NetworkingAction;
  busy: boolean;
  onClose: () => void;
}) {
  const t = useTranslate();
  const [editing, setEditing] = useState(pool);
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<ProxyPool | null>(null);
  const [testing, setTesting] = useState<ProxyPool | null>(null);
  const patch = (next: Partial<ProxyPool>) => setEditing((current) => ({ ...current, ...next }));

  const save = async () => {
    if (!editing.name.trim()) {
      setError(t("next.networking.pools.nameRequired"));
      return;
    }
    const { id, ...input } = editing;
    const failure = await action(id < 0 ? CREATE_POOL : UPDATE_POOL, { id, input });
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
    const failure = await action(DELETE_POOL, { id: deleting.id });
    setDeleting(null);
    if (!failure) {
      onClose();
    }
  };

  const candidates = data.proxyProfiles.filter((profile) => profile.kind === editing.kind);

  return (
    <>
      <RecordEditor
        open
        title={editing.id >= 0 ? editing.name : t("next.networking.pools.add")}
        note={proxyLabels[editing.kind]}
        error={error}
        busy={busy}
        saveLabel={t("next.networking.pools.save")}
        saveDisabled={editing.memberIds.length > MAX_POOL_MEMBERS}
        onSave={() => void save()}
        onDismiss={onClose}
        onDelete={editing.id >= 0 ? () => setDeleting(editing) : undefined}
        deleteLabel={t("next.networking.pools.delete")}
        extraActions={
          editing.id >= 0 ? (
            <SecondaryButton icon="test" onClick={() => setTesting(editing)}>
              {t("next.networking.pools.test")}
            </SecondaryButton>
          ) : null
        }
        sections={[
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
                          disabled={
                            !editing.memberIds.includes(profile.id) && editing.memberIds.length >= MAX_POOL_MEMBERS
                          }
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
        ]}
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

      {testing ? (
        <TestViaDialog key={testing.id} subject={{ pool: testing.id }} onClose={() => setTesting(null)} />
      ) : null}
    </>
  );
}
