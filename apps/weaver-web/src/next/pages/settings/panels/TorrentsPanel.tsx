// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "urql";
import {
  DELETE_TRACKER_RULE_MUTATION,
  SAVE_TRACKER_RULE_MUTATION,
  TORRENT_SETTINGS_QUERY,
  TRACKER_RULES_QUERY,
  UPDATE_TORRENT_SETTINGS_MUTATION,
} from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor } from "../../../components/RecordEditor";
import { Cell } from "../../../components/rows";
import { EM_DASH } from "../../../data/format";
import { SettingsBlocks, useDraft, usePanelState, type FieldSpec, type SettingsBlock } from "../framework";
import {
  EMPTY_SEEDING,
  GoalControl,
  seedingFields,
  seedingFrom,
  seedingInput,
  seedingIsEmpty,
  type SeedingPolicy,
} from "../seeding";

/**
 * Torrents: the global seeding policy, the tracker rules, and how the
 * torrent session connects.
 *
 * The policy and the connection save through `updateSettings`, each only
 * when it was edited. A seeding policy with no goal and no cap saves as none
 * at all. A tracker rule saves from its own editor.
 */

interface TrackerRule {
  id: number;
  hostPattern: string;
  /** Null while the rule leaves the goals to the category and the global policy. */
  seeding: SeedingPolicy | null;
  neverPause: boolean;
}

interface TrackerRuleForm {
  hostPattern: string;
  seeding: SeedingPolicy | null;
  neverPause: boolean;
}

const NEW_TRACKER_RULE: TrackerRuleForm = { hostPattern: "", seeding: null, neverPause: false };

/** A rule's goals in one line, as its row shows them. */
function goalsText(t: Translate, policy: SeedingPolicy | null): string {
  if (!policy) {
    return EM_DASH;
  }
  const minutes = t("next.seeding.minutes");
  const goals = [
    policy.ratioGoal === null ? null : `${t("next.seeding.ratioGoal")} ${policy.ratioGoal}`,
    policy.seedTimeGoalMinutes === null ? null : `${t("next.seeding.seedTimeGoal")} ${policy.seedTimeGoalMinutes} ${minutes}`,
    policy.inactiveGoalMinutes === null ? null : `${t("next.seeding.inactiveGoal")} ${policy.inactiveGoalMinutes} ${minutes}`,
  ].filter((goal): goal is string => goal !== null);
  return goals.length > 0 ? goals.join(" · ") : EM_DASH;
}

/** The tracker rules' table, and the editor and confirmation it opens. */
function useTrackerRules(t: Translate): { block: SettingsBlock; overlays: ReactNode } {
  const [{ data }, reexecute] = useQuery<{ trackerRules: TrackerRule[] }>({ query: TRACKER_RULES_QUERY });
  const [, saveRule] = useMutation(SAVE_TRACKER_RULE_MUTATION);
  const [, deleteRule] = useMutation(DELETE_TRACKER_RULE_MUTATION);
  const [editingId, setEditingId] = useState<number | "new" | null>(null);
  const [form, setForm] = useState<TrackerRuleForm>(NEW_TRACKER_RULE);
  const [opened, setOpened] = useState<TrackerRuleForm>(NEW_TRACKER_RULE);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState<TrackerRule | null>(null);

  const rules = useMemo(() => data?.trackerRules ?? [], [data?.trackerRules]);
  const editing = typeof editingId === "number" ? rules.find((rule) => rule.id === editingId) : null;

  const open = (rule: TrackerRule | null) => {
    setError(null);
    const start = rule
      ? { hostPattern: rule.hostPattern, seeding: rule.seeding ? seedingFrom(rule.seeding) : null, neverPause: rule.neverPause }
      : NEW_TRACKER_RULE;
    setForm(start);
    setOpened(start);
    setEditingId(rule ? rule.id : "new");
  };

  const save = async () => {
    setBusy(true);
    const result = await saveRule({
      id: editingId === "new" ? null : editingId,
      input: {
        hostPattern: form.hostPattern.trim(),
        seeding: form.seeding ? seedingInput(form.seeding) : null,
        neverPause: form.neverPause,
      },
    });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const remove = async () => {
    if (!confirmDelete) {
      return;
    }
    setBusy(true);
    await deleteRule({ id: confirmDelete.id });
    setBusy(false);
    setConfirmDelete(null);
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const block: SettingsBlock = {
    kind: "table",
    id: "trackerRules",
    title: t("next.torrents.trackerRules"),
    columns: "minmax(0, 1fr) minmax(0, 1.6fr) minmax(0, 0.6fr)",
    headers: [t("next.torrents.trackerRuleHost"), t("next.torrents.trackerRuleGoals"), t("next.torrents.trackerRuleNeverPause")],
    centered: [2],
    empty: t("next.torrents.trackerRulesEmpty"),
    add: { label: t("next.torrents.trackerRuleAdd"), onClick: () => open(null) },
    onRowClick: (id) => {
      const rule = rules.find((entry) => String(entry.id) === id);
      if (rule) {
        open(rule);
      }
    },
    rows: rules.map((rule) => ({
      id: String(rule.id),
      searchText: rule.hostPattern,
      cells: [
        <Cell key="host" mono>
          {rule.hostPattern}
        </Cell>,
        <Cell key="goals" className="text-wv-secondary">
          {goalsText(t, rule.seeding)}
        </Cell>,
        <Cell key="neverPause" mono className="text-wv-muted">
          {t(rule.neverPause ? "next.common.on" : "next.common.off")}
        </Cell>,
      ],
    })),
  };

  const overlays = (
    <>
      <RecordEditor
        open={editingId !== null}
        title={editingId === "new" ? t("next.torrents.trackerRuleAdd") : (editing?.hostPattern ?? t("next.torrents.trackerRule"))}
        note={editingId === "new" ? t("next.torrents.trackerRuleNewNote") : `#${editing?.id ?? ""}`}
        error={error}
        busy={busy}
        onSave={() => void save()}
        onDismiss={() => setEditingId(null)}
        onDelete={editing ? () => setConfirmDelete(editing) : undefined}
        deleteLabel={t("next.rss.removeRule")}
        sections={[
          {
            id: "trackerRule",
            title: t("next.torrents.trackerRule"),
            fields: [
              {
                id: "hostPattern",
                label: t("next.torrents.trackerRuleHost"),
                keywords: "tracker host announce",
                control: {
                  kind: "text",
                  value: form.hostPattern,
                  placeholder: "*.tracker.example",
                  onChange: (hostPattern) => setForm((current) => ({ ...current, hostPattern })),
                },
              },
              {
                id: "neverPause",
                label: t("next.torrents.trackerRuleNeverPause"),
                keywords: "schedule pause seed",
                control: {
                  kind: "toggle",
                  value: form.neverPause,
                  onChange: (neverPause) => setForm((current) => ({ ...current, neverPause })),
                },
              },
              {
                id: "ownSeeding",
                label: t("next.categories.ownSeeding"),
                keywords: "seed seeding policy",
                control: {
                  kind: "toggle",
                  value: form.seeding !== null,
                  onChange: (on) =>
                    setForm((current) => ({ ...current, seeding: on ? (opened.seeding ?? EMPTY_SEEDING) : null })),
                },
              },
              ...(form.seeding
                ? seedingFields(t, form.seeding, (seeding) => setForm((current) => ({ ...current, seeding })), {
                    inherits: true,
                    idPrefix: "trackerRuleSeeding",
                  })
                : ([] as FieldSpec[])),
            ],
          },
        ]}
      />

      <ConfirmDialog
        open={confirmDelete !== null}
        title={t("next.rss.removeRule")}
        note={confirmDelete?.hostPattern}
        busy={busy}
        confirmLabel={t("next.rss.removeRule")}
        body={t("next.torrents.trackerRuleRemoveBody")}
        onConfirm={() => void remove()}
        onDismiss={() => setConfirmDelete(null)}
      />
    </>
  );

  return { block, overlays };
}

type TorrentEncryption = "PREFER" | "REQUIRE";

interface TorrentConnection {
  enabled: boolean;
  basePort: number;
  announcePort: number | null;
  upnp: boolean;
  encryption: TorrentEncryption;
  maxActiveDownloads: number;
  maxPeersTotal: number;
}

interface TorrentForm {
  seeding: SeedingPolicy;
  torrent: TorrentConnection;
}

const DEFAULT_CONNECTION: TorrentConnection = {
  enabled: false,
  basePort: 6881,
  announcePort: null,
  upnp: false,
  encryption: "PREFER",
  maxActiveDownloads: 3,
  maxPeersTotal: 200,
};

/** Option labels are translation keys, resolved when the panel renders. */
const ENCRYPTIONS: { value: TorrentEncryption; label: string }[] = [
  { value: "PREFER", label: "next.torrents.encryptionPrefer" },
  { value: "REQUIRE", label: "next.torrents.encryptionRequire" },
];

const validPort = (port: number | null) => port === null || (Number.isInteger(port) && port >= 1 && port <= 65535);

/** The connection fields that differ from `saved`, as `TorrentSettingsInput` takes them. */
function connectionChanges(form: TorrentConnection, saved: TorrentConnection): Partial<TorrentConnection> {
  return Object.fromEntries(
    (Object.keys(form) as (keyof TorrentConnection)[])
      .filter((key) => form[key] !== saved[key])
      .map((key) => [key, form[key]]),
  ) as Partial<TorrentConnection>;
}

export function TorrentsPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{
    settings: { seeding: SeedingPolicy | null; torrent: TorrentConnection | null };
  }>({ query: TORRENT_SETTINGS_QUERY });
  const [updateState, updateSettings] = useMutation(UPDATE_TORRENT_SETTINGS_MUTATION);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const trackerRules = useTrackerRules(t);

  const source = useMemo<TorrentForm | null>(() => {
    const settings = data?.settings;
    return settings
      ? {
          seeding: seedingFrom(settings.seeding),
          torrent: { ...DEFAULT_CONNECTION, ...(settings.torrent ?? {}) },
        }
      : null;
  }, [data?.settings]);

  const draft = useDraft<TorrentForm>(source, () => {
    setStatus(null);
    setError(null);
  });
  const values = draft.value;

  usePanelState({
    dirty: draft.dirty,
    busy: updateState.fetching,
    status: error ?? status,
    failed: error !== null,
    revert: () => {
      setError(null);
      draft.revert();
    },
    save: () => {
      if (!values || !source) {
        return;
      }
      if (!validPort(values.torrent.basePort) || !validPort(values.torrent.announcePort)) {
        setError(t("next.torrents.portInvalid"));
        return;
      }
      const seeding = seedingInput(values.seeding);
      const seedingChanged = JSON.stringify(seeding) !== JSON.stringify(seedingInput(source.seeding));
      const torrent = connectionChanges(values.torrent, source.torrent);
      setError(null);
      void updateSettings({
        input: {
          ...(seedingChanged ? { seeding: seedingIsEmpty(seeding) ? null : seeding } : {}),
          ...(Object.keys(torrent).length > 0 ? { torrent } : {}),
        },
      }).then((result) => {
        if (result.error || !result.data?.updateSettings) {
          setError(result.error?.message ?? t("next.torrents.saveFailed"));
          return;
        }
        draft.markSaved();
        setStatus(t("next.settings.saved"));
        void reexecute({ requestPolicy: "network-only" });
      });
    },
  });

  const patchTorrent = (next: Partial<TorrentConnection>) =>
    values && draft.set({ torrent: { ...values.torrent, ...next } });

  const blocks: SettingsBlock[] = values
    ? [
        {
          kind: "section",
          id: "seeding",
          title: t("next.torrents.seeding"),
          fields: seedingFields(t, values.seeding, (seeding) => draft.set({ seeding }), { inherits: false }),
        },
        trackerRules.block,
        {
          kind: "section",
          id: "connection",
          title: t("next.torrents.connection"),
          fields: [
            {
              id: "enabled",
              label: t("next.torrents.enabled"),
              control: { kind: "toggle", value: values.torrent.enabled, onChange: (enabled) => patchTorrent({ enabled }) },
            },
            {
              id: "basePort",
              label: t("next.torrents.basePort"),
              keywords: "port listen",
              control: {
                kind: "number",
                value: values.torrent.basePort,
                min: 1,
                max: 65535,
                onChange: (basePort) => patchTorrent({ basePort: Math.round(basePort) }),
              },
            },
            {
              id: "announcePort",
              label: t("next.torrents.announcePort"),
              keywords: "port announce tracker",
              control: {
                kind: "custom",
                control: (
                  <GoalControl
                    label={t("next.torrents.announcePort")}
                    value={values.torrent.announcePort}
                    fallback={values.torrent.basePort}
                    onChange={(announcePort) =>
                      patchTorrent({ announcePort: announcePort === null ? null : Math.round(announcePort) })
                    }
                  />
                ),
              },
            },
            {
              id: "upnp",
              label: t("next.torrents.upnp"),
              keywords: "upnp port mapping",
              control: { kind: "toggle", value: values.torrent.upnp, onChange: (upnp) => patchTorrent({ upnp }) },
            },
            {
              id: "encryption",
              label: t("next.torrents.encryption"),
              control: {
                kind: "segmented",
                value: values.torrent.encryption,
                options: ENCRYPTIONS.map((option) => ({ ...option, label: t(option.label) })),
                onChange: (encryption) => patchTorrent({ encryption: encryption as TorrentEncryption }),
              },
            },
            {
              id: "maxActiveDownloads",
              label: t("next.torrents.maxActiveDownloads"),
              control: {
                kind: "number",
                value: values.torrent.maxActiveDownloads,
                min: 0,
                onChange: (next) => patchTorrent({ maxActiveDownloads: Math.max(0, Math.round(next)) }),
              },
            },
            {
              id: "maxPeersTotal",
              label: t("next.torrents.maxPeers"),
              keywords: "peers connections",
              control: {
                kind: "number",
                value: values.torrent.maxPeersTotal,
                min: 0,
                onChange: (next) => patchTorrent({ maxPeersTotal: Math.max(0, Math.round(next)) }),
              },
            },
          ],
        },
      ]
    : [];

  return (
    <>
      <SettingsBlocks blocks={blocks} loading={fetching && !data} />
      {trackerRules.overlays}
    </>
  );
}
