// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import { TORRENT_SETTINGS_QUERY, UPDATE_TORRENT_SETTINGS_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { SettingsBlocks, useDraft, usePanelState, type SettingsBlock } from "../framework";
import {
  GoalControl,
  seedingFields,
  seedingFrom,
  seedingInput,
  seedingIsEmpty,
  type SeedingPolicy,
} from "../seeding";

/**
 * Torrents: the global seeding policy, and how the torrent session connects.
 *
 * Both save through `updateSettings`, each only when it was edited. A
 * seeding policy with no goal and no cap saves as none at all.
 */

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

  return <SettingsBlocks blocks={blocks} loading={fetching && !data} />;
}
