// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect, useState } from "react";
import { useMutation, useQuery, useSubscription } from "urql";
import {
  ADD_TORRENT_TRACKERS_MUTATION,
  BAN_TORRENT_PEER_MUTATION,
  SET_TORRENT_DOWNLOAD_LIMIT_MUTATION,
  REANNOUNCE_TORRENT_MUTATION,
  REMOVE_TORRENT_TRACKERS_MUTATION,
  REPLACE_TORRENT_TRACKERS_MUTATION,
  SET_TORRENT_FILE_PRIORITIES_MUTATION,
  SET_TORRENT_FIRST_LAST_PIECE_FIRST_MUTATION,
  SET_TORRENT_SEEDING_MUTATION,
  SET_TORRENT_SEQUENTIAL_MUTATION,
  SET_TORRENT_UPLOAD_LIMIT_MUTATION,
  TORRENT_PEERS_QUERY,
  TORRENT_UPDATES_SUBSCRIPTION,
} from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import {
  torrentErrorText,
  trackerSourceKey,
  type TorrentActionError,
  type TorrentData,
  type TorrentFilePriority,
  type TorrentPeerData,
  type TorrentTrackerData,
} from "@/lib/torrents";
import { DetailBlock, Field } from "@/next/components/chrome";
import { BulkCluster, BulkMenu } from "@/next/components/BulkBar";
import { CheckBox, SecondaryButton, Select, Toggle } from "@/next/components/controls";
import { RecordEditor } from "@/next/components/RecordEditor";
import { GridRow } from "@/next/components/rows";
import { EM_DASH, formatRate, formatSize } from "@/next/data/format";
import { useNextData } from "@/next/data/next-data";
import { LimitControl } from "@/next/features/networking/limits";
import { informationFields, transferFields, type TorrentField } from "@/next/features/torrents/general";
import { countLabel } from "@/next/i18n/labels";
import { EMPTY_SEEDING, durationText, seedingFields, seedingInput, type SeedingPolicy } from "@/next/pages/settings/seeding";

const TRACKER_COLUMNS = {
  base: "minmax(0,1fr) 80px 112px max-content",
  sm: "minmax(0,1fr) 80px 112px 96px minmax(0,1fr) max-content",
};
const FILE_COLUMNS = {
  base: "14px minmax(0,1fr) 72px 120px",
  sm: "14px minmax(0,1fr) 90px 72px 120px",
};
const LIMIT_COLUMNS = { base: "minmax(0,1fr) max-content" };
const PEER_COLUMNS = {
  base: "minmax(0,1fr) 56px max-content",
  sm: "minmax(0,1fr) minmax(0,1fr) 56px 90px 90px max-content",
};

/**
 * A torrent job's torrent, followed while the page shows it. Following it
 * again reads it at once, as a change that pushes nothing still shows.
 */
export function useTorrent(id: number, active: boolean) {
  const { connection } = useNextData("connection");
  const [{ data }, follow] = useSubscription<{ torrentUpdates: TorrentData[] }>({
    query: TORRENT_UPDATES_SUBSCRIPTION,
    variables: { id },
    pause: !active || connection.isDisconnected,
  });
  return {
    torrent: active ? (data?.torrentUpdates[0] ?? null) : null,
    refresh: () => follow(),
  };
}

const ACTION_KEYS = {
  STOP: "next.seeding.actionStop",
  REMOVE: "next.seeding.actionRemove",
  REMOVE_WITH_DATA: "next.seeding.actionRemoveWithData",
} as const;

function seedingRows(t: Translate, policy: SeedingPolicy): { label: string; value: string }[] {
  const minutes = (value: number | null) => (value === null ? t("next.seeding.noGoal") : durationText(t, value));
  return [
    {
      label: t("next.seeding.ratioGoal"),
      value: policy.ratioGoal === null ? t("next.seeding.noGoal") : policy.ratioGoal.toFixed(2),
    },
    { label: t("next.seeding.seedTimeGoal"), value: minutes(policy.seedTimeGoalMinutes) },
    { label: t("next.seeding.inactiveGoal"), value: minutes(policy.inactiveGoalMinutes) },
    { label: t("next.seeding.mode"), value: t(policy.mode === "ALL" ? "next.seeding.modeAll" : "next.seeding.modeAny") },
    { label: t("next.seeding.action"), value: t(ACTION_KEYS[policy.action]) },
    {
      label: t("next.seeding.uploadCap"),
      value: policy.maxUploadBps === null ? EM_DASH : formatRate(policy.maxUploadBps),
    },
  ];
}

/** Where the policy in effect comes from: the torrent's own override, its tracker rule, its category, or the global one. */
function seedingSourceText(t: Translate, torrent: TorrentData): string {
  switch (torrent.seedingSource) {
    case "TORRENT":
      return t("next.torrent.seedingFromTorrent");
    case "TRACKER_RULE":
      return t("next.torrent.seedingFromTrackerRule", { name: torrent.seedingRule ?? "" });
    case "CATEGORY":
      return t("next.torrent.seedingFromCategory", { name: torrent.category ?? "" });
    case "GLOBAL":
      return t("next.torrent.seedingFromGlobal");
  }
}

/** A general section's rows in qBittorrent's three columns; a wide row spans them. */
function GeneralGrid({ fields }: { fields: TorrentField[] }) {
  return (
    <div className="grid grid-cols-2 gap-x-4 gap-y-[13px] sm:grid-cols-3">
      {fields.map((field) => (
        <div key={field.label} className={field.wide ? "col-span-full min-w-0" : "min-w-0"}>
          <Field label={field.label} value={field.value} title={field.value} />
        </div>
      ))}
    </div>
  );
}

/**
 * A torrent's peer connections, read again each time the torrent pushes an
 * update, each with its ban.
 */
function PeersBlock({ torrent, onReport }: { torrent: TorrentData; onReport: (message: string) => void }) {
  const t = useTranslate();
  const [{ data }, reread] = useQuery<{ torrentPeers: TorrentPeerData[] }>({
    query: TORRENT_PEERS_QUERY,
    variables: { id: torrent.id },
    requestPolicy: "network-only",
  });
  const [banState, banPeer] = useMutation(BAN_TORRENT_PEER_MUTATION);
  useEffect(() => {
    reread({ requestPolicy: "network-only" });
  }, [torrent, reread]);
  const peers = data?.torrentPeers ?? [];

  const ban = (address: string) => {
    void banPeer({ id: torrent.id, address }).then((result) => {
      if (result.error) {
        onReport(torrentErrorText(result.error));
        return;
      }
      reread({ requestPolicy: "network-only" });
    });
  };

  return (
    <DetailBlock id="peers" title={t("next.torrent.peers")} note={countLabel(t, "next.torrent.peerCount", peers.length)}>
      {peers.length === 0 ? (
        <span className="text-[12.5px] text-wv-muted">{EM_DASH}</span>
      ) : (
        peers.map((peer) => (
          <GridRow
            key={peer.address}
            columns={PEER_COLUMNS}
            gap={14}
            className="h-[38px] border-t border-wv-hairline"
            title={peer.address}
          >
            <span className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">{peer.address}</span>
            <span className="hidden min-w-0 truncate text-[12px] text-wv-tertiary sm:block" title={peer.client}>
              {peer.client}
            </span>
            <span className="text-right font-wv-mono text-[12px] text-wv-muted">{`${(peer.progress * 100).toFixed(1)}%`}</span>
            <span className="hidden text-right font-wv-mono text-[12px] text-wv-muted sm:block">
              {`↓ ${formatRate(peer.downloadBps)}`}
            </span>
            <span className="hidden text-right font-wv-mono text-[12px] text-wv-muted sm:block">
              {`↑ ${formatRate(peer.uploadBps)}`}
            </span>
            <button
              type="button"
              disabled={banState.fetching}
              onClick={() => ban(peer.address)}
              className="cursor-pointer text-right text-[12.5px] whitespace-nowrap text-wv-dim hover:text-wv-error disabled:cursor-default"
            >
              {t("next.torrent.ban")}
            </button>
          </GridRow>
        ))
      )}
    </DetailBlock>
  );
}

/**
 * The torrent's own download and upload limits, each set as it changes; off
 * is unlimited.
 */
function LimitsBlock({ torrent, onReport }: { torrent: TorrentData; onReport: (message: string) => void }) {
  const t = useTranslate();
  const [, setUpload] = useMutation(SET_TORRENT_UPLOAD_LIMIT_MUTATION);
  const [, setDownload] = useMutation(SET_TORRENT_DOWNLOAD_LIMIT_MUTATION);
  // Each control sends its own limit alone; the server keeps the other.
  const report = (result: { error?: TorrentActionError }) => {
    if (result.error) {
      onReport(torrentErrorText(result.error));
    }
  };
  const rows = [
    {
      label: t("next.torrent.downloadLimit"),
      value: torrent.downloadLimit,
      set: (download: number | null) => void setDownload({ id: torrent.id, download }).then(report),
    },
    {
      label: t("next.torrent.uploadLimit"),
      value: torrent.uploadLimit,
      set: (upload: number | null) => void setUpload({ id: torrent.id, upload }).then(report),
    },
  ];
  return (
    <DetailBlock id="limits" title={t("next.torrent.limits")}>
      {rows.map((row) => (
        <GridRow key={row.label} columns={LIMIT_COLUMNS} gap={14} className="min-h-[46px] border-t border-wv-hairline">
          <span className="min-w-0 truncate text-[12.5px] text-wv-fg">{row.label}</span>
          <LimitControl label={row.label} value={row.value} onChange={row.set} />
        </GridRow>
      ))}
    </DetailBlock>
  );
}

/** The tracker being added to or edited, as its dialog holds it. */
type TrackerEdit = { kind: "add"; urls: string } | { kind: "edit"; from: TorrentTrackerData; url: string };

const count = (value: number | null) => (value === null ? EM_DASH : String(value));

/**
 * A torrent's trackers, each with how announcing to it went, and the
 * actions on them: add, edit, remove and reannounce.
 */
function TrackersBlock({
  torrent,
  onChanged,
  onReport,
}: {
  torrent: TorrentData;
  onChanged: () => void;
  onReport: (message: string) => void;
}) {
  const t = useTranslate();
  const [addState, addTrackers] = useMutation(ADD_TORRENT_TRACKERS_MUTATION);
  const [replaceState, replaceTrackers] = useMutation(REPLACE_TORRENT_TRACKERS_MUTATION);
  const [removeState, removeTrackers] = useMutation(REMOVE_TORRENT_TRACKERS_MUTATION);
  const [reannounceState, reannounce] = useMutation(REANNOUNCE_TORRENT_MUTATION);
  const [editing, setEditing] = useState<TrackerEdit | null>(null);
  const [error, setError] = useState<string | null>(null);
  const busy = addState.fetching || replaceState.fetching || removeState.fetching || reannounceState.fetching;

  const settle = (result: { error?: TorrentActionError }, report: (message: string) => void) => {
    if (result.error) {
      report(torrentErrorText(result.error));
      return false;
    }
    onChanged();
    return true;
  };

  const open = (edit: TrackerEdit) => {
    setError(null);
    setEditing(edit);
  };

  const save = () => {
    if (editing === null) return;
    const done = (result: { error?: TorrentActionError }) => {
      if (settle(result, setError)) setEditing(null);
    };
    if (editing.kind === "add") {
      const urls = editing.urls
        .split("\n")
        .map((line) => line.trim())
        .filter((line) => line !== "");
      void addTrackers({ id: torrent.id, urls }).then(done);
      return;
    }
    const { from, url } = editing;
    const trackers = torrent.trackers.map((tracker) => ({
      tier: tracker.tier,
      url: tracker.url === from.url ? url.trim() : tracker.url,
    }));
    void replaceTrackers({ id: torrent.id, trackers }).then(done);
  };

  const remove = (url: string) => {
    void removeTrackers({ id: torrent.id, urls: [url] }).then((result) => settle(result, onReport));
  };

  const announce = (tracker: string | null) => {
    void reannounce({ id: torrent.id, tracker }).then((result) => settle(result, onReport));
  };

  const action = "cursor-pointer text-[12.5px] whitespace-nowrap text-wv-dim hover:text-wv-fg disabled:cursor-default";

  return (
    <DetailBlock
      id="trackers"
      title={t("next.torrent.trackers")}
      note={countLabel(t, "next.torrent.trackerCount", torrent.trackers.length)}
      right={
        <span className="flex gap-3">
          <button type="button" disabled={busy} onClick={() => announce(null)} className={action}>
            {t("next.torrent.reannounce")}
          </button>
          <button type="button" onClick={() => open({ kind: "add", urls: "" })} className={action}>
            {t("next.torrent.addTrackers")}
          </button>
        </span>
      }
    >
      {torrent.trackers.length === 0 ? (
        <span className="text-[12.5px] text-wv-muted">{EM_DASH}</span>
      ) : (
        torrent.trackers.map((tracker) => (
          <GridRow
            key={`${tracker.tier}-${tracker.url}`}
            columns={TRACKER_COLUMNS}
            gap={14}
            className="h-[38px] border-t border-wv-hairline"
            title={tracker.url}
          >
            <span className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">{tracker.url}</span>
            <span className="truncate font-wv-mono text-[11.5px] text-wv-muted">{t(trackerSourceKey(tracker.source))}</span>
            <span className="font-wv-mono text-[11.5px] text-wv-muted">
              {t(tracker.updating ? "next.torrent.tracker.UPDATING" : `next.torrent.tracker.${tracker.status}`)}
            </span>
            <span
              className="hidden text-right font-wv-mono text-[12px] text-wv-muted sm:block"
              title={t("next.torrent.trackerSwarmTitle")}
            >
              {t("next.torrent.trackerSwarm", {
                seeds: count(tracker.seeders),
                leeches: count(tracker.leechers),
                peers: count(tracker.peers),
              })}
            </span>
            <span className="hidden min-w-0 truncate text-[12px] text-wv-tertiary sm:block" title={tracker.message ?? undefined}>
              {tracker.message ?? ""}
            </span>
            <span className="flex justify-end gap-3">
              <button type="button" disabled={busy} onClick={() => announce(tracker.url)} className={action}>
                {t("next.torrent.reannounce")}
              </button>
              <button
                type="button"
                onClick={() => open({ kind: "edit", from: tracker, url: tracker.url })}
                className={action}
              >
                {t("next.common.edit")}
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => remove(tracker.url)}
                className="cursor-pointer text-[12.5px] whitespace-nowrap text-wv-dim hover:text-wv-error disabled:cursor-default"
              >
                {t("next.common.remove")}
              </button>
            </span>
          </GridRow>
        ))
      )}
      <RecordEditor
        open={editing !== null}
        title={t(editing?.kind === "edit" ? "next.torrent.editTracker" : "next.torrent.addTrackers")}
        note={torrent.name}
        error={error}
        busy={busy}
        onSave={save}
        onDismiss={() => setEditing(null)}
        sections={
          editing === null
            ? []
            : [
                {
                  id: "trackers",
                  title: t("next.torrent.trackers"),
                  fields: [
                    editing.kind === "add"
                      ? {
                          id: "tracker-urls",
                          label: t("next.torrent.trackerUrls"),
                          control: {
                            kind: "textarea",
                            rows: 6,
                            value: editing.urls,
                            onChange: (urls: string) => setEditing({ ...editing, urls }),
                          },
                        }
                      : {
                          id: "tracker-url",
                          label: t("next.torrent.trackerUrl"),
                          control: {
                            kind: "text",
                            type: "url",
                            mono: true,
                            value: editing.url,
                            onChange: (url: string) => setEditing({ ...editing, url }),
                          },
                        },
                  ],
                },
              ]
        }
      />
    </DetailBlock>
  );
}

/** A file's four levels, by the daemon's own names. */
const PRIORITY_KEYS: Record<TorrentFilePriority, string> = {
  IGNORED: "next.torrent.priority.IGNORED",
  NORMAL: "next.torrent.priority.NORMAL",
  HIGH: "next.torrent.priority.HIGH",
  MAXIMUM: "next.torrent.priority.MAXIMUM",
};

const isPriority = (value: string): value is TorrentFilePriority => Object.hasOwn(PRIORITY_KEYS, value);

/**
 * How the torrent's pieces are picked, and its files, each with its level.
 * A level set on one row is sent for that file alone; a level set over the
 * ticked rows is one request naming every one of them. What is shown is what
 * the torrent pushes back, never the level just sent.
 */
function FilesBlock({
  torrent,
  onChanged,
  onReport,
}: {
  torrent: TorrentData;
  onChanged: () => void;
  onReport: (message: string) => void;
}) {
  const t = useTranslate();
  const [prioritiesState, setPriorities] = useMutation(SET_TORRENT_FILE_PRIORITIES_MUTATION);
  const [sequentialState, setSequential] = useMutation(SET_TORRENT_SEQUENTIAL_MUTATION);
  const [firstLastState, setFirstLast] = useMutation(SET_TORRENT_FIRST_LAST_PIECE_FIRST_MUTATION);
  const [picked, setPicked] = useState<ReadonlySet<number>>(() => new Set());
  // Only files the torrent still lists count as ticked.
  const pickedFiles = torrent.files.filter((file) => picked.has(file.index));
  const allPicked = torrent.files.length > 0 && pickedFiles.length === torrent.files.length;

  const settle = (result: { error?: TorrentActionError }) => {
    if (result.error) {
      onReport(torrentErrorText(result.error));
      return false;
    }
    onChanged();
    return true;
  };

  const prioritize = (indexes: readonly number[], priority: TorrentFilePriority) =>
    setPriorities({ id: torrent.id, priorities: indexes.map((index) => ({ index, priority })) }).then(settle);

  const prioritizePicked = (value: string) => {
    if (!isPriority(value)) return;
    void prioritize(
      pickedFiles.map((file) => file.index),
      value,
    ).then((done) => {
      if (done) setPicked(new Set());
    });
  };

  const pick = (index: number) =>
    setPicked((current) => {
      const next = new Set(current);
      if (next.has(index)) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return next;
    });

  const levels = (Object.keys(PRIORITY_KEYS) as TorrentFilePriority[]).map((value) => ({
    value,
    label: t(PRIORITY_KEYS[value]),
  }));

  const toggles = [
    {
      label: t("next.torrent.sequential"),
      checked: torrent.sequential,
      busy: sequentialState.fetching,
      set: (sequential: boolean) => void setSequential({ id: torrent.id, sequential }).then(settle),
    },
    {
      label: t("next.torrent.firstLastPieceFirst"),
      checked: torrent.firstLastPieceFirst,
      busy: firstLastState.fetching,
      set: (firstLastPieceFirst: boolean) =>
        void setFirstLast({ id: torrent.id, firstLastPieceFirst }).then(settle),
    },
  ];

  const heading = "truncate font-wv-mono text-[10.5px] uppercase tracking-[0.12em] text-wv-note";

  return (
    <DetailBlock
      id="torrent-files"
      title={t("next.common.files")}
      note={countLabel(t, "next.job.fileCount", torrent.files.length)}
    >
      {toggles.map((toggle) => (
        <GridRow key={toggle.label} columns={LIMIT_COLUMNS} gap={14} className="min-h-[46px] border-t border-wv-hairline">
          <span className="min-w-0 truncate text-[12.5px] text-wv-fg">{toggle.label}</span>
          <Toggle label={toggle.label} checked={toggle.checked} disabled={toggle.busy} onChange={toggle.set} />
        </GridRow>
      ))}
      {torrent.files.length === 0 ? (
        <span className="border-t border-wv-hairline pt-3 text-[12.5px] text-wv-muted">{EM_DASH}</span>
      ) : (
        <>
          <GridRow columns={FILE_COLUMNS} gap={14} className="h-[38px] border-t border-wv-hairline">
            <CheckBox
              label={t("next.torrent.selectFiles")}
              checked={allPicked}
              onChange={() => setPicked(allPicked ? new Set() : new Set(torrent.files.map((file) => file.index)))}
            />
            {pickedFiles.length === 0 ? (
              <span className={heading}>{t("table.name")}</span>
            ) : (
              <BulkCluster count={pickedFiles.length} onClear={() => setPicked(new Set())}>
                <BulkMenu
                  label={t("next.downloads.setPriority")}
                  options={levels}
                  onSelect={prioritizePicked}
                  disabled={prioritiesState.fetching}
                >
                  {t("next.downloads.setPriority")}
                </BulkMenu>
              </BulkCluster>
            )}
            <span className={`hidden text-right sm:block ${heading}`}>{t("table.size")}</span>
            <span className={`text-right ${heading}`}>{t("table.progress")}</span>
            <span className={heading}>{t("table.priority")}</span>
          </GridRow>
          {torrent.files.map((file) => (
            <GridRow
              key={file.index}
              columns={FILE_COLUMNS}
              gap={14}
              selected={picked.has(file.index)}
              className="h-[38px] border-t border-wv-hairline"
              title={file.path}
            >
              <CheckBox
                label={t("next.common.selectItem", { name: file.path })}
                checked={picked.has(file.index)}
                onChange={() => pick(file.index)}
              />
              <span className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">{file.path}</span>
              <span className="hidden text-right font-wv-mono text-[12px] text-wv-muted sm:block">
                {formatSize(file.length)}
              </span>
              <span className="text-right font-wv-mono text-[12px] text-wv-muted">{`${(file.progress * 100).toFixed(1)}%`}</span>
              <Select
                label={t("next.torrent.filePriority", { name: file.path })}
                value={file.priority}
                options={levels}
                disabled={prioritiesState.fetching}
                onChange={(priority) => void prioritize([file.index], priority)}
                className="h-[28px] w-full min-w-0 text-[12.5px]"
              />
            </GridRow>
          ))}
        </>
      )}
    </DetailBlock>
  );
}

/** A torrent job's own blocks on the job's screen: its transfer, its limits, its information, how it seeds, its trackers, its peers and its files. */
export function TorrentDetail({
  torrent,
  onChanged,
  onReport,
}: {
  torrent: TorrentData;
  onChanged: () => void;
  onReport: (message: string) => void;
}) {
  const t = useTranslate();
  const [seedingState, setTorrentSeeding] = useMutation(SET_TORRENT_SEEDING_MUTATION);
  const [editing, setEditing] = useState<SeedingPolicy | null>(null);

  const saveSeeding = (seeding: SeedingPolicy | null) => {
    void setTorrentSeeding({ id: torrent.id, seeding: seeding === null ? null : seedingInput(seeding) }).then(
      (result) => {
        if (result.error) {
          onReport(torrentErrorText(result.error));
          return;
        }
        setEditing(null);
        onChanged();
      },
    );
  };

  return (
    <>
      <DetailBlock id="transfer" title={t("next.torrent.transfer")} note={torrent.detail ?? undefined}>
        <GeneralGrid fields={transferFields(t, torrent)} />
      </DetailBlock>

      <LimitsBlock torrent={torrent} onReport={onReport} />

      <DetailBlock id="information" title={t("next.torrent.information")}>
        <GeneralGrid fields={informationFields(t, torrent)} />
      </DetailBlock>

      <DetailBlock
        id="seeding"
        title={t("next.torrents.seeding")}
        note={torrent.seedingLeft ?? undefined}
        right={
          <button
            type="button"
            onClick={() => setEditing(torrent.seedingOverride ?? torrent.seedingPolicy)}
            className="cursor-pointer text-wv-dim hover:text-wv-fg"
          >
            {t("next.torrent.overrideSeeding")}
          </button>
        }
      >
        <div className="grid gap-x-4 gap-y-[13px]" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))" }}>
          <Field
            label={t("next.torrent.seedingFrom")}
            value={seedingSourceText(t, torrent)}
          />
          {seedingRows(t, torrent.seedingPolicy).map((row) => (
            <Field key={row.label} label={row.label} value={row.value} />
          ))}
        </div>
      </DetailBlock>

      <TrackersBlock torrent={torrent} onChanged={onChanged} onReport={onReport} />

      <PeersBlock torrent={torrent} onReport={onReport} />

      <FilesBlock torrent={torrent} onChanged={onChanged} onReport={onReport} />

      <RecordEditor
        open={editing !== null}
        title={t("next.torrent.overrideSeeding")}
        note={torrent.name}
        busy={seedingState.fetching}
        onSave={() => saveSeeding(editing ?? EMPTY_SEEDING)}
        onDismiss={() => setEditing(null)}
        extraActions={
          <SecondaryButton
            disabled={seedingState.fetching || torrent.seedingOverride === null}
            onClick={() => saveSeeding(null)}
          >
            {t("next.torrent.useInherited")}
          </SecondaryButton>
        }
        sections={
          editing === null
            ? []
            : [
                {
                  id: "seeding",
                  title: t("next.torrents.seeding"),
                  fields: seedingFields(t, editing, setEditing, { inherits: true, idPrefix: "torrent-seeding" }),
                },
              ]
        }
      />
    </>
  );
}
