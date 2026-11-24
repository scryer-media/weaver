// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useMutation, useQuery, useSubscription } from "urql";
import {
  ADD_TORRENT_TRACKERS_MUTATION,
  BAN_TORRENT_PEER_MUTATION,
  DHT_ANNOUNCE_TORRENT_MUTATION,
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
  TORRENT_UNPACKED_FILES_QUERY,
  TORRENT_UPDATES_SUBSCRIPTION,
} from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import {
  torrentErrorText,
  resolvedByKey,
  trackerSourceKey,
  type SettingLevel,
  type TorrentActionError,
  type TorrentChangesData,
  type TorrentData,
  type TorrentFilePriority,
  type TorrentPeerData,
  type TorrentTrackerData,
  type TorrentUnpackedFileData,
  type TorrentUnpackResult,
} from "@/lib/torrents";
import { DetailBlock, Field } from "@/next/components/chrome";
import { BulkCluster, BulkMenu } from "@/next/components/BulkBar";
import { CheckBox, SecondaryButton, Select, Toggle } from "@/next/components/controls";
import { RecordEditor } from "@/next/components/RecordEditor";
import { GridRow } from "@/next/components/rows";
import { cn } from "@/lib/utils";
import { EM_DASH, formatCount, formatRate, formatSize } from "@/next/data/format";
import { useNextData } from "@/next/data/next-data";
import { LimitControl } from "@/next/features/networking/limits";
import { NoticeBand } from "@/next/features/networking/presentation";
import { informationFields, transferFields, transferNote, type TorrentField } from "@/next/features/torrents/general";
import {
  PEER_PAGE_SIZE,
  nextPeerSort,
  peerPage,
  peerPageCount,
  peerSortFor,
  peersOfPage,
  sortPeers,
  type PeerSort,
  type PeerSortKey,
} from "@/next/features/torrents/peers";
import { countLabel } from "@/next/i18n/labels";
import { testIds, torrentIds } from "@/next/testids";
import { durationText, seedingFields, seedingFrom, seedingInherits, seedingInput, type SeedingPolicy } from "@/next/pages/settings/seeding";

const TRACKER_COLUMNS = {
  // The actions column is max-content in every row; the heading row carries
  // an invisible copy of the actions so it is as wide there.
  base: "minmax(0,1fr) 88px max-content",
  sm: "minmax(0,1.2fr) 150px 88px 56px 64px 56px minmax(0,1fr) max-content",
};
const FILE_COLUMNS = {
  base: "14px minmax(0,1fr) 72px 120px",
  sm: "14px minmax(0,1fr) 90px 72px 120px",
};
const LIMIT_COLUMNS = { base: "minmax(0,1fr) max-content" };
const NO_PEERS: TorrentPeerData[] = [];
const PEER_COLUMNS = {
  // Each row is its own grid, so every column but the shared 1fr ones has a
  // fixed width: a max-content one sized by the Ban button would sit wider in
  // the rows than in the headings and move every column after it.
  base: "minmax(0,1fr) 72px 44px",
  sm: "minmax(0,1fr) minmax(0,1fr) 84px 72px 90px 90px 44px",
};

/**
 * A torrent job's torrent, followed while the page shows it. Following it
 * again reads it at once, as a change that pushes nothing still shows.
 */
export function useTorrent(id: number, active: boolean) {
  const { connection } = useNextData("connection");
  // A push carries the torrent only when it moved, and its id once gone.
  const [{ data }, follow] = useSubscription<{ torrentUpdates: TorrentChangesData<TorrentData> }, TorrentData | null>(
    {
      query: TORRENT_UPDATES_SUBSCRIPTION,
      variables: { id },
      pause: !active || connection.isDisconnected,
    },
    (held, pushed) => {
      const { fullUpdate, torrents, removedIds } = pushed.torrentUpdates;
      if (removedIds.includes(id)) return null;
      return torrents.find((torrent) => torrent.id === id) ?? (fullUpdate ? null : (held ?? null));
    },
  );
  return {
    torrent: active ? (data ?? null) : null,
    refresh: () => follow(),
  };
}

const ACTION_KEYS = {
  STOP: "next.seeding.actionStop",
  REMOVE: "next.seeding.actionRemove",
  REMOVE_WITH_DATA: "next.seeding.actionRemoveWithData",
} as const;

/** A torrent's seeding terms in force, each with the level it comes from. */
function seedingRows(
  t: Translate,
  torrent: TorrentData,
): { term: string; label: string; value: string; level: SettingLevel }[] {
  const terms = torrent.seedingPolicy;
  const minutes = (value: number | null) => (value === null ? t("next.seeding.noGoal") : durationText(t, value));
  return [
    {
      term: "ratioGoal",
      label: t("next.seeding.ratioGoal"),
      value: terms.ratioGoal === null ? t("next.seeding.noGoal") : terms.ratioGoal.toFixed(2),
      level: terms.levels.ratioGoal,
    },
    {
      term: "seedTimeGoalMinutes",
      label: t("next.seeding.seedTimeGoal"),
      value: minutes(terms.seedTimeGoalMinutes),
      level: terms.levels.seedTimeGoalMinutes,
    },
    {
      term: "inactiveGoalMinutes",
      label: t("next.seeding.inactiveGoal"),
      value: minutes(terms.inactiveGoalMinutes),
      level: terms.levels.inactiveGoalMinutes,
    },
    {
      term: "mode",
      label: t("next.seeding.mode"),
      value: t(terms.mode === "ALL" ? "next.seeding.modeAll" : "next.seeding.modeAny"),
      level: terms.levels.mode,
    },
    { term: "action", label: t("next.seeding.action"), value: t(ACTION_KEYS[terms.action]), level: terms.levels.action },
    {
      term: "seedTimeCounting",
      label: t("next.seeding.seedTimeCounting"),
      value: t(
        terms.seedTimeCounting === "CONSECUTIVE"
          ? "next.seeding.countingConsecutive"
          : "next.seeding.countingCumulative",
      ),
      level: terms.levels.seedTimeCounting,
    },
    {
      term: "uploadCap",
      label: t("next.seeding.uploadCap"),
      value: terms.uploadCap === null ? EM_DASH : formatRate(terms.uploadCap),
      level: terms.levels.uploadCap,
    },
  ];
}

/** Where a term in force comes from: the torrent itself, its tracker rule, its category, or the global settings. */
function levelText(t: Translate, torrent: TorrentData, level: SettingLevel): string {
  switch (level) {
    case "TORRENT":
      return t("next.torrent.seedingFromTorrent");
    case "TRACKER_RULE":
      return t("next.torrent.seedingFromTrackerRule", { name: torrent.trackerRule ?? "" });
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
        <div
          key={field.key}
          data-testid={torrentIds.field(field.key)}
          className={field.wide ? "col-span-full min-w-0" : "min-w-0"}
        >
          <Field label={field.label} value={field.value} title={field.value} />
        </div>
      ))}
    </div>
  );
}

/**
 * A torrent's peer connections, read again each time the torrent pushes an
 * update, each with its ban. They are read whole and shown a page of 25 at a
 * time: by download speed while the torrent downloads and by upload speed
 * while it uploads, until a heading is clicked, then by that column.
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
  const peers = data?.torrentPeers ?? NO_PEERS;
  // Null until a heading is clicked: the order then follows the torrent, so
  // a download that finishes turns to its uploads by itself.
  const [chosen, setChosen] = useState<PeerSort | null>(null);
  const sort = chosen ?? peerSortFor(torrent);
  const [asked, setAsked] = useState(0);
  const sorted = useMemo(() => sortPeers(peers, sort), [peers, sort]);
  // Peers leaving can empty the page asked for: the last one there is shows.
  const page = peerPage(asked, sorted.length);
  const pageCount = peerPageCount(sorted.length);
  const shown = peersOfPage(sorted, page);
  const sortBy = (key: PeerSortKey) => {
    setChosen(nextPeerSort(sort, key));
    setAsked(0);
  };
  const heading = (key: PeerSortKey, label: string, className?: string) => (
    <PeerHeading sortKey={key} label={label} sort={sort} onSort={sortBy} className={className} />
  );

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
        <>
          <GridRow columns={PEER_COLUMNS} gap={14} className="h-[28px]" testId={torrentIds.peerHeadings}>
            {heading("address", t("next.dns.address"))}
            {heading("client", t("next.torrent.peerClient"), "hidden sm:flex")}
            {heading("connection", t("next.torrent.peerConnection"), "hidden justify-center sm:flex")}
            {heading("progress", t("next.networking.speed.progress"), "justify-center")}
            {heading("downloadBps", t("next.quota.download"), "hidden justify-center sm:flex")}
            {heading("uploadBps", t("next.quota.upload"), "hidden justify-center sm:flex")}
            <span />
          </GridRow>
          {/*
            The rows reorder as the peers' speeds move. Left as candidates,
            one of them becomes the browser's scroll anchor and the page
            scrolls after it each time it moves, so none of them is one.
          */}
          <div className="flex min-w-0 flex-col [overflow-anchor:none]">
            {shown.map((peer) => (
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
                <span className="hidden text-center font-wv-mono text-[12px] text-wv-muted sm:block">
                  {peer.connection === "UTP" ? "uTP" : "TCP"}
                </span>
                <span className="text-center font-wv-mono text-[12px] text-wv-muted">{`${(peer.progress * 100).toFixed(1)}%`}</span>
                <span className="hidden text-center font-wv-mono text-[12px] text-wv-muted sm:block">
                  {`↓ ${formatRate(peer.downloadBps)}`}
                </span>
                <span className="hidden text-center font-wv-mono text-[12px] text-wv-muted sm:block">
                  {`↑ ${formatRate(peer.uploadBps)}`}
                </span>
                <SmallAction tone="danger" disabled={banState.fetching} onClick={() => ban(peer.address)}>
                  {t("next.torrent.ban")}
                </SmallAction>
              </GridRow>
            ))}
          </div>
          {pageCount > 1 ? (
            <div className="flex items-center justify-end gap-3 border-t border-wv-hairline pt-3">
              <span data-testid={torrentIds.peerRange} className="font-wv-mono text-[11.5px] whitespace-nowrap text-wv-muted">
                {t("next.pagination.range", {
                  from: formatCount(page * PEER_PAGE_SIZE + 1),
                  to: formatCount(page * PEER_PAGE_SIZE + shown.length),
                  total: formatCount(sorted.length),
                })}
              </span>
              <SecondaryButton
                size="compact"
                testId={torrentIds.peerPrevious}
                disabled={page === 0}
                onClick={() => setAsked(page - 1)}
              >
                {t("action.previous")}
              </SecondaryButton>
              <SecondaryButton
                size="compact"
                testId={torrentIds.peerNext}
                disabled={page + 1 >= pageCount}
                onClick={() => setAsked(page + 1)}
              >
                {t("action.next")}
              </SecondaryButton>
            </div>
          ) : null}
        </>
      )}
    </DetailBlock>
  );
}

/** A heading of the peers: a click sorts by its column, a second turns the order round. */
function PeerHeading({
  sortKey,
  label,
  sort,
  onSort,
  className,
}: {
  sortKey: PeerSortKey;
  label: string;
  sort: PeerSort;
  onSort: (key: PeerSortKey) => void;
  className?: string;
}) {
  const active = sort.key === sortKey;
  return (
    <button
      type="button"
      data-testid={torrentIds.peerSort(sortKey)}
      aria-sort={active ? (sort.descending ? "descending" : "ascending") : undefined}
      onClick={() => onSort(sortKey)}
      className={cn(
        "flex min-w-0 cursor-pointer items-center font-wv-mono text-[11px] whitespace-nowrap hover:text-wv-fg",
        active ? "text-wv-fg" : "text-wv-muted",
        className,
      )}
    >
      {/* The arrow hangs past the label, so a centered label sits over its column's figures. */}
      <span className="relative min-w-0">
        <span className="block truncate">{label}</span>
        {active ? (
          <span aria-hidden className="absolute top-0 left-full pl-1">
            {sort.descending ? "↓" : "↑"}
          </span>
        ) : null}
      </span>
    </button>
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
/** The tone of a small row action: plain, one that adds or changes, or one that removes. */
type ActionTone = "plain" | "accent" | "danger";

const ACTION_TONE: Record<ActionTone, string> = {
  plain: "",
  accent: "!border-wv-accent/50 text-wv-accent hover:!border-wv-accent",
  danger: "!border-wv-danger-border bg-wv-danger-bg text-wv-error-text hover:!border-wv-danger-border-hover hover:bg-wv-danger-bg-hover",
};

/** A small bordered action that fits a table row or a section heading. */
function SmallAction({
  tone = "plain",
  disabled,
  onClick,
  testId,
  children,
}: {
  tone?: ActionTone;
  disabled?: boolean;
  onClick: () => void;
  testId?: string;
  children: ReactNode;
}) {
  return (
    <SecondaryButton
      size="compact"
      testId={testId}
      disabled={disabled}
      onClick={onClick}
      className={cn("h-6 px-2 text-[11.5px]", disabled ? "" : ACTION_TONE[tone])}
    >
      {children}
    </SecondaryButton>
  );
}

/** Who named the tracker, and who resolves its name; whole on hover when the column cuts it. */
function TrackerSource({ tracker }: { tracker: TorrentData["trackers"][number] }) {
  const t = useTranslate();
  const source = [t(trackerSourceKey(tracker.source)), tracker.resolvedBy && t(resolvedByKey(tracker.resolvedBy))]
    .filter(Boolean)
    .join(" · ");
  return (
    <span className="hidden min-w-0 truncate font-wv-mono text-[11.5px] text-wv-muted sm:block" title={source}>
      {source}
    </span>
  );
}

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
  const [dhtState, dhtAnnounce] = useMutation(DHT_ANNOUNCE_TORRENT_MUTATION);
  const [editing, setEditing] = useState<TrackerEdit | null>(null);
  const [error, setError] = useState<string | null>(null);
  const busy =
    addState.fetching || replaceState.fetching || removeState.fetching || reannounceState.fetching || dhtState.fetching;

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

  // One set for each row; the heading row holds an invisible copy so the
  // max-content column is as wide there as in the rows.
  const rowActions = (tracker: TorrentData["trackers"][number]) => (
    <>
      <SmallAction disabled={busy} onClick={() => announce(tracker.url)}>
        {t("next.torrent.reannounce")}
      </SmallAction>
      <SmallAction tone="accent" onClick={() => open({ kind: "edit", from: tracker, url: tracker.url })}>
        {t("next.common.edit")}
      </SmallAction>
      <SmallAction tone="danger" disabled={busy} onClick={() => remove(tracker.url)}>
        {t("next.common.remove")}
      </SmallAction>
    </>
  );

  return (
    <DetailBlock
      id="trackers"
      title={t("next.torrent.trackers")}
      note={countLabel(t, "next.torrent.trackerCount", torrent.trackers.length)}
      right={
        <span className="flex gap-2">
          <SmallAction disabled={busy} onClick={() => announce(null)}>
            {t("next.torrent.reannounce")}
          </SmallAction>
          {/* A private torrent is never looked up on the DHT. */}
          {torrent.private ? null : (
            <SmallAction
              disabled={busy}
              onClick={() => void dhtAnnounce({ id: torrent.id }).then((result) => settle(result, onReport))}
            >
              {t("next.torrent.dhtAnnounce")}
            </SmallAction>
          )}
          <SmallAction tone="accent" onClick={() => open({ kind: "add", urls: "" })}>
            {t("next.torrent.addTrackers")}
          </SmallAction>
        </span>
      }
    >
      {torrent.trackers.length === 0 ? (
        <span className="text-[12.5px] text-wv-muted">{EM_DASH}</span>
      ) : (
        <>
        <GridRow columns={TRACKER_COLUMNS} gap={14} className="h-[28px] font-wv-mono text-[11px] text-wv-muted">
          <span className="min-w-0 truncate">{t("next.torrent.trackerUrl")}</span>
          <span className="hidden min-w-0 truncate sm:block">{t("next.torrent.trackerSource")}</span>
          <span className="min-w-0 truncate">{t("next.torrent.trackerStatus")}</span>
          <span className="hidden min-w-0 truncate text-center sm:block">{t("next.torrent.seeds")}</span>
          <span className="hidden min-w-0 truncate text-center sm:block">{t("next.torrent.trackerLeeches")}</span>
          <span className="hidden min-w-0 truncate text-center sm:block">{t("next.torrent.peers")}</span>
          <span className="hidden min-w-0 truncate sm:block">{t("next.torrent.trackerMessage")}</span>
          <span aria-hidden className="invisible flex gap-2">
            {rowActions(torrent.trackers[0])}
          </span>
        </GridRow>
        {torrent.trackers.map((tracker) => (
          <GridRow
            key={`${tracker.tier}-${tracker.url}`}
            testId={torrentIds.tracker(tracker.url)}
            columns={TRACKER_COLUMNS}
            gap={14}
            className="h-[38px] border-t border-wv-hairline"
            title={tracker.url}
          >
            <span className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">{tracker.url}</span>
            <TrackerSource tracker={tracker} />
            <span
              data-testid={torrentIds.trackerStatus(tracker.updating ? "UPDATING" : tracker.status)}
              className="font-wv-mono text-[11.5px] text-wv-muted"
            >
              {t(tracker.updating ? "next.torrent.tracker.UPDATING" : `next.torrent.tracker.${tracker.status}`)}
            </span>
            <span className="hidden text-center font-wv-mono text-[12px] text-wv-muted sm:block">{count(tracker.seeders)}</span>
            <span className="hidden text-center font-wv-mono text-[12px] text-wv-muted sm:block">{count(tracker.leechers)}</span>
            <span className="hidden text-center font-wv-mono text-[12px] text-wv-muted sm:block">{count(tracker.peers)}</span>
            <span className="hidden min-w-0 truncate text-[12px] text-wv-tertiary sm:block" title={tracker.message ?? undefined}>
              {tracker.message ?? ""}
            </span>
            <span className="flex justify-end gap-2">{rowActions(tracker)}</span>
          </GridRow>
        ))}
        </>
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
                  // A private torrent's trackers are the only place it is
                  // announced: one added by mistake tells an outside tracker
                  // its info hash and this address.
                  body: torrent.private ? (
                    <NoticeBand tone="warn" role="status">
                      {t("next.torrent.privateTrackerWarning")}
                    </NoticeBand>
                  ) : undefined,
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
  const unpacked = useUnpackedFiles(torrent);

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
      {torrent.unpacking ? (
        <GridRow
          testId={torrentIds.unpacking}
          columns={LIMIT_COLUMNS}
          gap={14}
          className="min-h-[46px] border-t border-wv-hairline"
          title={torrent.unpacking.reason ?? undefined}
        >
          <span className="min-w-0 truncate text-[12.5px] text-wv-fg">{t("next.torrent.unpacking")}</span>
          <span className="min-w-0 truncate text-right text-[12.5px] text-wv-muted">
            {torrent.unpacking.reason
              ? `${t(UNPACK_KEYS[torrent.unpacking.result])}: ${torrent.unpacking.reason}`
              : t(UNPACK_KEYS[torrent.unpacking.result])}
          </span>
        </GridRow>
      ) : null}
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
              testId={torrentIds.file}
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
              <span data-testid={torrentIds.filePath} className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">
                {file.path}
              </span>
              <span className="hidden text-right font-wv-mono text-[12px] text-wv-muted sm:block">
                {formatSize(file.length)}
              </span>
              <span data-testid={torrentIds.fileProgress} className="text-right font-wv-mono text-[12px] text-wv-muted">
                {`${(file.progress * 100).toFixed(1)}%`}
              </span>
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
          {unpacked.map((file) => (
            <GridRow
              key={`unpacked:${file.path}`}
              testId={torrentIds.unpackedFile}
              columns={FILE_COLUMNS}
              gap={14}
              className="h-[38px] border-t border-wv-hairline"
              title={file.path}
            >
              <span />
              <span data-testid={torrentIds.filePath} className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">
                {file.path}
              </span>
              <span className="hidden text-right font-wv-mono text-[12px] text-wv-muted sm:block">
                {formatSize(file.length)}
              </span>
              <span data-testid={torrentIds.fileProgress} className="text-right font-wv-mono text-[12px] text-wv-muted">
                100.0%
              </span>
              <span className="truncate text-[12.5px] text-wv-muted">{t("next.torrent.unpacked")}</span>
            </GridRow>
          ))}
        </>
      )}
    </DetailBlock>
  );
}

const UNPACK_KEYS: Record<TorrentUnpackResult, string> = {
  WHOLE: "next.torrent.unpackWhole",
  PART: "next.torrent.unpackPart",
  FAILED: "next.torrent.unpackFailed",
  REFUSED: "next.torrent.unpackRefused",
};

/**
 * The files a torrent's unpacking placed beside its own, read only once its
 * unpacking is over and again when how it came out changes.
 */
function useUnpackedFiles(torrent: TorrentData): TorrentUnpackedFileData[] {
  const result = torrent.unpacking?.result ?? null;
  const [{ data }, reread] = useQuery<{
    torrent: { id: number; unpacking: { files: TorrentUnpackedFileData[] } | null } | null;
  }>({
    query: TORRENT_UNPACKED_FILES_QUERY,
    variables: { id: torrent.id },
    pause: result === null,
    requestPolicy: "network-only",
  });
  useEffect(() => {
    if (result !== null) reread({ requestPolicy: "network-only" });
  }, [result, reread]);
  if (result === null) return [];
  return data?.torrent?.unpacking?.files ?? [];
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
      <DetailBlock id="transfer" title={t("next.torrent.transfer")} note={transferNote(t, torrent)}>
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
          <SmallAction
            tone="accent"
            testId={torrentIds.seedingOverride}
            onClick={() => setEditing(seedingFrom(torrent.seedingOverride))}
          >
            {t("next.torrent.overrideSeeding")}
          </SmallAction>
        }
      >
        <div className="grid gap-x-4 gap-y-[13px]" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))" }}>
          {seedingRows(t, torrent).map((row) => (
            <Field
              key={row.term}
              label={row.label}
              value={
                <>
                  <span data-testid={torrentIds.seedingTerm(row.term)}>{row.value}</span>
                  <span data-testid={torrentIds.seedingLevel(row.term, row.level)} className="ml-[6px] text-wv-muted">
                    {levelText(t, torrent, row.level)}
                  </span>
                </>
              }
            />
          ))}
        </div>
      </DetailBlock>

      <TrackersBlock torrent={torrent} onChanged={onChanged} onReport={onReport} />

      <PeersBlock torrent={torrent} onReport={onReport} />

      <FilesBlock torrent={torrent} onChanged={onChanged} onReport={onReport} />

      <RecordEditor
        open={editing !== null}
        testId={testIds.dialog("torrent-seeding")}
        title={t("next.torrent.overrideSeeding")}
        note={torrent.name}
        busy={seedingState.fetching}
        onSave={() => saveSeeding(editing === null || seedingInherits(editing) ? null : editing)}
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
