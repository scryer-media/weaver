// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { useMutation, useSubscription } from "urql";
import { SET_TORRENT_SEEDING_MUTATION, TORRENT_UPDATES_SUBSCRIPTION } from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { torrentErrorText, type TorrentData } from "@/lib/torrents";
import { DetailBlock, Field } from "@/next/components/chrome";
import { SecondaryButton } from "@/next/components/controls";
import { RecordEditor } from "@/next/components/RecordEditor";
import { GridRow } from "@/next/components/rows";
import { EM_DASH, formatRate, formatSize } from "@/next/data/format";
import { useNextData } from "@/next/data/next-data";
import { countLabel } from "@/next/i18n/labels";
import { EMPTY_SEEDING, durationText, seedingFields, seedingInput, type SeedingPolicy } from "@/next/pages/settings/seeding";

const TRACKER_COLUMNS = { base: "minmax(0,1fr) 112px", sm: "minmax(0,1fr) 112px minmax(0,1fr)" };
const FILE_COLUMNS = { base: "minmax(0,1fr) 56px 90px" };

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

/** A torrent job's own blocks on the job's screen: the swarm, its trackers, its files and how it seeds. */
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

  const percent = torrent.totalBytes > 0 ? (torrent.haveBytes / torrent.totalBytes) * 100 : 0;
  const swarm = [
    { label: t("next.torrent.infoHash"), value: torrent.infohash },
    { label: t("next.inspector.totalSize"), value: formatSize(torrent.totalBytes) },
    { label: t("next.torrent.progress"), value: `${percent.toFixed(1)}%` },
    { label: t("next.torrent.seedsLeechers"), value: `${torrent.seeds} / ${torrent.leechers}` },
    { label: t("next.torrent.peers"), value: String(torrent.peers) },
    { label: t("next.torrent.ratio"), value: torrent.ratio.toFixed(2) },
    { label: t("next.torrent.uploaded"), value: formatSize(torrent.uploadedBytes) },
    { label: t("next.job.downloaded"), value: formatSize(torrent.downloadedBytes) },
    { label: t("next.torrent.route"), value: torrent.route ?? EM_DASH },
    { label: t("next.torrents.unpack"), value: t(torrent.unpack ? "next.common.on" : "next.common.off") },
  ];

  return (
    <>
      <DetailBlock id="torrent" title={t("next.protocol.torrent")} note={torrent.detail ?? undefined}>
        <div className="grid gap-x-4 gap-y-[13px]" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))" }}>
          {swarm.map((field) => (
            <Field key={field.label} label={field.label} value={field.value} title={field.value} />
          ))}
        </div>
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

      <DetailBlock
        id="trackers"
        title={t("next.torrent.trackers")}
        note={countLabel(t, "next.torrent.trackerCount", torrent.trackers.length)}
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
              <span className="font-wv-mono text-[11.5px] text-wv-muted">
                {t(`next.torrent.tracker.${tracker.status}`)}
              </span>
              <span className="hidden min-w-0 truncate text-[12px] text-wv-tertiary sm:block" title={tracker.message ?? undefined}>
                {tracker.message ?? ""}
              </span>
            </GridRow>
          ))
        )}
      </DetailBlock>

      <DetailBlock
        id="torrent-files"
        title={t("next.common.files")}
        note={countLabel(t, "next.job.fileCount", torrent.files.length)}
      >
        {torrent.files.length === 0 ? (
          <span className="text-[12.5px] text-wv-muted">{EM_DASH}</span>
        ) : (
          torrent.files.map((file) => (
            <GridRow
              key={file.index}
              columns={FILE_COLUMNS}
              gap={14}
              className="h-[38px] border-t border-wv-hairline"
              title={file.path}
            >
              <span className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">{file.path}</span>
              <span className="text-right font-wv-mono text-[12px] text-wv-muted">{`${(file.progress * 100).toFixed(1)}%`}</span>
              <span className="text-right font-wv-mono text-[12px] text-wv-muted">{formatSize(file.length)}</span>
            </GridRow>
          ))
        )}
      </DetailBlock>

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
