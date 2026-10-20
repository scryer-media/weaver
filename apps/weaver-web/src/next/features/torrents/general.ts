// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import type { TorrentData } from "@/lib/torrents";
import { EM_DASH, formatDate, formatDuration, formatRate, formatSize } from "../../data/format.ts";

/** One row of a torrent's general sections; a wide one spans the whole row. */
export interface TorrentField {
  label: string;
  value: string;
  wide?: boolean;
}

const seconds = (value: number | null) => (value === null ? EM_DASH : formatDuration(value));
const text = (value: string | null) => (value === null || value === "" ? EM_DASH : value);

/** The Transfer section, in qBittorrent's order. */
export function transferFields(t: Translate, torrent: TorrentData): TorrentField[] {
  const total = (count: number, total: number | null) =>
    t("next.torrent.totalValue", { count, total: total === null ? EM_DASH : String(total) });
  const speed = (rate: number, average: number) =>
    t("next.torrent.averageValue", { rate: formatRate(rate), average: formatRate(average) });
  return [
    { label: t("next.torrent.timeActive"), value: formatDuration(torrent.activeSeconds) },
    { label: t("next.torrent.eta"), value: seconds(torrent.etaSeconds) },
    {
      label: t("next.torrent.connections"),
      value: t("next.torrent.connectionsValue", { count: torrent.peers, max: torrent.maxPeers }),
    },
    { label: t("next.job.downloaded"), value: formatSize(torrent.downloadedBytes) },
    { label: t("next.torrent.uploaded"), value: formatSize(torrent.uploadedBytes) },
    { label: t("next.torrent.seeds"), value: total(torrent.seeds, torrent.swarmSeeds) },
    { label: t("next.torrent.downloadSpeed"), value: speed(torrent.downloadBps, torrent.downloadAvgBps) },
    { label: t("next.torrent.uploadSpeed"), value: speed(torrent.uploadBps, torrent.uploadAvgBps) },
    { label: t("next.torrent.peers"), value: total(torrent.leechers, torrent.swarmLeechers) },
    { label: t("next.torrent.wasted"), value: formatSize(torrent.wastedBytes) },
    { label: t("next.torrent.shareRatio"), value: torrent.ratio.toFixed(2) },
    { label: t("next.torrent.reannounceIn"), value: seconds(torrent.reannounceSeconds) },
    { label: t("next.torrent.lastSeenComplete"), value: formatDate(torrent.lastSeenCompleteMs) },
  ];
}

/** The Information section, in qBittorrent's order, with the route and unpacking after it. */
export function informationFields(t: Translate, torrent: TorrentData): TorrentField[] {
  const pieces =
    torrent.pieceCount === null || torrent.pieceSize === null
      ? EM_DASH
      : t("next.torrent.piecesValue", {
          count: torrent.pieceCount,
          size: formatSize(torrent.pieceSize),
          have: torrent.piecesHave ?? 0,
        });
  return [
    { label: t("next.inspector.totalSize"), value: formatSize(torrent.totalBytes) },
    { label: t("next.torrent.pieces"), value: pieces },
    { label: t("next.torrent.createdBy"), value: text(torrent.createdBy) },
    { label: t("next.torrent.addedOn"), value: formatDate(torrent.addedAtMs) },
    { label: t("next.torrent.completedOn"), value: formatDate(torrent.completedAtMs) },
    { label: t("next.torrent.createdOn"), value: formatDate(torrent.createdAtMs) },
    { label: t("next.torrent.private"), value: t(torrent.private ? "next.common.yes" : "next.common.no") },
    { label: t("next.torrent.route"), value: text(torrent.route) },
    { label: t("next.torrents.unpack"), value: t(torrent.unpack ? "next.common.on" : "next.common.off") },
    { label: t("next.torrent.infoHashV1"), value: text(torrent.infohash), wide: true },
    { label: t("next.torrent.savePath"), value: text(torrent.savePath), wide: true },
    { label: t("next.torrent.comment"), value: text(torrent.comment), wide: true },
  ];
}
