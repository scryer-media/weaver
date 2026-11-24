// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { ReactNode } from "react";
import type { Client } from "urql";
import { TORRENT_STATE_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import type { TorrentState } from "@/lib/torrents";
import { ConfirmDialog } from "@/next/components/ConfirmDialog";
import { torrentIds } from "@/next/testids";

/** Whether a torrent in `state` is held because its files are gone. */
export function isFilesMissing(state: TorrentState | null | undefined): boolean {
  return state === "FILES_MISSING";
}

/**
 * Whether the torrent is still held for its files once a Resume has been
 * answered. The resume itself looks for them again, so files put back let it
 * go; only a torrent still held is explained.
 */
export async function stillFilesMissing(client: Client, id: number): Promise<boolean> {
  const result = await client
    .query<{ torrent: { state: TorrentState } | null }>(TORRENT_STATE_QUERY, { id }, { requestPolicy: "network-only" })
    .toPromise();
  return isFilesMissing(result.data?.torrent?.state);
}

/**
 * What Resume opens on a torrent still missing its files after it looked
 * for them again: resuming cannot help until the files are back or the
 * torrent points where they are, and a recheck starts the download again
 * from what is on disk.
 */
export function FilesMissingDialog({
  open,
  note,
  busy = false,
  onRecheck,
  other,
  onDismiss,
}: {
  open: boolean;
  note?: ReactNode;
  busy?: boolean;
  onRecheck: () => void;
  /** Where the location is changed: Move on the job's page, or the way there. */
  other: { label: string; onConfirm: () => void };
  onDismiss: () => void;
}) {
  const t = useTranslate();
  return (
    <ConfirmDialog
      open={open}
      title={t("next.torrent.filesMissing")}
      note={note}
      body={t("next.torrent.filesMissingBody")}
      confirmLabel={t("next.torrent.recheck")}
      destructive={false}
      busy={busy}
      alternative={{ ...other, plain: true }}
      onConfirm={onRecheck}
      onDismiss={onDismiss}
      testId={torrentIds.filesMissingDialog}
    />
  );
}
