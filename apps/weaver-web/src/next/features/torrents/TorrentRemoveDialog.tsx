// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { useMutation } from "urql";
import { REMOVE_TORRENT_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { removeOutcome } from "@/lib/torrents";
import { ConfirmDialog } from "@/next/components/ConfirmDialog";
import { CheckRow } from "@/next/features/networking/presentation";
import { testIds, torrentIds } from "@/next/testids";

/**
 * Removing a torrent: first whether its files go too, then, when the row
 * says its seeding goal is not met yet, what is left of it and a second
 * confirm. The daemon never refuses the remove itself.
 */
export function TorrentRemoveDialog({
  id,
  name,
  goalLeft = null,
  onRemoved,
  onFailed,
  onDismiss,
}: {
  /** The torrent's job id; the dialog is open while it is set. */
  id: number | null;
  name?: string;
  /** What is left of its seeding goal, as its row holds it; null once met or with none. */
  goalLeft?: string | null;
  onRemoved: (id: number) => void;
  onFailed: (message: string) => void;
  onDismiss: () => void;
}) {
  const t = useTranslate();
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [unmet, setUnmet] = useState(false);
  const [state, removeTorrent] = useMutation<{ removeTorrent: boolean }>(REMOVE_TORRENT_MUTATION);

  const close = () => {
    setDeleteFiles(false);
    setUnmet(false);
    onDismiss();
  };

  const remove = async (goalConfirmed: boolean) => {
    if (id === null) return;
    if (goalLeft !== null && !goalConfirmed) {
      setUnmet(true);
      return;
    }
    const result = await removeTorrent({ id, deleteFiles });
    const outcome = removeOutcome(result.error);
    setDeleteFiles(false);
    setUnmet(false);
    if (outcome.kind === "removed") {
      onRemoved(id);
    } else {
      onFailed(outcome.message);
    }
  };

  return (
    <>
      <ConfirmDialog
        open={id !== null && !unmet}
        testId={testIds.dialog("torrent-remove")}
        title={t("next.torrent.removeTitle")}
        note={name}
        busy={state.fetching}
        body={
          <CheckRow
            checked={deleteFiles}
            onChange={setDeleteFiles}
            label={t("next.torrent.deleteFiles")}
            testId={torrentIds.removeDeleteFiles}
          />
        }
        confirmLabel={t("next.common.remove")}
        onConfirm={() => void remove(false)}
        onDismiss={close}
      />
      <ConfirmDialog
        open={id !== null && unmet}
        testId={testIds.dialog("torrent-unmet-goal")}
        title={t("next.torrent.unmetGoalTitle")}
        note={name}
        busy={state.fetching}
        body={t("next.torrent.unmetGoalBody", { left: goalLeft ?? "" })}
        confirmLabel={t("next.torrent.removeAnyway")}
        solid
        onConfirm={() => void remove(true)}
        onDismiss={close}
      />
    </>
  );
}
