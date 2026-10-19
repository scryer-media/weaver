// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { useMutation } from "urql";
import { REMOVE_TORRENT_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { removeOutcome } from "@/lib/torrents";
import { ConfirmDialog } from "@/next/components/ConfirmDialog";
import { CheckRow } from "@/next/features/networking/presentation";

/**
 * Removing a torrent: first whether its files go too, then, only when the
 * daemon answers that its seeding goal is not met yet, the daemon's reason
 * and a second confirm that sends the remove again with that confirmed.
 */
export function TorrentRemoveDialog({
  id,
  name,
  onRemoved,
  onFailed,
  onDismiss,
}: {
  /** The torrent's job id; the dialog is open while it is set. */
  id: number | null;
  name?: string;
  onRemoved: (id: number) => void;
  onFailed: (message: string) => void;
  onDismiss: () => void;
}) {
  const t = useTranslate();
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [unmet, setUnmet] = useState<string | null>(null);
  const [state, removeTorrent] = useMutation<{ removeTorrent: boolean }>(REMOVE_TORRENT_MUTATION);

  const close = () => {
    setDeleteFiles(false);
    setUnmet(null);
    onDismiss();
  };

  const remove = async (confirmUnmetGoal: boolean) => {
    if (id === null) return;
    const result = await removeTorrent({ id, deleteFiles, confirmUnmetGoal });
    const outcome = removeOutcome(result.error);
    if (outcome.kind === "unmetGoal" && !confirmUnmetGoal) {
      setUnmet(outcome.message);
      return;
    }
    setDeleteFiles(false);
    setUnmet(null);
    if (outcome.kind === "removed") {
      onRemoved(id);
    } else {
      onFailed(outcome.message);
    }
  };

  return (
    <>
      <ConfirmDialog
        open={id !== null && unmet === null}
        title={t("next.torrent.removeTitle")}
        note={name}
        busy={state.fetching}
        body={
          <CheckRow checked={deleteFiles} onChange={setDeleteFiles} label={t("next.torrent.deleteFiles")} />
        }
        confirmLabel={t("next.common.remove")}
        onConfirm={() => void remove(false)}
        onDismiss={close}
      />
      <ConfirmDialog
        open={id !== null && unmet !== null}
        title={t("next.torrent.unmetGoalTitle")}
        note={name}
        busy={state.fetching}
        body={unmet}
        confirmLabel={t("next.torrent.removeAnyway")}
        solid
        onConfirm={() => void remove(true)}
        onDismiss={close}
      />
    </>
  );
}
