// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useCallback, useLayoutEffect, useRef, useState } from "react";
import { useMutation } from "urql";
import { SUBMIT_TORRENT_MUTATION } from "@/graphql/queries";
import { base64Of, isMagnetLink, torrentErrorText } from "@/lib/torrents";

/** A `.torrent` file waiting in the Add dialog, and how its submit went. */
export interface TorrentEntry {
  localId: string;
  file: File;
  status: "staged" | "submitting" | "submitted" | "failed";
  /** The daemon's refusal, as it gave it. */
  error?: string;
}

export interface TorrentSubmitOptions {
  category: string | null;
  paused: boolean;
  forced: boolean;
}

let nextLocalId = 0;

/**
 * The torrent side of the Add dialog: picked or dropped `.torrent` files and
 * one magnet link, each sent on its own through `submitTorrent`.
 */
export function useAddTorrents() {
  const [entries, setEntries] = useState<TorrentEntry[]>([]);
  const [magnet, setMagnet] = useState("");
  const [magnetError, setMagnetError] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);
  const [forced, setForced] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [, submitTorrent] = useMutation<{ submitTorrent: { id: number } }>(SUBMIT_TORRENT_MUTATION);
  const entriesRef = useRef(entries);
  useLayoutEffect(() => {
    entriesRef.current = entries;
  });

  const reset = useCallback(() => {
    setEntries([]);
    setMagnet("");
    setMagnetError(null);
    setPaused(false);
    setForced(false);
  }, []);

  const addFiles = useCallback((files: readonly File[]) => {
    const listed = new Set(entriesRef.current.map((entry) => `${entry.file.name}:${entry.file.size}`));
    const added = files
      .filter((file) => !listed.has(`${file.name}:${file.size}`))
      .map<TorrentEntry>((file) => ({ localId: `torrent-${nextLocalId++}`, file, status: "staged" }));
    if (added.length > 0) setEntries((current) => [...current, ...added]);
  }, []);

  const removeFile = useCallback((localId: string) => {
    setEntries((current) => current.filter((entry) => entry.localId !== localId));
  }, []);

  const editMagnet = useCallback((value: string) => {
    setMagnet(value);
    setMagnetError(null);
  }, []);

  const magnetText = magnet.trim();
  const magnetInvalid = magnetText !== "" && !isMagnetLink(magnetText);
  const readyCount =
    entries.filter((entry) => entry.status === "staged" || entry.status === "failed").length +
    (magnetText !== "" && !magnetInvalid ? 1 : 0);

  /** Sends everything waiting; true when nothing was refused. */
  const submit = useCallback(
    async (options: TorrentSubmitOptions): Promise<boolean> => {
      const waiting = entriesRef.current.filter((entry) => entry.status === "staged" || entry.status === "failed");
      const link = magnet.trim();
      if (waiting.length === 0 && (link === "" || !isMagnetLink(link))) return true;
      setSubmitting(true);
      const ids = new Set(waiting.map((entry) => entry.localId));
      setEntries((current) =>
        current.map((entry) => (ids.has(entry.localId) ? { ...entry, status: "submitting", error: undefined } : entry)),
      );
      const base = { category: options.category, paused: options.paused, forced: options.forced };
      let clean = true;
      for (const entry of waiting) {
        const metainfoBase64 = base64Of(new Uint8Array(await entry.file.arrayBuffer()));
        const result = await submitTorrent({ input: { ...base, metainfoBase64 } });
        const error = result.error ? torrentErrorText(result.error) : undefined;
        if (error !== undefined) clean = false;
        setEntries((current) =>
          current.map((candidate) =>
            candidate.localId === entry.localId
              ? { ...candidate, status: error === undefined ? "submitted" : "failed", error }
              : candidate,
          ),
        );
      }
      if (link !== "" && isMagnetLink(link)) {
        const result = await submitTorrent({ input: { ...base, magnet: link } });
        if (result.error) {
          clean = false;
          setMagnetError(torrentErrorText(result.error));
        } else {
          setMagnet("");
        }
      }
      setSubmitting(false);
      return clean;
    },
    [magnet, submitTorrent],
  );

  return {
    entries,
    magnet,
    setMagnet: editMagnet,
    magnetError,
    magnetInvalid,
    paused,
    setPaused,
    forced,
    setForced,
    submitting,
    readyCount,
    hasSources: entries.length > 0 || magnetText !== "",
    addFiles,
    removeFile,
    reset,
    submit,
  };
}
