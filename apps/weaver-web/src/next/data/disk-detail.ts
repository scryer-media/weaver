// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { DiskVolume } from "@/lib/context/live-data-context";
import type { Translate } from "@/lib/context/translate-context";
import type { DiskWrite, JobData } from "@/lib/job-types";
import { formatRate, formatSize } from "./format.ts";

type DiskJob = Pick<
  JobData,
  "diskWrite" | "status" | "remainingBytes" | "totalBytes" | "downloadedBytes"
>;

/** Statuses that show their own work: the disk is named only while it is slow. */
const WORKING = new Set(["VERIFYING", "REPAIRING", "EXTRACTING"]);
/** Statuses that fetch bytes: only these can be held back by a slow disk. */
const FETCHING = new Set(["DOWNLOADING", "FETCHING_REPAIR_DATA"]);

/**
 * The disk write a row is showing: a download a slow disk holds back while it
 * fetches (a job waiting its turn or paused writes nothing, whatever disk it
 * is bound for), or one done fetching whose bytes still wait for
 * its disk, slow or not. A job verifying, repairing or unpacking shows its own
 * status, and the disk only while its own writes wait on a slow one, as the
 * server lists it. Null otherwise.
 */
export function shownDiskWrite(
  job: DiskJob,
): { write: DiskWrite; fetched: boolean } | null {
  const write = job.diskWrite;
  if (!write) return null;
  if (WORKING.has(job.status))
    return write.slow ? { write, fetched: false } : null;
  const fetched =
    (job.remainingBytes ?? job.totalBytes - job.downloadedBytes) <= 0;
  if (fetched) return write.draining ? { write, fetched } : null;
  return write.slow && FETCHING.has(job.status) ? { write, fetched } : null;
}

/** The figures of the disk `write` names, from the live disk list. */
export function diskVolumeOf(
  write: Pick<DiskWrite, "volume">,
  volumes: readonly DiskVolume[],
): DiskVolume | undefined {
  return volumes.find((volume) => volume.label === write.volume);
}

/**
 * What the download is doing with its disk, in full: still fetching, with the
 * bytes waiting for its disk and the rate the disk takes them, or done
 * fetching, with the bytes left to write. The figures are the disk's, from
 * every job that writes there, so the text names the disk. Until the live
 * disk list carries the disk, the short form. Null when the row shows no disk
 * write.
 */
export function diskDetail(
  t: Translate,
  job: DiskJob,
  volumes: readonly DiskVolume[],
): string | null {
  const shown = shownDiskWrite(job);
  if (!shown) return null;
  const { write, fetched } = shown;
  const figures = diskVolumeOf(write, volumes);
  if (!figures) return t("next.status.diskShort");
  const volume = write.volume;
  const waiting = formatSize(figures.waitingBytes);
  if (fetched) {
    return t("next.status.diskLeft", { left: waiting, volume });
  }
  return figures.writeRateBytesPerSec > 0
    ? t("next.status.diskWriting", {
        waiting,
        volume,
        rate: formatRate(figures.writeRateBytesPerSec),
      })
    : t("next.status.diskWaiting", { waiting, volume });
}

/** The short form for a row's status line; the full text is `diskDetail`. */
export function diskShort(t: Translate, job: DiskJob): string | null {
  return shownDiskWrite(job) ? t("next.status.diskShort") : null;
}
