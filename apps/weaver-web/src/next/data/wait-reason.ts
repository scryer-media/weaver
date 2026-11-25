// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import { formatSize } from "./format.ts";

/**
 * A wait reason that carries what it waits on after its code and a colon:
 * `login_refused:<host:port>[, <host:port>...]` names the servers refusing
 * the login that the download waits on.
 */
const LOGIN_REFUSED = "login_refused:";

/** What the inspector says a download is waiting for, from its wait reason. */
export function waitReasonDetail(t: Translate, reason: string): string {
  if (reason.startsWith(LOGIN_REFUSED)) {
    return t("next.status.wait.login_refused", {
      server: reason.slice(LOGIN_REFUSED.length),
    });
  }
  // Any other reason is either a code the screen names, or the daemon's own
  // sentence, shown as it came.
  if (!/^[a-z0-9_]+$/i.test(reason)) return reason;
  const key = `next.status.wait.${reason.toLowerCase()}`;
  const label = t(key);
  return label === key ? reason.toLowerCase().replace(/_/g, " ") : label;
}

/**
 * A job kept from starting by its disk, with what it needs and what the disk
 * leaves: "needs X, Y free", or for a disk too small "needs X, holds Y".
 * Null when the row carries no figures, as for a job a full disk held.
 */
export function diskWaitDetail(
  t: Translate,
  reason: string,
  need: number | null | undefined,
  free: number | null | undefined,
): string | null {
  if (need == null) return null;
  if (reason === "disk_space") {
    return free == null
      ? t("next.status.wait.disk_space_unread", { need: formatSize(need) })
      : t("next.status.wait.disk_space_need", {
          need: formatSize(need),
          free: formatSize(free),
        });
  }
  if (reason === "disk_too_small" && free != null) {
    return t("next.status.wait.disk_too_small_need", {
      need: formatSize(need),
      most: formatSize(free),
    });
  }
  return null;
}
