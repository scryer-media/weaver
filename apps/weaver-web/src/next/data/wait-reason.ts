// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";

/**
 * A wait reason that carries what it waits on after its code and a colon:
 * `login_refused:<host:port>[, <host:port>...]` names the servers refusing
 * the login that the download waits on.
 */
const LOGIN_REFUSED = "login_refused:";

/** What the inspector says a download is waiting for, from its wait reason. */
export function waitReasonDetail(t: Translate, reason: string): string {
  if (reason.startsWith(LOGIN_REFUSED)) {
    return t("next.status.wait.login_refused", { server: reason.slice(LOGIN_REFUSED.length) });
  }
  const key = `next.status.wait.${reason.toLowerCase()}`;
  const label = t(key);
  return label === key ? reason.toLowerCase().replace(/_/g, " ") : label;
}
