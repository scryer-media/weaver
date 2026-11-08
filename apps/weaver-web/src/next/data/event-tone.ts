// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/**
 * The colour an event kind takes in a job's log.
 *
 * Event kinds are open-ended — the engine adds them as the pipeline grows — so
 * this reads the name rather than enumerating it. Three tones only: something
 * went wrong, something finished well, something the engine decided (a retry,
 * a skipped recovery set, an archive given up on). Everything else stays quiet.
 */
export function eventTone(kind: string): "bad" | "good" | "note" | "plain" {
  if (/FAIL|ERROR|MISSING|CANCEL|INTERRUPT|CORRUPT|DAMAGE/.test(kind)) {
    return "bad";
  }
  if (/COMPLETE|SUCCEED|PASSED|REPAIRED/.test(kind)) {
    return "good";
  }
  if (/RETRY|SKIP|REPAIR|PAUSE|RESUME|WAIT|PROPAGAT|DEMOT/.test(kind)) {
    return "note";
  }
  return "plain";
}
