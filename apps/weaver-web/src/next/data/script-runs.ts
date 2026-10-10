import type { Translate } from "@/lib/context/translate-context";
import { formatSpan } from "./format.ts";

/**
 * What started a script run, in words.
 *
 * The daemon names a trigger as it stores it: `post_processing`, `scan`, or a
 * kind and what it was for, as in `queue:NZB_ADDED`. A queue event keeps its
 * name; the id after a feed or a schedule tells a reader nothing and is dropped.
 */
export function triggerLabel(t: Translate, event: string): string {
  const split = event.indexOf(":");
  const kind = split < 0 ? event : event.slice(0, split);
  const detail = split < 0 ? "" : event.slice(split + 1);
  switch (kind) {
    case "post_processing":
      return t("next.postProcessing.kindPostProcessing");
    case "queue":
      return detail === ""
        ? t("next.postProcessing.kindQueue")
        : `${t("next.postProcessing.kindQueue")} · ${detail}`;
    case "scan":
      return t("next.postProcessing.kindScan");
    case "feed":
      return t("next.postProcessing.kindFeed");
    case "scheduler":
      return t("next.schedules.schedule");
    default:
      return event;
  }
}

/** How long a run took: `240 ms` under a second, then `1.5s`, `2m 05s`. */
export function formatRunDuration(milliseconds: number): string {
  return milliseconds >= 0 && milliseconds < 1000
    ? `${Math.round(milliseconds)} ms`
    : formatSpan(milliseconds);
}
