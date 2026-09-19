import type { Translate } from "@/lib/context/translate-context";
import type { ProviderHealth } from "./next-data";
import { formatDuration } from "./format.ts";

/** What the rail says about one server, once its state has had its say. */
export interface ProviderActivity {
  /** The state word the row leads with. */
  word: string;
  /** One line saying why, for every state that is not simply running. */
  cause: string | null;
  /** The raw counts — active, connected, maximum — kept as the secondary reading. */
  fraction: string;
  /** Which of the rail's colours the bar takes. */
  tone: "accent" | "warn" | "error" | "inert";
}

/**
 * Turn one server's activity into the words the rail shows.
 *
 * "1 / 100" is a true statement that reads as a broken daemon, because a
 * fraction only answers "how much" while the question someone actually has is
 * "why so little". So the state word leads and the fraction follows it: a
 * provider refusing connections, a pool still reading an index, and a server
 * with nothing left to fetch all show one of a hundred, and they are three
 * different situations with three different answers.
 *
 * The counts never disappear — a state word nobody can check is worse than a
 * fraction nobody can read — they stop being the headline.
 *
 * Pure, so it can be read back in a test: the clock comes in rather than being
 * asked for, and `t` does the wording.
 */
export function providerActivityLabel(
  provider: ProviderHealth,
  t: Translate,
  now: number,
): ProviderActivity {
  const max = provider.connectionsMax || provider.connectionsConfigured;
  const remaining =
    provider.activityUntilEpochMs != null && provider.activityUntilEpochMs > now
      ? formatDuration((provider.activityUntilEpochMs - now) / 1000)
      : null;
  // Two counts against one maximum: how many sockets are connected, and how
  // many of those are carrying a request. A held-back pool is the case where
  // they part company, and that gap is the whole story.
  const counts = t("next.rail.counts", {
    busy: provider.connectionsBusy,
    open: provider.connectionsOpen,
    max,
  });

  switch (provider.activity) {
    case "disabled":
      return {
        word: t("next.rail.activity.disabled"),
        cause: t("next.rail.cause.disabled"),
        fraction: counts,
        tone: "error",
      };
    case "cooling_down":
      return {
        word: t("next.rail.activity.coolingDown"),
        cause: remaining
          ? t("next.rail.cause.coolingDownResume", { time: remaining })
          : t("next.rail.cause.coolingDown"),
        fraction: counts,
        tone: "warn",
      };
    case "over_limit":
      return {
        word: t("next.rail.activity.overLimit"),
        cause: remaining
          ? t("next.rail.cause.overLimitRetry", { time: remaining })
          : t("next.rail.cause.overLimit"),
        fraction: counts,
        tone: "warn",
      };
    case "degraded":
      return {
        word: t("next.rail.activity.degraded"),
        cause: t("next.rail.cause.degraded"),
        fraction: counts,
        tone: "error",
      };
    case "preparing":
      return {
        word: t("next.rail.activity.preparing"),
        cause: t("next.rail.cause.preparing"),
        fraction: counts,
        tone: "inert",
      };
    case "downloading":
      return {
        word: t("next.rail.activity.downloading"),
        cause: null,
        fraction: counts,
        tone: "accent",
      };
    // A server the daemon has not classified is not worth inventing a word
    // for; idle is what the rail said before any of this existed.
    default:
      return {
        word: t("next.rail.idle"),
        cause: null,
        fraction: counts,
        tone: "inert",
      };
  }
}
