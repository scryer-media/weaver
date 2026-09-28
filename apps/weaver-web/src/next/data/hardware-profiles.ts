/**
 * The hardware-profile choice, reduced to what a picker needs to draw.
 *
 * Every number belongs to the daemon: it knows what each profile would do on
 * this machine and reports it, so the cards never keep a second copy that can
 * drift from the one the pipeline actually runs.
 */

import { formatSize } from "./format.ts";
import type { Translate } from "@/lib/context/translate-context";

export type HardwareProfileName = "EFFICIENT" | "BALANCED" | "PERFORMANCE";

/** What one profile would do here, as the daemon resolves it. */
export interface HardwareProfileOption {
  profile: HardwareProfileName;
  sevenzDecodeMemoryBytes: number;
  decodeThreads: number;
  extractThreads: number;
  /** Null when the configured connection count is the only limit. */
  maxConcurrentDownloads: number | null;
}

export interface HardwareProfileSettings {
  selected: HardwareProfileName | null;
  recommended: HardwareProfileName;
  available: HardwareProfileName[];
  options: HardwareProfileOption[];
  detected: { memoryBytes: number; cores: number };
}

const NAME_KEY: Record<HardwareProfileName, string> = {
  EFFICIENT: "next.performance.efficient",
  BALANCED: "next.performance.balanced",
  PERFORMANCE: "next.performance.performance",
};

const BODY_KEY: Record<HardwareProfileName, string> = {
  EFFICIENT: "next.performance.efficientBody",
  BALANCED: "next.performance.balancedBody",
  PERFORMANCE: "next.performance.performanceBody",
};

/**
 * Whether the question is worth asking.
 *
 * A machine that can honour only one profile has nothing to choose, so the
 * setup step and the settings section are both left out rather than shown with
 * a single card nobody can decline.
 */
export function offersProfileChoice(settings: HardwareProfileSettings | null | undefined): boolean {
  return (settings?.available.length ?? 0) > 1;
}

/** The card the picker should open on: the operator's choice, else the recommendation. */
export function initialProfile(settings: HardwareProfileSettings): HardwareProfileName {
  return settings.selected ?? settings.recommended;
}

export function profileName(t: Translate, profile: HardwareProfileName): string {
  return t(NAME_KEY[profile]);
}

export function profileBody(t: Translate, profile: HardwareProfileName): string {
  return t(BODY_KEY[profile]);
}

/**
 * The two or three concrete lines under a card's name, so a name means
 * something before it is picked.
 */
export function profileFacts(t: Translate, option: HardwareProfileOption): string[] {
  const facts = [
    t("next.performance.factDecodeMemory", {
      size: formatSize(option.sevenzDecodeMemoryBytes),
    }),
    t("next.performance.factThreads", {
      decode: option.decodeThreads,
      extract: option.extractThreads,
    }),
  ];
  if (option.maxConcurrentDownloads !== null) {
    facts.push(t("next.performance.factDownloads", { count: option.maxConcurrentDownloads }));
  }
  return facts;
}

/** "16 GB RAM, 8 cores" — what the recommendation was judged against. */
export function detectedHardware(t: Translate, settings: HardwareProfileSettings): string {
  return t("next.performance.detected", {
    memory: formatSize(settings.detected.memoryBytes),
    cores: settings.detected.cores,
  });
}
