// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

export type HealthTone = "good" | "warn" | "low" | "failing";

// The health, in tenths of a percent, below which the job's loss outruns
// what its recovery can rebuild: the server's critical health. A post with
// no recovery fails below 85%; one whose recovery is over half the post
// never does.
export function criticalHealthMilli(totalBytes: number, recoveryBytes: number): number {
  if (recoveryBytes <= 0) return 850;
  if (recoveryBytes * 2 > totalBytes) return 0;
  return Math.floor(((totalBytes - recoveryBytes * 2) * 1000) / (totalBytes - recoveryBytes));
}

// Green at 95% and up, yellow from 85%, orange below; red below the job's
// critical health, whatever the band.
export function healthTone(healthMilli: number, criticalMilli: number): HealthTone {
  if (healthMilli < criticalMilli) return "failing";
  if (healthMilli >= 950) return "good";
  if (healthMilli >= 850) return "warn";
  return "low";
}

export const HEALTH_TONE_CLASS: Record<HealthTone, string> = {
  good: "text-wv-accent",
  warn: "text-wv-warn",
  low: "text-wv-orange",
  failing: "text-wv-error",
};
