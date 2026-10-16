// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

export type DuplicateAction = "ACCEPT" | "WARN" | "PAUSE" | "BLOCK";

export type DuplicatePolicy = {
  strictActiveOrSuccess: DuplicateAction;
  strictFailedOrCancelled: DuplicateAction;
  articleLayoutActiveOrSuccess: DuplicateAction;
  articleLayoutFailedOrCancelled: DuplicateAction;
  articleSet: DuplicateAction;
  normalizedName: DuplicateAction;
};

export const DUPLICATE_ACTIONS: DuplicateAction[] = ["ACCEPT", "WARN", "PAUSE", "BLOCK"];

export const DEFAULT_DUPLICATE_POLICY: DuplicatePolicy = {
  strictActiveOrSuccess: "BLOCK",
  strictFailedOrCancelled: "WARN",
  articleLayoutActiveOrSuccess: "PAUSE",
  articleLayoutFailedOrCancelled: "WARN",
  articleSet: "WARN",
  normalizedName: "WARN",
};

export function normalizeDuplicatePolicy(value?: Partial<DuplicatePolicy> | null): DuplicatePolicy {
  return { ...DEFAULT_DUPLICATE_POLICY, ...value };
}
