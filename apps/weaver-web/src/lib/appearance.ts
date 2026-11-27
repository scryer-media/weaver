// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import {
  OFFLINE_APPEARANCE_CACHE,
  OFFLINE_APPEARANCE_PATH,
} from "./offline-appearance.ts";

export const PALETTES = ["WEAVER", "PHOSPHOR", "AMBER", "TERMINAL"] as const;
export const MODES = ["SYSTEM", "LIGHT", "DARK"] as const;
export const ACCENTS = [
  "TEAL",
  "BLUE",
  "GREEN",
  "AMBER",
  "VIOLET",
  "ROSE",
] as const;

// Queued is not here: it is always grey.
export const PROGRESS_STAGES = [
  "DOWNLOADING",
  "PAUSED",
  "CHECKING",
  "COPYING",
  "EXTRACTING",
  "COMPLETED",
  "FAILED",
] as const;
export const PROGRESS_COLORS = [
  "GREY",
  "TEAL",
  "CYAN",
  "BLUE",
  "INDIGO",
  "VIOLET",
  "MAGENTA",
  "ROSE",
  "RED",
  "AMBER",
  "LIME",
  "GREEN",
] as const;

export type Palette = (typeof PALETTES)[number];
export type ProgressStage = (typeof PROGRESS_STAGES)[number];
export type ProgressColor = (typeof PROGRESS_COLORS)[number];
export type ProgressColorPick = {
  palette: Palette;
  stage: ProgressStage;
  color: ProgressColor;
};

export type Appearance = {
  palette: Palette;
  mode: (typeof MODES)[number];
  accent: (typeof ACCENTS)[number] | null;
  /** Picks for every palette; a stage without one uses its palette's default. */
  progressColors: ProgressColorPick[];
};

/** The CSS variable a stage draws with, and the swatch a colour resolves to. */
export const stageVariable = (stage: ProgressStage) =>
  `--wv-stage-${stage.toLowerCase()}`;
export const swatchVariable = (color: ProgressColor) =>
  `--wv-swatch-${color.toLowerCase()}`;

/** The pick list after setting (or, with null, clearing) one palette's stages. */
export function withProgressColors(
  picks: ProgressColorPick[],
  palette: Palette,
  changes: { stage: ProgressStage; color: ProgressColor | null }[],
): ProgressColorPick[] {
  const changed = new Set(changes.map(({ stage }) => stage));
  const kept = picks.filter(
    (pick) => pick.palette !== palette || !changed.has(pick.stage),
  );
  const added = changes.flatMap(({ stage, color }) =>
    color ? [{ palette, stage, color }] : [],
  );
  return [...kept, ...added];
}

export function syncBrowserThemeColor() {
  const meta = document.querySelector<HTMLMetaElement>(
    'meta[name="theme-color"]',
  );
  if (meta) meta.content = getComputedStyle(document.body).backgroundColor;
}

type AppearanceState = {
  value: Appearance;
  saving: boolean;
  error: string | null;
};
const defaults: Appearance = {
  palette: "WEAVER",
  mode: "SYSTEM",
  accent: null,
  progressColors: [],
};
const listeners = new Set<() => void>();
let state: AppearanceState = { value: defaults, saving: false, error: null };

export const appearanceSnapshot = () => state;
export function subscribeAppearance(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

function render(stateNext: AppearanceState) {
  state = stateNext;
  const value = state.value;
  const { dataset } = document.documentElement;
  dataset.palette = value.palette;
  dataset.mode = value.mode;
  dataset.accent = value.accent ?? "";
  const { style } = document.documentElement;
  for (const stage of PROGRESS_STAGES) {
    const pick = value.progressColors.find(
      (entry) => entry.palette === value.palette && entry.stage === stage,
    );
    if (pick) {
      style.setProperty(
        stageVariable(stage),
        `var(${swatchVariable(pick.color)})`,
      );
    } else {
      style.removeProperty(stageVariable(stage));
    }
  }
  syncBrowserThemeColor();
  for (const listener of listeners) listener();
}

export function applyAppearance(value: Appearance) {
  render({ value, saving: false, error: null });
}

// The served shell carries palette, mode and accent. Progress picks arrive
// with the auth status, which is read before any job is drawn.
export function readEntryAppearance(): Appearance {
  const { palette, mode, accent } = document.documentElement.dataset;
  return {
    palette: PALETTES.find((entry) => entry === palette) ?? defaults.palette,
    mode: MODES.find((entry) => entry === mode) ?? defaults.mode,
    accent: ACCENTS.find((entry) => entry === accent) ?? null,
    progressColors: [],
  };
}

async function rememberOfflineAppearance(value: Appearance) {
  try {
    const cache = await window.caches.open(OFFLINE_APPEARANCE_CACHE);
    await cache.put(
      new URL(OFFLINE_APPEARANCE_PATH, document.baseURI),
      Response.json({ appearance: value, savedAt: Date.now() }),
    );
  } catch {
    // Browsers with storage disabled still use the daemon's confirmed setting.
  }
}

// The preview belongs to the document so navigating away while it saves
// still commits or restores the same appearance for the entire shell.
export async function saveAppearance(
  patch: Partial<Appearance>,
  persist: () => Promise<Appearance>,
  failure: string,
) {
  if (state.saving) return;
  const previous = state.value;
  render({ value: { ...previous, ...patch }, saving: true, error: null });
  try {
    const saved = await persist();
    await rememberOfflineAppearance(saved);
    applyAppearance(saved);
  } catch (error) {
    render({
      value: previous,
      saving: false,
      error: error instanceof Error ? error.message : failure,
    });
  }
}
