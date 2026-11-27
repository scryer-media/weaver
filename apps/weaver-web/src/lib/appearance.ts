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

export type Appearance = {
  palette: (typeof PALETTES)[number];
  mode: (typeof MODES)[number];
  accent: (typeof ACCENTS)[number] | null;
};

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
  syncBrowserThemeColor();
  for (const listener of listeners) listener();
}

export function applyAppearance(value: Appearance) {
  render({ value, saving: false, error: null });
}

export function readEntryAppearance() {
  const { palette, mode, accent } = document.documentElement.dataset;
  return {
    palette: PALETTES.find((entry) => entry === palette) ?? defaults.palette,
    mode: MODES.find((entry) => entry === mode) ?? defaults.mode,
    accent: ACCENTS.find((entry) => entry === accent) ?? null,
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
