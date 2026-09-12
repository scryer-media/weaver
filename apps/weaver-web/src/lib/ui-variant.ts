/**
 * Which interface the browser renders.
 *
 * Weaver ships two complete, independent UIs: `classic`, the card-based app
 * that has always been here, and `next`, the hairline three-region redesign
 * under `src/next`. They share the GraphQL layer and nothing else — no
 * layout, no tokens, no typefaces — so the choice is made once, above the
 * router, and the losing tree never mounts.
 *
 * The preference is per-browser rather than a server setting: it decides what
 * paints, so it has to be readable synchronously before the first query
 * resolves, and two people pointing at the same daemon can reasonably disagree.
 * `index.html` reads the same key to stamp `data-ui` before first paint, which
 * is what keeps the two background colours from flashing over each other.
 */

export type UiVariant = "classic" | "next";

export const UI_VARIANT_STORAGE_KEY = "weaver.ui-variant";

export const DEFAULT_UI_VARIANT: UiVariant = "classic";

function isUiVariant(value: string | null): value is UiVariant {
  return value === "classic" || value === "next";
}

export function readUiVariant(): UiVariant {
  try {
    const stored = window.localStorage.getItem(UI_VARIANT_STORAGE_KEY);
    return isUiVariant(stored) ? stored : DEFAULT_UI_VARIANT;
  } catch {
    // Private browsing and blocked site data both throw on access rather than
    // returning null; the default is the right answer in either case.
    return DEFAULT_UI_VARIANT;
  }
}

/** Mirror the variant onto `<html>` so the scoped CSS in `next/theme.css` applies. */
export function applyUiVariant(variant: UiVariant): void {
  const root = document.documentElement;
  if (variant === "next") {
    root.dataset.ui = "next";
  } else {
    delete root.dataset.ui;
  }
}

/**
 * Persist the variant and reload.
 *
 * A full reload rather than a re-render: the two trees own different global
 * state (theme class, body background, document title cadence, the classic
 * UI's PWA and toast providers), and unmounting one live UI to mount the other
 * leaves enough of that behind to be worth the 200ms.
 */
export function setUiVariant(variant: UiVariant): void {
  try {
    window.localStorage.setItem(UI_VARIANT_STORAGE_KEY, variant);
  } catch {
    // A browser that cannot store the choice still gets it for this session.
  }
  applyUiVariant(variant);
  window.location.reload();
}
