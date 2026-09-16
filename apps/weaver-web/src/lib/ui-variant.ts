/**
 * Which interface the browser renders.
 *
 * Weaver ships two complete, independent UIs: `next`, the hairline
 * three-region interface under `src/next` and the default, and `classic`, the
 * card-based app that came before it. They share the GraphQL layer and nothing else — no
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

/** A browser that never chose gets the new interface. */
export const DEFAULT_UI_VARIANT: UiVariant = "next";

function isUiVariant(value: string | null): value is UiVariant {
  return value === "classic" || value === "next";
}

/**
 * Where the choice is kept, most durable first. A browser that blocks local
 * storage often still allows session storage, which carries the choice through
 * the reload that applies it and for the rest of the tab's life.
 */
function stores(): (() => Storage)[] {
  return [() => window.localStorage, () => window.sessionStorage];
}

export function readUiVariant(): UiVariant {
  for (const store of stores()) {
    try {
      const stored = store().getItem(UI_VARIANT_STORAGE_KEY);
      if (isUiVariant(stored)) {
        return stored;
      }
    } catch {
      // Private browsing and blocked site data both throw on access rather
      // than returning null; try the next store.
    }
  }
  return DEFAULT_UI_VARIANT;
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
 * Persist the variant and reload. Returns false, without reloading, when the
 * browser keeps nothing: the reload could only land on the current interface.
 *
 * A full reload rather than a re-render: the two trees own different global
 * state (theme class, body background, document title cadence, the classic
 * UI's PWA and toast providers), and unmounting one live UI to mount the other
 * leaves enough of that behind to be worth the 200ms.
 */
export function setUiVariant(variant: UiVariant): boolean {
  const [durable, session] = stores();
  let saved = false;
  try {
    durable().setItem(UI_VARIANT_STORAGE_KEY, variant);
    saved = true;
    try {
      session().removeItem(UI_VARIANT_STORAGE_KEY);
    } catch {
      // Nothing was kept there if it cannot be reached.
    }
  } catch {
    try {
      session().setItem(UI_VARIANT_STORAGE_KEY, variant);
      saved = true;
    } catch {
      // Neither store accepts it.
    }
  }
  if (!saved) {
    // A reload would come straight back to the interface already showing.
    return false;
  }
  applyUiVariant(variant);
  window.location.reload();
  return true;
}
