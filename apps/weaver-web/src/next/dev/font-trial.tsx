import { useState } from "react";
import { cn } from "@/lib/utils";

/**
 * A font trial for the dev server: a picker that sets `--font-wv-ui` to any
 * face in `FACES`, so candidates can be compared on real screens.
 *
 * `NextApp` loads it behind `import.meta.env.DEV`, so a production build
 * carries none of it. The choice is an inline override on `<html>`, and picking
 * the committed face removes the override rather than restating it.
 *
 * Each tab keeps its own choice across reloads, a new tab starts from the last
 * trial left on, and `?font=<id>` sets a tab's choice from its URL — so two
 * windows can hold two faces side by side.
 *
 * A candidate no stylesheet declares lists its files: install its fontsource
 * package as a dev dependency and import the latin woff2 with `?url`. Those
 * are registered as `FontFace`s, which, like `@font-face` rules, are fetched
 * only once text is set in them.
 */

interface TrialFile {
  url: string;
  /** One weight for a static file, or the `min max` axis of a variable one. */
  weight: string;
}

interface TrialFace {
  /** What `?font=` takes and what storage holds. */
  id: string;
  label: string;
  family: string;
  files?: readonly TrialFile[];
}

/** The face `theme.css` commits to. */
const COMMITTED: TrialFace = { id: "fira-code", label: "Fira Code", family: "Fira Code Variable" };

const FACES: readonly TrialFace[] = [
  COMMITTED,
  // Classic's mono face, so the top-level `fonts.css` already declares it.
  { id: "jetbrains-mono", label: "JetBrains Mono", family: "JetBrains Mono Variable" },
];

/** The token's own fallbacks, as `theme.css` lists them. */
const FALLBACKS = 'ui-monospace, "SFMono-Regular", Menlo, monospace';

/** The latin subset `next/fonts.css` cuts every face to. */
const LATIN =
  "U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, " +
  "U+0329, U+2000-206F, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD";

const STORAGE_KEY = "weaver.font-trial";

for (const face of FACES) {
  for (const file of face.files ?? []) {
    document.fonts.add(
      new FontFace(face.family, `url("${file.url}")`, {
        weight: file.weight,
        display: "swap",
        unicodeRange: LATIN,
      }),
    );
  }
}

function stack(face: TrialFace): string {
  return `"${face.family}", ${FALLBACKS}`;
}

function findFace(id: string | null): TrialFace | undefined {
  return FACES.find((face) => face.id === id);
}

// Getters, because reaching for storage at all throws when site data is blocked.
const session = () => window.sessionStorage;
const local = () => window.localStorage;

function readChoice(storage: () => Storage): TrialFace | undefined {
  try {
    return findFace(storage().getItem(STORAGE_KEY));
  } catch {
    return undefined;
  }
}

function writeChoice(storage: () => Storage, face: TrialFace | null): void {
  try {
    if (face) {
      storage().setItem(STORAGE_KEY, face.id);
    } else {
      storage().removeItem(STORAGE_KEY);
    }
  } catch {
    // The choice still holds until the page reloads.
  }
}

function applyChoice(face: TrialFace): void {
  const style = document.documentElement.style;
  if (face.id === COMMITTED.id) {
    style.removeProperty("--font-wv-ui");
  } else {
    style.setProperty("--font-wv-ui", stack(face));
  }
}

function initialChoice(): TrialFace {
  const fromUrl = findFace(new URLSearchParams(window.location.search).get("font"));
  if (fromUrl) {
    writeChoice(session, fromUrl);
  }
  return fromUrl ?? readChoice(session) ?? readChoice(local) ?? COMMITTED;
}

// Applied as the module loads, before the picker first renders, so a reload
// paints straight into the chosen face.
const initial = initialChoice();
applyChoice(initial);

export default function FontTrial() {
  // By id, so the selection survives this module being hot-replaced.
  const [activeId, setActiveId] = useState(initial.id);
  const [open, setOpen] = useState(false);
  const active = findFace(activeId) ?? COMMITTED;

  const choose = (face: TrialFace) => {
    applyChoice(face);
    writeChoice(session, face);
    // A new tab opens on the committed face unless a trial was left on.
    writeChoice(local, face.id === COMMITTED.id ? null : face);
    setActiveId(face.id);
  };

  return (
    <div className="fixed right-3 bottom-[46px] z-30 flex flex-col items-end gap-1">
      {open ? (
        <div
          role="group"
          aria-label="Font trial"
          className="flex min-w-[220px] flex-col border border-wv-control bg-wv-chrome shadow-wv-menu"
        >
          {FACES.map((face) => (
            <button
              key={face.id}
              type="button"
              aria-pressed={face.id === active.id}
              onClick={() => choose(face)}
              style={{ fontFamily: stack(face) }}
              className={cn(
                "flex cursor-pointer items-baseline justify-between gap-5 px-3 py-[9px] text-left text-[12.5px] hover:bg-wv-menu-hover",
                face.id === active.id ? "bg-wv-selected text-wv-strong" : "text-wv-secondary",
              )}
            >
              {face.label}
              {face.id === COMMITTED.id ? (
                <span className="text-[11px] text-wv-muted">committed</span>
              ) : null}
            </button>
          ))}
        </div>
      ) : null}
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
        className="cursor-pointer border border-wv-control bg-wv-chrome px-2.5 py-1 text-[11px] text-wv-muted shadow-wv-menu hover:text-wv-fg"
      >
        Font: {active.label}
      </button>
    </div>
  );
}
