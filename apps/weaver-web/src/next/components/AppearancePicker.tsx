// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { Check } from "lucide-react";
import { useSyncExternalStore, type ReactNode } from "react";
import { useMutation } from "urql";
import { UPDATE_WEB_APPEARANCE_MUTATION } from "@/graphql/queries";
import {
  ACCENTS,
  MODES,
  PALETTES,
  appearanceSnapshot,
  saveAppearance,
  subscribeAppearance,
  type Appearance,
} from "@/lib/appearance";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";

const CHOICE_STYLES = {
  tile: {
    base: "gap-2 border px-2 py-1 text-xs focus-visible:outline-offset-2",
    selected: "border-wv-accent bg-wv-row-picked text-wv-fg",
    idle: "border-wv-control bg-wv-input text-wv-secondary hover:bg-wv-row-hover",
  },
  row: {
    base: "min-h-7 min-w-0 gap-2 px-1.5 text-left text-xs",
    selected: "bg-[var(--wv-rail-selection)] font-medium text-wv-fg",
    idle: "text-wv-secondary hover:bg-wv-row-hover",
  },
  segment: {
    base: "min-h-7 min-w-0 justify-center border-b px-1 text-[11px]",
    selected:
      "border-[color:var(--wv-rail-accent)] bg-[var(--wv-rail-selection)] font-medium text-wv-fg",
    idle: "border-transparent text-wv-secondary hover:bg-wv-row-hover",
  },
  swatch: {
    base: "h-7 min-w-0 justify-center border",
    selected:
      "border-[color:var(--wv-rail-accent)] bg-[var(--wv-rail-selection)]",
    idle: "border-transparent hover:border-[color:var(--wv-rail-frame)] hover:bg-wv-row-hover",
  },
} as const;

function Choice({
  selected,
  onClick,
  children,
  className,
  testId,
  variant = "tile",
  label,
}: {
  selected: boolean;
  onClick: () => void;
  children: ReactNode;
  className?: string;
  testId: string;
  variant?: keyof typeof CHOICE_STYLES;
  label?: string;
}) {
  const styles = CHOICE_STYLES[variant];
  return (
    <button
      type="button"
      aria-pressed={selected}
      aria-label={label}
      title={label}
      onClick={onClick}
      data-testid={testId}
      className={cn(
        "relative flex cursor-pointer items-center focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-wv-control-focus disabled:cursor-default",
        styles.base,
        selected ? styles.selected : styles.idle,
        className,
      )}
    >
      {children}
      {selected && (variant === "tile" || variant === "row") && (
        <Check
          size={13}
          aria-hidden="true"
          className={cn(
            "shrink-0",
            variant === "row"
              ? "ml-auto text-[color:var(--wv-rail-accent)]"
              : "text-wv-accent",
          )}
        />
      )}
    </button>
  );
}

function QueuePreview({
  palette,
  mode,
  accent,
  compact,
}: Appearance & { compact: boolean }) {
  return (
    <span
      aria-hidden="true"
      data-palette={palette}
      data-mode={mode}
      data-accent={accent ?? ""}
      className={cn("wv-queue-preview", compact && "wv-queue-preview-compact")}
    >
      <span className="wv-preview-rail">
        <i />
        <i />
        <i />
      </span>
      <span className="wv-preview-list">
        <i />
        <i />
        <i />
      </span>
    </span>
  );
}

export function AppearancePicker({
  editable,
  idPrefix = "appearance",
  compact = false,
}: {
  editable: boolean;
  idPrefix?: string;
  compact?: boolean;
}) {
  const t = useTranslate();
  const { value, saving, error } = useSyncExternalStore(
    subscribeAppearance,
    appearanceSnapshot,
  );
  const [, update] = useMutation<{ updateWebAppearance: Appearance }>(
    UPDATE_WEB_APPEARANCE_MUTATION,
  );
  const choose = (patch: Partial<Appearance>) => {
    const failure = t("next.settings.saveFailed");
    void saveAppearance(
      patch,
      async () => {
        const answer = await update({ input: patch });
        if (answer.error || !answer.data?.updateWebAppearance) {
          throw new Error(answer.error?.graphQLErrors[0]?.message ?? failure);
        }
        return answer.data.updateWebAppearance;
      },
      failure,
    );
  };
  const label = (name: string) => t(`next.appearance.${name.toLowerCase()}`);
  // The store blocks overlapping saves. Keep the focused choice mounted and
  // focused in the compact popover while a save is pending.
  return (
    <fieldset
      disabled={!editable || (saving && !compact)}
      aria-disabled={compact && saving ? true : undefined}
      aria-label={t("next.appearance.title")}
      aria-busy={saving}
      className={cn(
        "w-full min-w-0 max-w-2xl",
        compact ? "space-y-2" : "space-y-3",
      )}
    >
      <div
        className={compact ? "grid" : "grid grid-cols-2 gap-2 sm:grid-cols-4"}
      >
        {PALETTES.map((palette) => (
          <Choice
            key={palette}
            selected={value.palette === palette}
            onClick={() => choose({ palette })}
            variant={compact ? "row" : "tile"}
            className={
              compact ? undefined : "flex-wrap justify-start gap-x-1.5 p-2"
            }
            testId={`${idPrefix}-palette-${palette}`}
          >
            <QueuePreview {...value} palette={palette} compact={compact} />
            <span className="min-w-0 truncate">{label(palette)}</span>
          </Choice>
        ))}
      </div>
      {value.palette === "WEAVER" && (
        <div
          role="group"
          aria-label={label("mode")}
          className={
            compact
              ? "grid grid-cols-3 gap-px border-t border-wv-hairline pt-2"
              : "flex flex-wrap gap-1"
          }
        >
          {compact && (
            <span className="col-span-full mb-1 px-1.5 text-[11px] text-wv-muted">
              {label("mode")}
            </span>
          )}
          {MODES.map((mode) => (
            <Choice
              key={mode}
              selected={value.mode === mode}
              onClick={() => choose({ mode })}
              variant={compact ? "segment" : "tile"}
              testId={`${idPrefix}-mode-${mode}`}
            >
              <span className="min-w-0 truncate">{label(mode)}</span>
            </Choice>
          ))}
        </div>
      )}
      <div
        role="group"
        aria-label={label("accent")}
        className={
          compact
            ? "grid grid-cols-7 gap-x-0.5 gap-y-1 border-t border-wv-hairline pt-2"
            : "flex flex-wrap items-center gap-1"
        }
      >
        <div
          className={
            compact
              ? "col-span-full flex items-baseline justify-between gap-2 px-1.5 text-[11px]"
              : "contents"
          }
        >
          <span className={cn("text-wv-muted", !compact && "mr-2 text-xs")}>
            {label("accent")}
          </span>
          {compact && (
            <span
              data-testid={`${idPrefix}-selected-accent`}
              className="min-w-0 text-right text-wv-fg"
            >
              {label(value.accent ?? "default")}
            </span>
          )}
        </div>
        {[null, ...ACCENTS].map((accent) => {
          const accentKey = accent ?? "default";
          const accentLabel = label(accentKey);
          return (
            <Choice
              key={accentKey}
              selected={value.accent === accent}
              onClick={() => choose({ accent })}
              variant={compact ? "swatch" : "tile"}
              label={compact ? accentLabel : undefined}
              testId={`${idPrefix}-accent-${accentKey}`}
            >
              {(compact || accent) && (
                <span
                  aria-hidden="true"
                  data-accent={accent ?? ""}
                  data-palette={accent === null ? value.palette : undefined}
                  data-mode={accent === null ? value.mode : undefined}
                  className={cn(
                    "shrink-0",
                    compact ? "wv-rail-swatch size-3" : "size-2",
                    accent === null
                      ? "border border-[color:var(--wv-rail-accent,var(--wv-accent))]"
                      : "bg-[var(--wv-rail-accent,var(--wv-accent))]",
                  )}
                />
              )}
              {!compact && accentLabel}
            </Choice>
          );
        })}
      </div>
      {error && (
        <p role="alert" className="text-xs text-wv-error-text">
          {error}
        </p>
      )}
    </fieldset>
  );
}
