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
  PROGRESS_COLORS,
  PROGRESS_STAGES,
  appearanceSnapshot,
  saveAppearance,
  stageVariable,
  subscribeAppearance,
  swatchVariable,
  withProgressColors,
  type Appearance,
  type ProgressColor,
  type ProgressStage,
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
  color: {
    base: "size-6 justify-center border",
    selected: "border-wv-fg bg-wv-row-picked",
    idle: "border-transparent hover:border-wv-control hover:bg-wv-row-hover",
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
  const choose = (
    patch: Partial<Appearance>,
    input: Record<string, unknown> = patch,
  ) => {
    const failure = t("next.settings.saveFailed");
    void saveAppearance(
      patch,
      async () => {
        const answer = await update({ input });
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
        "w-full min-w-0",
        compact ? "space-y-2" : "flex flex-wrap items-start gap-x-10 gap-y-5",
      )}
    >
      {/* In Settings the progress colours sit beside the heading and these,
          below once the width cannot hold both. */}
      <div
        className={
          compact ? "contents" : "min-w-0 max-w-2xl flex-[1_1_34rem] space-y-3"
        }
      >
        {!compact && (
          <div className="text-[13.5px] font-medium tracking-[-0.005em] text-wv-fg">
            {t("next.appearance.title")}
          </div>
        )}
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
      </div>
      {!compact && (
        <ProgressColors
          value={value}
          idPrefix={idPrefix}
          onChange={(changes) =>
            choose(
              {
                progressColors: withProgressColors(
                  value.progressColors,
                  value.palette,
                  changes,
                ),
              },
              {
                progressColors: changes.map((change) => ({
                  palette: value.palette,
                  ...change,
                })),
              },
            )
          }
        />
      )}
      {error && (
        <p role="alert" className="basis-full text-xs text-wv-error-text">
          {error}
        </p>
      )}
    </fieldset>
  );
}

type ProgressChange = { stage: ProgressStage; color: ProgressColor | null };

/**
 * The palette in use's colour for each job stage. Picks are kept per palette,
 * so this always edits the palette that is showing. Only Settings shows it.
 */
function ProgressColors({
  value,
  idPrefix,
  onChange,
}: {
  value: Appearance;
  idPrefix: string;
  onChange: (changes: ProgressChange[]) => void;
}) {
  const t = useTranslate();
  const picked = new Map(
    value.progressColors
      .filter((pick) => pick.palette === value.palette)
      .map((pick) => [pick.stage, pick.color]),
  );
  return (
    <div
      role="group"
      aria-label={t("next.appearance.progress")}
      className="min-w-0 flex-[1_1_31rem] space-y-1.5"
    >
      <div className="flex justify-end">
        <button
          type="button"
          disabled={picked.size === 0}
          onClick={() =>
            onChange(PROGRESS_STAGES.map((stage) => ({ stage, color: null })))
          }
          data-testid={`${idPrefix}-progress-reset`}
          className="cursor-pointer text-xs text-wv-secondary underline-offset-2 hover:text-wv-fg hover:underline disabled:cursor-default disabled:text-wv-disabled disabled:no-underline"
        >
          {t("next.appearance.resetProgress")}
        </button>
      </div>
      <div className="space-y-1.5">
        {PROGRESS_STAGES.map((stage) => {
          const stageLabel = t(`next.appearance.stage.${stage.toLowerCase()}`);
          const current = picked.get(stage) ?? null;
          return (
            <div
              key={stage}
              role="group"
              aria-label={stageLabel}
              data-testid={`${idPrefix}-progress-${stage}`}
              className="flex flex-wrap items-center gap-x-3 gap-y-1"
            >
              <span className="flex w-36 min-w-0 items-center gap-2 text-xs text-wv-secondary">
                <span
                  aria-hidden="true"
                  className="h-1.5 w-6 shrink-0"
                  style={{ background: `var(${stageVariable(stage)})` }}
                />
                <span className="truncate">{stageLabel}</span>
              </span>
              <span className="flex flex-wrap gap-0.5">
                {[null, ...PROGRESS_COLORS].map((color) => {
                  const colorLabel = t(
                    `next.appearance.${(color ?? "default").toLowerCase()}`,
                  );
                  return (
                    <Choice
                      key={color ?? "default"}
                      selected={current === color}
                      onClick={() => onChange([{ stage, color }])}
                      variant="color"
                      label={colorLabel}
                      testId={`${idPrefix}-progress-${stage}-${color ?? "default"}`}
                    >
                      <span
                        aria-hidden="true"
                        className={cn(
                          "size-3.5 shrink-0",
                          color === null && "border-2",
                        )}
                        style={
                          color === null
                            ? {
                                borderColor: `var(${stageVariable(stage)}-default)`,
                              }
                            : { background: `var(${swatchVariable(color)})` }
                        }
                      />
                    </Choice>
                  );
                })}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
