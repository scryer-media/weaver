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

function Choice({
  selected,
  onClick,
  children,
  className,
  testId,
}: {
  selected: boolean;
  onClick: () => void;
  children: ReactNode;
  className?: string;
  testId: string;
}) {
  return (
    <button
      type="button"
      aria-pressed={selected}
      onClick={onClick}
      data-testid={testId}
      className={cn(
        "relative flex cursor-pointer items-center gap-2 border px-2 py-1 text-xs focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-wv-control-focus disabled:cursor-default",
        selected
          ? "border-wv-accent bg-wv-row-picked text-wv-fg"
          : "border-wv-control bg-wv-input text-wv-secondary hover:bg-wv-row-hover",
        className,
      )}
    >
      {children}
      {selected && (
        <Check
          size={13}
          aria-hidden="true"
          className="shrink-0 text-wv-accent"
        />
      )}
    </button>
  );
}

function QueuePreview({ palette, mode, accent }: Appearance) {
  return (
    <span
      aria-hidden="true"
      data-palette={palette}
      data-mode={mode}
      data-accent={accent ?? ""}
      className="wv-queue-preview"
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
}: {
  editable: boolean;
  idPrefix?: string;
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
  return (
    <fieldset
      disabled={!editable || saving}
      aria-label={t("next.appearance.title")}
      aria-busy={saving}
      className="w-full min-w-0 max-w-2xl space-y-3"
    >
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
        {PALETTES.map((palette) => (
          <Choice
            key={palette}
            selected={value.palette === palette}
            onClick={() => choose({ palette })}
            className="flex-wrap justify-start gap-x-1.5 p-2"
            testId={`${idPrefix}-palette-${palette}`}
          >
            <QueuePreview {...value} palette={palette} />
            {label(palette)}
          </Choice>
        ))}
      </div>
      {value.palette === "WEAVER" && (
        <div
          role="group"
          aria-label={label("mode")}
          className="flex flex-wrap gap-1"
        >
          {MODES.map((mode) => (
            <Choice
              key={mode}
              selected={value.mode === mode}
              onClick={() => choose({ mode })}
              testId={`${idPrefix}-mode-${mode}`}
            >
              {label(mode)}
            </Choice>
          ))}
        </div>
      )}
      <div
        role="group"
        aria-label={label("accent")}
        className="flex flex-wrap items-center gap-1"
      >
        <span className="mr-2 text-xs text-wv-muted">{label("accent")}</span>
        {[null, ...ACCENTS].map((accent) => (
          <Choice
            key={accent ?? "default"}
            selected={value.accent === accent}
            onClick={() => choose({ accent })}
            testId={`${idPrefix}-accent-${accent ?? "default"}`}
          >
            {accent && (
              <span
                aria-hidden="true"
                data-accent={accent}
                className="size-2 bg-[var(--wv-accent)]"
              />
            )}
            {label(accent ?? "default")}
          </Choice>
        ))}
      </div>
      {error && (
        <p role="alert" className="text-xs text-wv-error-text">
          {error}
        </p>
      )}
    </fieldset>
  );
}
