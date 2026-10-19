// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import type { Translate } from "@/lib/context/translate-context";
import { NumberField, Toggle } from "../../components/controls";
import { LimitControl } from "../../features/networking/limits";
import type { FieldSpec } from "./framework";

/**
 * A seeding policy in an editor. The global policy and a category's own one
 * are edited with these same fields; a category's goal left unset falls
 * through to the global one, so there an unset goal reads as inherited, and
 * in the global policy as no goal.
 */

export type SeedingMode = "ANY" | "ALL";
export type SeedingAction = "STOP" | "REMOVE" | "REMOVE_WITH_DATA";

/** When a torrent has seeded enough, and what is done with it then. Null goals are not set at this level. */
export interface SeedingPolicy {
  ratioGoal: number | null;
  seedTimeGoalMinutes: number | null;
  inactiveGoalMinutes: number | null;
  mode: SeedingMode;
  action: SeedingAction;
  /** Each torrent's upload cap, in bytes per second. */
  maxUploadBps: number | null;
}

export const EMPTY_SEEDING: SeedingPolicy = {
  ratioGoal: null,
  seedTimeGoalMinutes: null,
  inactiveGoalMinutes: null,
  mode: "ANY",
  action: "STOP",
  maxUploadBps: null,
};

/** Whether the policy sets no goal and no cap. */
export const seedingIsEmpty = (policy: SeedingPolicy) =>
  policy.ratioGoal === null &&
  policy.seedTimeGoalMinutes === null &&
  policy.inactiveGoalMinutes === null &&
  policy.maxUploadBps === null;

/** The policy as `SeedingPolicyInput` takes it. */
export function seedingInput(policy: SeedingPolicy): SeedingPolicy {
  return {
    ratioGoal: policy.ratioGoal === null ? null : Math.max(0, policy.ratioGoal),
    seedTimeGoalMinutes: policy.seedTimeGoalMinutes === null ? null : Math.max(0, Math.round(policy.seedTimeGoalMinutes)),
    inactiveGoalMinutes: policy.inactiveGoalMinutes === null ? null : Math.max(0, Math.round(policy.inactiveGoalMinutes)),
    mode: policy.mode,
    action: policy.action,
    maxUploadBps: policy.maxUploadBps,
  };
}

/** A policy as the daemon sent it, with its enums defaulted for an older reply. */
export function seedingFrom(policy: Partial<SeedingPolicy> | null | undefined): SeedingPolicy {
  return { ...EMPTY_SEEDING, ...(policy ?? {}) };
}

/**
 * A goal that is set or not: the number while it is set, then its switch,
 * then, while it is off, what off means here. A goal switched off comes back
 * as it was when it is switched on again.
 */
export function GoalControl({
  label,
  value,
  onChange,
  fallback,
  offLabel,
  suffix,
  precision = 0,
  step = 1,
}: {
  label: string;
  value: number | null;
  onChange: (next: number | null) => void;
  /** The value a goal switched on starts from when it has none yet. */
  fallback: number;
  /** What off means, shown beside the switch while it is off. */
  offLabel?: string;
  suffix?: string;
  precision?: number;
  step?: number;
}) {
  const [last, setLast] = useState(value ?? fallback);
  return (
    <div className="flex items-center gap-[10px]">
      {value !== null ? (
        <NumberField
          label={label}
          value={value}
          min={0}
          step={step}
          precision={precision}
          suffix={suffix}
          onChange={(next) => {
            const clamped = Math.max(0, next);
            setLast(clamped);
            onChange(clamped);
          }}
        />
      ) : null}
      <Toggle checked={value !== null} label={label} onChange={(on) => onChange(on ? last : null)} />
      {value === null && offLabel ? <span className="font-wv-mono text-[11.5px] text-wv-muted">{offLabel}</span> : null}
    </div>
  );
}

/** Labels are translation keys, resolved when the fields render. */
const MODES: { value: SeedingMode; label: string }[] = [
  { value: "ANY", label: "next.seeding.modeAny" },
  { value: "ALL", label: "next.seeding.modeAll" },
];

const ACTIONS: { value: SeedingAction; label: string }[] = [
  { value: "STOP", label: "next.seeding.actionStop" },
  { value: "REMOVE", label: "next.seeding.actionRemove" },
  { value: "REMOVE_WITH_DATA", label: "next.seeding.actionRemoveWithData" },
];

/**
 * The policy's fields: its three goals, whether any or all of them end
 * seeding, what is done then, and each torrent's upload cap. `inherits` is
 * true for a category's own policy, whose unset goals fall through.
 */
export function seedingFields(
  t: Translate,
  policy: SeedingPolicy,
  onChange: (next: SeedingPolicy) => void,
  options: { inherits: boolean; idPrefix?: string },
): FieldSpec[] {
  const prefix = options.idPrefix ?? "seeding";
  const offLabel = t(options.inherits ? "next.seeding.inherits" : "next.seeding.noGoal");
  const patch = (next: Partial<SeedingPolicy>) => onChange({ ...policy, ...next });
  const goal = (
    id: "ratioGoal" | "seedTimeGoalMinutes" | "inactiveGoalMinutes",
    label: string,
    extra: { fallback: number; suffix?: string; precision?: number; step?: number },
  ): FieldSpec => ({
    id: `${prefix}-${id}`,
    label,
    keywords: "seed seeding goal",
    control: {
      kind: "custom",
      control: (
        <GoalControl
          label={label}
          value={policy[id]}
          offLabel={offLabel}
          onChange={(next) => patch({ [id]: next })}
          {...extra}
        />
      ),
    },
  });
  return [
    goal("ratioGoal", t("next.seeding.ratioGoal"), { fallback: 1, precision: 2, step: 0.1 }),
    goal("seedTimeGoalMinutes", t("next.seeding.seedTimeGoal"), { fallback: 1440, suffix: t("next.seeding.minutes") }),
    goal("inactiveGoalMinutes", t("next.seeding.inactiveGoal"), { fallback: 60, suffix: t("next.seeding.minutes") }),
    {
      id: `${prefix}-mode`,
      label: t("next.seeding.mode"),
      control: {
        kind: "segmented",
        value: policy.mode,
        options: MODES.map((option) => ({ ...option, label: t(option.label) })),
        onChange: (mode) => patch({ mode: mode as SeedingMode }),
      },
    },
    {
      id: `${prefix}-action`,
      label: t("next.seeding.action"),
      control: {
        kind: "select",
        value: policy.action,
        options: ACTIONS.map((option) => ({ ...option, label: t(option.label) })),
        onChange: (action) => patch({ action: action as SeedingAction }),
      },
    },
    {
      id: `${prefix}-maxUploadBps`,
      label: t("next.seeding.uploadCap"),
      keywords: "upload speed limit rate",
      control: {
        kind: "custom",
        control: (
          <div className="flex items-center gap-[10px]">
            <LimitControl
              label={t("next.seeding.uploadCap")}
              value={policy.maxUploadBps}
              suffix="MB/s"
              onChange={(maxUploadBps) => patch({ maxUploadBps })}
            />
            {options.inherits && policy.maxUploadBps === null ? (
              <span className="font-wv-mono text-[11.5px] text-wv-muted">{offLabel}</span>
            ) : null}
          </div>
        ),
      },
    },
  ];
}
