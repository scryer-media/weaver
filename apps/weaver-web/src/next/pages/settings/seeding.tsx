// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState, type ReactNode } from "react";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { NumberField, Toggle, UnitStrip } from "../../components/controls";
import { LimitControl } from "../../features/networking/limits";
import type { FieldSpec } from "./framework";
import { trimNumber } from "./quota";

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

/** The units a duration goal is typed in; it is kept in minutes. */
export type DurationUnit = "min" | "h" | "d";

const UNIT_MINUTES: Record<DurationUnit, number> = { min: 1, h: 60, d: 1440 };
const UNIT_KEYS: Record<DurationUnit, string> = {
  min: "next.seeding.minutes",
  h: "next.seeding.hours",
  d: "next.seeding.days",
};
const DURATION_UNITS: readonly DurationUnit[] = ["min", "h", "d"];

/** The largest unit the minutes divide into whole. */
export function durationUnit(minutes: number): DurationUnit {
  if (minutes > 0 && minutes % UNIT_MINUTES.d === 0) return "d";
  if (minutes > 0 && minutes % UNIT_MINUTES.h === 0) return "h";
  return "min";
}

/** Minutes as they read best: `90 min`, `2 h`, `1 d`. */
export function durationText(t: Translate, minutes: number): string {
  const unit = durationUnit(minutes);
  return `${trimNumber(minutes / UNIT_MINUTES[unit])} ${t(UNIT_KEYS[unit])}`;
}

/**
 * A duration goal: the number in the unit beside it, then its switch. The
 * unit starts as the one the goal divides into whole, and changing it
 * changes only how the same goal reads.
 */
export function DurationGoalControl({
  label,
  value,
  onChange,
  fallback,
  offLabel,
}: {
  label: string;
  /** Minutes, or null while the goal is off. */
  value: number | null;
  onChange: (next: number | null) => void;
  fallback: number;
  offLabel?: string;
}) {
  const t = useTranslate();
  const [last, setLast] = useState(value ?? fallback);
  const [unit, setUnit] = useState<DurationUnit>(() => durationUnit(value ?? fallback));
  return (
    <div className="flex items-center gap-[10px]">
      {value !== null ? (
        <>
          <NumberField
            label={label}
            value={value / UNIT_MINUTES[unit]}
            min={0}
            precision={2}
            onChange={(next) => {
              const minutes = Math.max(0, Math.round(next * UNIT_MINUTES[unit]));
              setLast(minutes);
              onChange(minutes);
            }}
          />
          <UnitStrip
            options={DURATION_UNITS.map((option) => ({ value: option, label: t(UNIT_KEYS[option]) }))}
            value={unit}
            onChange={setUnit}
          />
        </>
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
  const goal = (id: "ratioGoal" | "seedTimeGoalMinutes" | "inactiveGoalMinutes", label: string, control: ReactNode): FieldSpec => ({
    id: `${prefix}-${id}`,
    label,
    keywords: "seed seeding goal",
    control: { kind: "custom", control },
  });
  const ratioLabel = t("next.seeding.ratioGoal");
  const seedTimeLabel = t("next.seeding.seedTimeGoal");
  const inactiveLabel = t("next.seeding.inactiveGoal");
  return [
    goal(
      "ratioGoal",
      ratioLabel,
      <GoalControl
        label={ratioLabel}
        value={policy.ratioGoal}
        offLabel={offLabel}
        onChange={(ratioGoal) => patch({ ratioGoal })}
        fallback={1}
        precision={2}
        step={0.1}
      />,
    ),
    goal(
      "seedTimeGoalMinutes",
      seedTimeLabel,
      <DurationGoalControl
        label={seedTimeLabel}
        value={policy.seedTimeGoalMinutes}
        offLabel={offLabel}
        onChange={(seedTimeGoalMinutes) => patch({ seedTimeGoalMinutes })}
        fallback={1440}
      />,
    ),
    goal(
      "inactiveGoalMinutes",
      inactiveLabel,
      <DurationGoalControl
        label={inactiveLabel}
        value={policy.inactiveGoalMinutes}
        offLabel={offLabel}
        onChange={(inactiveGoalMinutes) => patch({ inactiveGoalMinutes })}
        fallback={60}
      />,
    ),
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
