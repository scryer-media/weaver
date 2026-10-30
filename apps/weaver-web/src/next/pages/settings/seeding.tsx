// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState, type ReactNode } from "react";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { NumberField, Segmented, Toggle, UnitStrip } from "../../components/controls";
import { LimitControl } from "../../features/networking/limits";
import type { FieldSpec } from "./framework";
import { trimNumber } from "./quota";

/**
 * Seeding terms in an editor. The global terms and those of a category, a
 * tracker rule or a torrent are edited with these same fields. Below the
 * global level each term inherits, is off, or is set; at the global level,
 * where nothing is above, a goal is off or set and the mode and action are
 * always set.
 */

export type SeedingMode = "ANY" | "ALL";
export type SeedingAction = "STOP" | "REMOVE" | "REMOVE_WITH_DATA";
/** Whether the seed-time goal counts all the seeding there has been, or only the current unbroken run. */
export type SeedTimeCounting = "CUMULATIVE" | "CONSECUTIVE";

/** How a level holds one setting. */
export type LeafState = "INHERIT" | "OFF" | "SET";

/** One setting at one level; `value` is there while it is set. */
export interface Leaf<T> {
  state: LeafState;
  value: T | null;
}

export const INHERIT: Leaf<never> = { state: "INHERIT", value: null };
export const OFF: Leaf<never> = { state: "OFF", value: null };
export const leafSet = <T,>(value: T): Leaf<T> => ({ state: "SET", value });

/** The value while the leaf is set here. */
export const leafValue = <T,>(leaf: Leaf<T>): T | null => (leaf.state === "SET" ? leaf.value : null);

/** A leaf as the daemon sent it, inheriting while it sent none. */
export const leafFrom = <T,>(leaf: Leaf<T> | null | undefined): Leaf<T> =>
  leaf ? { state: leaf.state, value: leaf.state === "SET" ? leaf.value : null } : INHERIT;

/** A leaf as a `*LeafInput` takes it: a value with `SET` and only then. */
export const leafInput = <T,>(leaf: Leaf<T>): { state: LeafState; value?: T | null } =>
  leaf.state === "SET" ? { state: "SET", value: leaf.value } : { state: leaf.state };

/** When a torrent has seeded enough, and what is done with it then, at one level. */
export interface SeedingPolicy {
  ratioGoal: Leaf<number>;
  seedTimeGoalMinutes: Leaf<number>;
  inactiveGoalMinutes: Leaf<number>;
  mode: Leaf<SeedingMode>;
  action: Leaf<SeedingAction>;
  seedTimeCounting: Leaf<SeedTimeCounting>;
}

/** Terms that inherit every one: a level holding them has none of its own. */
export const INHERITED_SEEDING: SeedingPolicy = {
  ratioGoal: INHERIT,
  seedTimeGoalMinutes: INHERIT,
  inactiveGoalMinutes: INHERIT,
  mode: INHERIT,
  action: INHERIT,
  seedTimeCounting: INHERIT,
};

/** The global terms before any is set: no goal, any goal met stops. */
export const GLOBAL_SEEDING: SeedingPolicy = {
  ratioGoal: OFF,
  seedTimeGoalMinutes: OFF,
  inactiveGoalMinutes: OFF,
  mode: leafSet("ANY"),
  action: leafSet("STOP"),
  seedTimeCounting: leafSet("CUMULATIVE"),
};

/** Whether every term inherits; such terms are kept as none. */
export const seedingInherits = (policy: SeedingPolicy) =>
  Object.values(policy).every((leaf) => leaf.state === "INHERIT");

const goalInput = (leaf: Leaf<number>, round: boolean): Leaf<number> =>
  leaf.state === "SET" && leaf.value !== null
    ? leafSet(Math.max(0, round ? Math.round(leaf.value) : leaf.value))
    : leaf;

/** The terms as `SeedingPolicyInput` takes them. */
export function seedingInput(policy: SeedingPolicy) {
  return {
    ratioGoal: leafInput(goalInput(policy.ratioGoal, false)),
    seedTimeGoalMinutes: leafInput(goalInput(policy.seedTimeGoalMinutes, true)),
    inactiveGoalMinutes: leafInput(goalInput(policy.inactiveGoalMinutes, true)),
    mode: leafInput(policy.mode),
    action: leafInput(policy.action),
    seedTimeCounting: leafInput(policy.seedTimeCounting),
  };
}

/** Terms as the daemon sent them; a term it left out inherits. */
export function seedingFrom(policy: Partial<SeedingPolicy> | null | undefined): SeedingPolicy {
  return {
    ratioGoal: leafFrom(policy?.ratioGoal),
    seedTimeGoalMinutes: leafFrom(policy?.seedTimeGoalMinutes),
    inactiveGoalMinutes: leafFrom(policy?.inactiveGoalMinutes),
    mode: leafFrom(policy?.mode),
    action: leafFrom(policy?.action),
    seedTimeCounting: leafFrom(policy?.seedTimeCounting),
  };
}

/** The terms at the global level: a goal that inherits is off, and the mode and action are set. */
export function seedingAtGlobal(policy: SeedingPolicy): SeedingPolicy {
  const goal = (leaf: Leaf<number>) => (leaf.state === "INHERIT" ? OFF : leaf);
  return {
    ratioGoal: goal(policy.ratioGoal),
    seedTimeGoalMinutes: goal(policy.seedTimeGoalMinutes),
    inactiveGoalMinutes: goal(policy.inactiveGoalMinutes),
    mode: policy.mode.state === "SET" ? policy.mode : GLOBAL_SEEDING.mode,
    action: policy.action.state === "SET" ? policy.action : GLOBAL_SEEDING.action,
    seedTimeCounting:
      policy.seedTimeCounting.state === "SET" ? policy.seedTimeCounting : GLOBAL_SEEDING.seedTimeCounting,
  };
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

const COUNTINGS: { value: SeedTimeCounting; label: string }[] = [
  { value: "CUMULATIVE", label: "next.seeding.countingCumulative" },
  { value: "CONSECUTIVE", label: "next.seeding.countingConsecutive" },
];

const ACTIONS: { value: SeedingAction; label: string }[] = [
  { value: "STOP", label: "next.seeding.actionStop" },
  { value: "REMOVE", label: "next.seeding.actionRemove" },
  { value: "REMOVE_WITH_DATA", label: "next.seeding.actionRemoveWithData" },
];

/**
 * A setting below the global level: inherited, off, or set, and while it is
 * set, its value in the control `input` draws. A value switched away from
 * comes back as it was when the setting is set again.
 */
export function LeafControl<T>({
  label,
  leaf,
  onChange,
  fallback,
  offLabel,
  input,
}: {
  label: string;
  leaf: Leaf<T>;
  onChange: (next: Leaf<T>) => void;
  /** The value a setting starts from when it is first set. */
  fallback: T;
  /** What off means here: no goal, unlimited. Null offers no off. */
  offLabel: string | null;
  input: (value: T, onValue: (next: T) => void) => ReactNode;
}) {
  const t = useTranslate();
  const [last, setLast] = useState<T>(leafValue(leaf) ?? fallback);
  const states: { value: LeafState; label: string }[] = [
    { value: "INHERIT", label: t("next.seeding.leafInherit") },
    ...(offLabel === null ? [] : [{ value: "OFF" as const, label: offLabel }]),
    { value: "SET", label: t("next.seeding.leafSet") },
  ];
  const value = leafValue(leaf);
  return (
    <div className="flex flex-wrap items-center gap-[10px]">
      <Segmented<LeafState>
        label={label}
        value={leaf.state}
        options={states}
        onChange={(state) => onChange(state === "SET" ? leafSet(last) : { state, value: null })}
      />
      {value !== null
        ? input(value, (next) => {
            setLast(next);
            onChange(leafSet(next));
          })
        : null}
    </div>
  );
}

/** A duration typed in the unit beside it, kept in minutes. */
function DurationField({ label, value, onChange }: { label: string; value: number; onChange: (next: number) => void }) {
  const t = useTranslate();
  const [unit, setUnit] = useState<DurationUnit>(() => durationUnit(value));
  return (
    <>
      <NumberField
        label={label}
        value={value / UNIT_MINUTES[unit]}
        min={0}
        precision={2}
        onChange={(next) => onChange(Math.max(0, Math.round(next * UNIT_MINUTES[unit])))}
      />
      <UnitStrip
        options={DURATION_UNITS.map((option) => ({ value: option, label: t(UNIT_KEYS[option]) }))}
        value={unit}
        onChange={setUnit}
      />
    </>
  );
}

const ratioField = (label: string) => (value: number, onValue: (next: number) => void) => (
  <NumberField label={label} value={value} min={0} step={0.1} precision={2} onChange={(next) => onValue(Math.max(0, next))} />
);

const durationField = (label: string) => (value: number, onValue: (next: number) => void) => (
  <DurationField label={label} value={value} onChange={onValue} />
);

/**
 * The terms' fields: their three goals, whether any or all of them end
 * seeding, and what is done then. `inherits` is true below the global level,
 * where each term can inherit.
 */
export function seedingFields(
  t: Translate,
  policy: SeedingPolicy,
  onChange: (next: SeedingPolicy) => void,
  options: { inherits: boolean; idPrefix?: string },
): FieldSpec[] {
  const prefix = options.idPrefix ?? "seeding";
  const noGoal = t("next.seeding.noGoal");
  const patch = (next: Partial<SeedingPolicy>) => onChange({ ...policy, ...next });
  const goal = (id: "ratioGoal" | "seedTimeGoalMinutes" | "inactiveGoalMinutes", label: string, control: ReactNode): FieldSpec => ({
    id: `${prefix}-${id}`,
    label,
    keywords: "seed seeding goal",
    control: { kind: "custom", control },
  });
  // At the global level a goal is on or off.
  const globalGoal = (leaf: Leaf<number>) => (leaf.state === "SET" ? leaf.value : null);
  const fromGlobal = (value: number | null): Leaf<number> => (value === null ? OFF : leafSet(value));
  const ratioLabel = t("next.seeding.ratioGoal");
  const seedTimeLabel = t("next.seeding.seedTimeGoal");
  const inactiveLabel = t("next.seeding.inactiveGoal");
  const inherit = { value: INHERIT_CHOICE, label: t("next.seeding.leafInherit") };
  const choice = <T extends string>(leaf: Leaf<T>, fallback: T) =>
    leaf.state === "SET" && leaf.value !== null ? leaf.value : options.inherits ? INHERIT_CHOICE : fallback;
  const fromChoice = <T extends string>(value: string): Leaf<T> => (value === INHERIT_CHOICE ? INHERIT : leafSet(value as T));
  return [
    goal(
      "ratioGoal",
      ratioLabel,
      options.inherits ? (
        <LeafControl
          label={ratioLabel}
          leaf={policy.ratioGoal}
          offLabel={noGoal}
          fallback={1}
          onChange={(ratioGoal) => patch({ ratioGoal })}
          input={ratioField(ratioLabel)}
        />
      ) : (
        <GoalControl
          label={ratioLabel}
          value={globalGoal(policy.ratioGoal)}
          offLabel={noGoal}
          onChange={(ratioGoal) => patch({ ratioGoal: fromGlobal(ratioGoal) })}
          fallback={1}
          precision={2}
          step={0.1}
        />
      ),
    ),
    goal(
      "seedTimeGoalMinutes",
      seedTimeLabel,
      options.inherits ? (
        <LeafControl
          label={seedTimeLabel}
          leaf={policy.seedTimeGoalMinutes}
          offLabel={noGoal}
          fallback={1440}
          onChange={(seedTimeGoalMinutes) => patch({ seedTimeGoalMinutes })}
          input={durationField(seedTimeLabel)}
        />
      ) : (
        <DurationGoalControl
          label={seedTimeLabel}
          value={globalGoal(policy.seedTimeGoalMinutes)}
          offLabel={noGoal}
          onChange={(minutes) => patch({ seedTimeGoalMinutes: fromGlobal(minutes) })}
          fallback={1440}
        />
      ),
    ),
    goal(
      "inactiveGoalMinutes",
      inactiveLabel,
      options.inherits ? (
        <LeafControl
          label={inactiveLabel}
          leaf={policy.inactiveGoalMinutes}
          offLabel={noGoal}
          fallback={60}
          onChange={(inactiveGoalMinutes) => patch({ inactiveGoalMinutes })}
          input={durationField(inactiveLabel)}
        />
      ) : (
        <DurationGoalControl
          label={inactiveLabel}
          value={globalGoal(policy.inactiveGoalMinutes)}
          offLabel={noGoal}
          onChange={(minutes) => patch({ inactiveGoalMinutes: fromGlobal(minutes) })}
          fallback={60}
        />
      ),
    ),
    {
      id: `${prefix}-mode`,
      label: t("next.seeding.mode"),
      control: {
        kind: "segmented",
        value: choice(policy.mode, "ANY"),
        options: [
          ...(options.inherits ? [inherit] : []),
          ...MODES.map((option) => ({ ...option, label: t(option.label) })),
        ],
        onChange: (mode) => patch({ mode: fromChoice<SeedingMode>(mode) }),
      },
    },
    {
      id: `${prefix}-seedTimeCounting`,
      label: t("next.seeding.seedTimeCounting"),
      keywords: "seed seeding time run",
      control: {
        kind: "segmented",
        value: choice(policy.seedTimeCounting, "CUMULATIVE"),
        options: [
          ...(options.inherits ? [inherit] : []),
          ...COUNTINGS.map((option) => ({ ...option, label: t(option.label) })),
        ],
        onChange: (counting) => patch({ seedTimeCounting: fromChoice<SeedTimeCounting>(counting) }),
      },
    },
    {
      id: `${prefix}-action`,
      label: t("next.seeding.action"),
      control: {
        kind: "select",
        value: choice(policy.action, "STOP"),
        options: [
          ...(options.inherits ? [inherit] : []),
          ...ACTIONS.map((option) => ({ ...option, label: t(option.label) })),
        ],
        onChange: (action) => patch({ action: fromChoice<SeedingAction>(action) }),
      },
    },
  ];
}

// The mode or action choice that inherits.
const INHERIT_CHOICE = "INHERIT";

/** The rate a cap starts from when it is first set: 10 MB/s. */
const DEFAULT_UPLOAD_CAP = 10 * 1024 * 1024;

/**
 * An upload cap below the global level, each torrent's in bytes per second:
 * inherited, unlimited, or a rate in MB/s.
 */
export function uploadCapField(t: Translate, cap: Leaf<number>, onChange: (next: Leaf<number>) => void, idPrefix: string): FieldSpec {
  const label = t("next.seeding.uploadCap");
  return {
    id: `${idPrefix}-uploadCap`,
    label,
    keywords: "upload speed limit rate",
    control: {
      kind: "custom",
      control: (
        <LeafControl
          label={label}
          leaf={cap}
          offLabel={t("next.networking.unlimited")}
          fallback={DEFAULT_UPLOAD_CAP}
          onChange={onChange}
          input={(bps, onValue) => (
            <NumberField
              label={label}
              value={Math.round((bps / (1024 * 1024)) * 10) / 10}
              min={0}
              step={0.1}
              precision={1}
              suffix="MB/s"
              onChange={(next) => onValue(Math.round(Math.max(0, next) * 1024 * 1024))}
            />
          )}
        />
      ),
    },
  };
}

/** The global upload cap, each torrent's in bytes per second; null is none. */
export function globalUploadCapField(t: Translate, cap: number | null, onChange: (next: number | null) => void): FieldSpec {
  const label = t("next.seeding.uploadCap");
  return {
    id: "seeding-uploadCap",
    label,
    keywords: "upload speed limit rate",
    control: { kind: "custom", control: <LimitControl label={label} value={cap} suffix="MB/s" onChange={onChange} /> },
  };
}
