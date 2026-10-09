import type { Translate } from "@/lib/context/translate-context";

/**
 * Script instances: a script wired to one trigger, with what was saved for it.
 *
 * The instance is what runs. A script's header only offers a preset, which
 * fills a form and is never read back into a saved instance. Everything here is
 * the arithmetic the Scripts screen is drawn from, kept apart from React so it
 * can be tested on its own.
 */

export type ScriptKind = "POST_PROCESSING" | "QUEUE" | "SCAN" | "SCHEDULER" | "FEED";

/** The triggers, in the order every list and picker draws them. */
export const SCRIPT_KINDS: readonly ScriptKind[] = ["POST_PROCESSING", "QUEUE", "SCAN", "SCHEDULER", "FEED"];

export const SCRIPT_KIND_LABELS: Record<ScriptKind, string> = {
  POST_PROCESSING: "next.postProcessing.kindPostProcessing",
  QUEUE: "next.postProcessing.kindQueue",
  SCAN: "next.postProcessing.kindScan",
  SCHEDULER: "next.schedules.schedule",
  FEED: "next.postProcessing.kindFeed",
};

/** The queue events an instance can run on, in the order the daemon lists them. */
export const QUEUE_EVENTS = [
  "FILE_DOWNLOADED",
  "URL_COMPLETED",
  "NZB_MARKED",
  "NZB_ADDED",
  "NZB_NAMED",
  "NZB_DOWNLOADED",
  "NZB_DELETED",
] as const;

export type QueueEvent = (typeof QUEUE_EVENTS)[number];

export type ScriptOptionType = "STRING" | "INTEGER" | "NUMBER" | "BOOLEAN" | "SECRET";

/** What a script's header says about one input, for drawing its field. */
export interface ScriptOption {
  name: string;
  section?: string | null;
  optionType: ScriptOptionType;
  displayName?: string | null;
  description: string[];
  select: string[];
  required: boolean;
  defaultValue?: string | null;
}

/** One input. A secret's value is never read back, so it is null. */
export interface ScriptInstanceValue {
  name: string;
  value: string | null;
  secret: boolean;
}

export interface ScriptPresetTrigger {
  trigger: ScriptKind;
  queueEvent: QueueEvent | null;
}

/** What a script's header offers as a starting point. */
export interface ScriptPreset {
  triggers: ScriptPresetTrigger[];
  taskTimes: string[];
  inputs: ScriptInstanceValue[];
}

export interface DiscoveredScript {
  name: string;
  displayName: string;
  adapter: "SABNZBD" | "NZBGET";
  kinds: ScriptKind[];
  queueEvents: string[];
  taskTimes: string[];
  version?: string | null;
  options: ScriptOption[];
  preset: ScriptPreset;
}

export interface ScriptInstance {
  id: string;
  name: string;
  script: string;
  trigger: ScriptKind;
  queueEvent: QueueEvent | null;
  inputs: ScriptInstanceValue[];
  /** Empty runs for every category. */
  categories: string[];
  enabled: boolean;
  blocking: boolean;
  /** Null runs under the default timeout of its trigger. */
  timeoutSeconds: number | null;
  runOrder: number;
  /** Why the script cannot run as things stand; null when it can. */
  scriptProblem: string | null;
  /** The script's header no longer declares the inputs this instance holds. */
  headerDrift: boolean;
}

/** The longest an instance may be given: seven days. */
export const MAX_TIMEOUT_SECONDS = 7 * 24 * 60 * 60;

/** Only a download has a category, so only these triggers can be narrowed to one. */
export function categoryScoped(trigger: ScriptKind): boolean {
  return trigger === "POST_PROCESSING" || trigger === "QUEUE";
}

/** A trigger in words; a queue trigger keeps the event it runs on. */
export function triggerTitle(t: Translate, trigger: ScriptKind, queueEvent: string | null): string {
  const kind = t(SCRIPT_KIND_LABELS[trigger]);
  return trigger === "QUEUE" && queueEvent ? `${kind} · ${queueEvent}` : kind;
}

/* ------------------------------------------------------------------ groups */

export interface InstanceGroup {
  /** Stable across renders: the trigger, and the event for a queue group. */
  id: string;
  trigger: ScriptKind;
  queueEvent: QueueEvent | null;
  instances: ScriptInstance[];
}

function groupId(trigger: ScriptKind, queueEvent: string | null): string {
  return trigger === "QUEUE" ? `QUEUE:${queueEvent ?? ""}` : trigger;
}

function byRunOrder(left: ScriptInstance, right: ScriptInstance): number {
  return left.runOrder - right.runOrder;
}

/**
 * Instances by what starts them: one group per trigger, and one per event for
 * the queue, each in the order its instances run. A group with nothing in it
 * is left out.
 */
export function groupInstances(instances: readonly ScriptInstance[]): InstanceGroup[] {
  const groups: InstanceGroup[] = [];
  const add = (trigger: ScriptKind, queueEvent: QueueEvent | null) => {
    const id = groupId(trigger, queueEvent);
    const members = instances
      .filter((instance) => groupId(instance.trigger, instance.queueEvent) === id)
      .sort(byRunOrder);
    if (members.length > 0) {
      groups.push({ id, trigger, queueEvent, instances: members });
    }
  };
  for (const trigger of SCRIPT_KINDS) {
    if (trigger === "QUEUE") {
      for (const event of QUEUE_EVENTS) {
        add(trigger, event);
      }
      // A queue instance whose event this screen does not know still has a row.
      const known = new Set<string>(QUEUE_EVENTS);
      const others = [...new Set(
        instances
          .filter((instance) => instance.trigger === "QUEUE" && !known.has(instance.queueEvent ?? ""))
          .map((instance) => instance.queueEvent),
      )];
      for (const event of others) {
        add(trigger, event);
      }
    } else {
      add(trigger, null);
    }
  }
  return groups;
}

/**
 * The order to ask for when an instance trades places with its neighbour.
 *
 * The daemon orders one trigger's instances together, while the table shows
 * the queue's by event, so the neighbour is the next row of the same group and
 * the ids sent are every instance of the trigger. Null when there is no
 * neighbour that way.
 */
export function reorderedIds(
  instances: readonly ScriptInstance[],
  id: string,
  direction: -1 | 1,
): { trigger: ScriptKind; ids: string[] } | null {
  const moving = instances.find((instance) => instance.id === id);
  if (!moving) {
    return null;
  }
  const group = groupInstances(instances).find((entry) =>
    entry.instances.some((instance) => instance.id === id),
  );
  const row = group?.instances.findIndex((instance) => instance.id === id) ?? -1;
  const neighbour = group?.instances[row + direction];
  if (!neighbour) {
    return null;
  }
  const ids = instances
    .filter((instance) => instance.trigger === moving.trigger)
    .sort(byRunOrder)
    .map((instance) => instance.id);
  const from = ids.indexOf(moving.id);
  const to = ids.indexOf(neighbour.id);
  ids[from] = neighbour.id;
  ids[to] = moving.id;
  return { trigger: moving.trigger, ids };
}

/** The discovered scripts nothing has been wired to yet. */
export function scriptsWithoutInstance(
  scripts: readonly DiscoveredScript[],
  instances: readonly ScriptInstance[],
): DiscoveredScript[] {
  const wired = new Set(instances.map((instance) => instance.script));
  return scripts.filter((script) => !wired.has(script.name));
}

function sameTrigger(left: ScriptPresetTrigger, instance: ScriptInstance): boolean {
  return left.trigger === instance.trigger
    && (left.trigger !== "QUEUE" || left.queueEvent === instance.queueEvent);
}

/** The triggers a script's header declares that no instance of it runs on yet. */
export function unwiredTriggers(
  script: DiscoveredScript,
  instances: readonly ScriptInstance[],
): ScriptPresetTrigger[] {
  const own = instances.filter((instance) => instance.script === script.name);
  return script.preset.triggers.filter((trigger) => !own.some((instance) => sameTrigger(trigger, instance)));
}

/* -------------------------------------------------------------------- form */

/** One input as the editor holds it. */
export interface InstanceInputForm {
  name: string;
  /** What is typed. A secret starts blank, whatever is saved. */
  value: string;
  secret: boolean;
  /** A secret the daemon already holds a value for: left blank, it is kept. */
  stored: boolean;
}

export interface InstanceForm {
  /** Blank takes the script's name. */
  name: string;
  script: string;
  trigger: ScriptKind;
  /** Only read while the trigger is the queue. */
  queueEvent: QueueEvent;
  inputs: InstanceInputForm[];
  categories: string[];
  enabled: boolean;
  blocking: boolean;
  /** Zero runs under the trigger's default timeout. */
  timeoutSeconds: number;
}

function sameName(left: string, right: string): boolean {
  return left.toLowerCase() === right.toLowerCase();
}

function presetInputs(script: DiscoveredScript | undefined): InstanceInputForm[] {
  return (script?.preset.inputs ?? []).map((input) => ({
    name: input.name,
    // A secret is never pre-filled from the header.
    value: input.secret ? "" : (input.value ?? ""),
    secret: input.secret,
    stored: false,
  }));
}

/**
 * A new instance of `script`, filled from its header: the first trigger it
 * declares and every declared input at its default.
 */
export function newInstanceForm(script: DiscoveredScript | undefined): InstanceForm {
  const declared = script?.preset.triggers[0];
  return {
    name: "",
    script: script?.name ?? "",
    trigger: declared?.trigger ?? "POST_PROCESSING",
    queueEvent: declared?.queueEvent ?? firstQueueEvent(script),
    inputs: presetInputs(script),
    categories: [],
    enabled: true,
    blocking: true,
    timeoutSeconds: 0,
  };
}

/** The queue event a form starts on: the first the header declares, else the first there is. */
export function firstQueueEvent(script: DiscoveredScript | undefined): QueueEvent {
  return script?.preset.triggers.find((trigger) => trigger.trigger === "QUEUE")?.queueEvent ?? "NZB_ADDED";
}

/**
 * A saved instance as the editor shows it.
 *
 * A secret the header declares and the instance was never given has no saved
 * input at all, so it is added here: otherwise there would be nowhere to type it.
 */
export function formFromInstance(instance: ScriptInstance, script: DiscoveredScript | undefined): InstanceForm {
  const inputs: InstanceInputForm[] = instance.inputs.map((input) => ({
    name: input.name,
    value: input.secret ? "" : (input.value ?? ""),
    secret: input.secret,
    stored: input.secret,
  }));
  for (const declared of presetInputs(script)) {
    if (declared.secret && !inputs.some((input) => sameName(input.name, declared.name))) {
      inputs.push(declared);
    }
  }
  return {
    name: instance.name,
    script: instance.script,
    trigger: instance.trigger,
    queueEvent: instance.queueEvent ?? firstQueueEvent(script),
    inputs,
    categories: instance.categories,
    enabled: instance.enabled,
    blocking: instance.blocking,
    timeoutSeconds: instance.timeoutSeconds ?? 0,
  };
}

/**
 * The form after another script is picked. A new instance starts again from
 * that script's header; a saved one keeps what is saved in it.
 */
export function withScript(form: InstanceForm, script: DiscoveredScript | undefined, creating: boolean): InstanceForm {
  if (!creating) {
    return { ...form, script: script?.name ?? form.script };
  }
  const fresh = newInstanceForm(script);
  return { ...fresh, name: form.name, enabled: form.enabled, blocking: form.blocking, timeoutSeconds: form.timeoutSeconds };
}

export interface ScriptInstanceInput {
  name: string;
  script: string;
  trigger: ScriptKind;
  queueEvent: QueueEvent | null;
  inputs: { name: string; value: string | null; secret: boolean }[];
  categories: string[];
  enabled: boolean;
  blocking: boolean;
  timeoutSeconds: number | null;
}

/** What the daemon is sent for a form. A secret left blank is sent without a value, which keeps the saved one. */
export function inputFromForm(form: InstanceForm): ScriptInstanceInput {
  return {
    name: form.name.trim(),
    script: form.script,
    trigger: form.trigger,
    queueEvent: form.trigger === "QUEUE" ? form.queueEvent : null,
    inputs: form.inputs.map((input) => ({
      name: input.name.trim(),
      value: input.secret && input.value === "" ? null : input.value,
      secret: input.secret,
    })),
    categories: categoryScoped(form.trigger) ? form.categories : [],
    enabled: form.enabled,
    blocking: form.blocking,
    timeoutSeconds: form.timeoutSeconds > 0 ? Math.min(MAX_TIMEOUT_SECONDS, Math.round(form.timeoutSeconds)) : null,
  };
}

/** A saved instance sent back as it is, apart from `patch`. Its secrets are kept. */
export function inputFromInstance(
  instance: ScriptInstance,
  patch: Partial<Pick<ScriptInstanceInput, "enabled" | "blocking">> = {},
): ScriptInstanceInput {
  return {
    name: instance.name,
    script: instance.script,
    trigger: instance.trigger,
    queueEvent: instance.trigger === "QUEUE" ? instance.queueEvent : null,
    inputs: instance.inputs.map((input) => ({
      name: input.name,
      value: input.secret ? null : (input.value ?? ""),
      secret: input.secret,
    })),
    categories: instance.categories,
    enabled: instance.enabled,
    blocking: instance.blocking,
    timeoutSeconds: instance.timeoutSeconds,
    ...patch,
  };
}

/** Letters, digits, `_` and `-`, starting with a letter; dots join such parts. */
const INPUT_NAME = /^[A-Za-z][A-Za-z0-9_-]*(\.[A-Za-z][A-Za-z0-9_-]*)*$/;

/** Why `name` cannot be added to `inputs`, as a translation key; null when it can. */
export function inputNameProblem(name: string, inputs: readonly InstanceInputForm[]): string | null {
  const trimmed = name.trim();
  if (!INPUT_NAME.test(trimmed) || trimmed.length > 128) {
    return "next.postProcessing.inputNameInvalid";
  }
  return inputs.some((input) => sameName(input.name, trimmed)) ? "next.postProcessing.inputNameTaken" : null;
}

/** What the header declares about an input, whatever case the instance holds its name in. */
export function declaredOption(script: DiscoveredScript | undefined, name: string): ScriptOption | undefined {
  return script?.options.find((option) => sameName(option.name, name));
}

/** A boolean input as the daemon writes one: `yes` or `no`. */
export function booleanInputValue(on: boolean): string {
  return on ? "yes" : "no";
}

export function booleanInputOn(value: string): boolean {
  return ["yes", "true", "1", "on"].includes(value.trim().toLowerCase());
}

/** A timeout exactly as it was set: `45s`, `1m 30s`, `2h`, `7d`. */
export function formatTimeout(seconds: number): string {
  const total = Math.max(0, Math.round(seconds));
  const parts = [
    [Math.floor(total / 86400), "d"],
    [Math.floor(total / 3600) % 24, "h"],
    [Math.floor(total / 60) % 60, "m"],
    [total % 60, "s"],
  ] as const;
  const shown = parts.filter(([amount]) => amount > 0).map(([amount, unit]) => `${amount}${unit}`);
  return shown.length > 0 ? shown.join(" ") : "0s";
}
