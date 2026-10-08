import type { Translate } from "@/lib/context/translate-context";
import type { SettingsBlock } from "@/next/pages/settings/framework";

export const eventScriptDefaults = {
  eventScriptConcurrency: 1,
  eventScriptTimeoutSeconds: 300,
  fileDownloadedEventInterval: 0,
  scriptOutputCeilingBytes: 1048576,
  scriptOutputRunsPerJob: 32,
  scriptOutputRingBytes: 67108864,
  scriptOutputRunCapBytes: 2097152,
};

export type EventScriptOptions = typeof eventScriptDefaults;

export function eventScriptOptions(source: Partial<EventScriptOptions>): EventScriptOptions {
  return Object.fromEntries(Object.entries(eventScriptDefaults).map(([key, fallback]) =>
    [key, source[key as keyof EventScriptOptions] ?? fallback],
  )) as EventScriptOptions;
}

/** Each limit with the range the daemon accepts and the unit it is counted in. */
const FIELDS: {
  key: keyof EventScriptOptions;
  label: string;
  help: string;
  min: number;
  max: number;
  unit?: string;
}[] = [
  { key: "eventScriptConcurrency", label: "next.postProcessing.eventConcurrency", help: "next.postProcessing.eventConcurrencyHelp", min: 1, max: 8 },
  { key: "eventScriptTimeoutSeconds", label: "next.postProcessing.eventTimeout", help: "next.postProcessing.eventTimeoutHelp", min: 1, max: 86400, unit: "next.general.seconds" },
  { key: "fileDownloadedEventInterval", label: "next.postProcessing.fileEventInterval", help: "next.postProcessing.fileEventIntervalHelp", min: -1, max: 86400, unit: "next.general.seconds" },
  { key: "scriptOutputCeilingBytes", label: "next.postProcessing.outputCeiling", help: "next.postProcessing.outputCeilingHelp", min: 65536, max: 8388608, unit: "next.postProcessing.bytes" },
  { key: "scriptOutputRunsPerJob", label: "next.postProcessing.outputRuns", help: "next.postProcessing.outputRunsHelp", min: 1, max: 128 },
  { key: "scriptOutputRingBytes", label: "next.postProcessing.outputBudget", help: "next.postProcessing.outputBudgetHelp", min: 1048576, max: 1073741824, unit: "next.postProcessing.bytes" },
  { key: "scriptOutputRunCapBytes", label: "next.postProcessing.outputRunCap", help: "next.postProcessing.outputRunCapHelp", min: 65536, max: 8388608, unit: "next.postProcessing.bytes" },
];

/** The limits event scripts run under, and how much of what they print is kept. */
export function eventScriptSection(
  t: Translate,
  value: EventScriptOptions,
  onChange: (patch: Partial<EventScriptOptions>) => void,
): SettingsBlock {
  return {
    kind: "section",
    id: "event-scripts",
    title: t("next.postProcessing.events"),
    note: t("next.postProcessing.eventsNote"),
    fields: FIELDS.map(({ key, label, help, min, max, unit }) => ({
      id: key,
      label: t(label),
      help: t(help),
      control: {
        kind: "number",
        value: value[key],
        min,
        max,
        suffix: unit === undefined ? undefined : t(unit),
        onChange: (next: number) => onChange({ [key]: next }),
      },
    })),
  };
}
