import type { Translate } from "@/lib/context/translate-context";
import { NumberField } from "@/next/components/controls";
import type { FieldSpec, SettingsBlock } from "@/next/pages/settings/framework";

export const eventScriptDefaults = {
  eventScriptTimeoutSeconds: 300,
  fileDownloadedEventInterval: 0,
  scriptOutputRunsPerJob: 32,
  scriptOutputFailedRunsPerJob: 8,
};

export type EventScriptOptions = typeof eventScriptDefaults;

export function eventScriptOptions(source: Partial<EventScriptOptions>): EventScriptOptions {
  return Object.fromEntries(Object.entries(eventScriptDefaults).map(([key, fallback]) =>
    [key, source[key as keyof EventScriptOptions] ?? fallback],
  )) as EventScriptOptions;
}

/** Each limit with the range the daemon accepts. */
const FIELDS: {
  key: keyof EventScriptOptions;
  label: string;
  help: string;
  min: number;
  max: number;
  unit?: string;
  /** Lowering it deletes stored runs on save, so the field says so first. */
  deletesWhenLowered?: boolean;
}[] = [
  { key: "eventScriptTimeoutSeconds", label: "next.postProcessing.eventTimeout", help: "next.postProcessing.eventTimeoutHelp", min: 1, max: 86400, unit: "next.general.seconds" },
  { key: "fileDownloadedEventInterval", label: "next.postProcessing.fileEventInterval", help: "next.postProcessing.fileEventIntervalHelp", min: -1, max: 86400, unit: "next.general.seconds" },
  { key: "scriptOutputRunsPerJob", label: "next.postProcessing.outputRuns", help: "next.postProcessing.outputRunsHelp", min: 1, max: 128, deletesWhenLowered: true },
  { key: "scriptOutputFailedRunsPerJob", label: "next.postProcessing.outputFailedRuns", help: "next.postProcessing.outputFailedRunsHelp", min: 0, max: 128, deletesWhenLowered: true },
];

/**
 * The limits event scripts run under, and how many of their runs are kept.
 * `saved` is what the daemon holds now: a retention limit lowered below it
 * warns that saving deletes runs.
 */
export function eventScriptSection(
  t: Translate,
  value: EventScriptOptions,
  onChange: (patch: Partial<EventScriptOptions>) => void,
  saved: EventScriptOptions = value,
): SettingsBlock {
  return {
    kind: "section",
    id: "event-scripts",
    title: t("next.postProcessing.events"),
    note: t("next.postProcessing.eventsNote"),
    fields: FIELDS.map(({ key, label, help, min, max, unit, deletesWhenLowered }): FieldSpec => {
      const suffix = unit === undefined ? undefined : t(unit);
      const update = (next: number) => {
        if (next !== value[key]) onChange({ [key]: next });
      };
      if (!deletesWhenLowered) {
        return {
          id: key,
          label: t(label),
          help: t(help),
          control: { kind: "number", value: value[key], min, max, suffix, onChange: update },
        };
      }
      const lowered = value[key] < saved[key];
      return {
        id: key,
        label: t(label),
        help: t(help),
        control: {
          kind: "custom",
          control: (
            <span className="flex max-w-[320px] flex-col items-end gap-[6px]">
              <NumberField value={value[key]} onChange={update} label={t(label)} min={min} max={max} suffix={suffix} />
              {lowered ? (
                <span role="alert" data-testid={`${key}-lowered`} className="text-right text-[11.5px] leading-[1.4] text-wv-warn">
                  {t("next.postProcessing.outputLoweringDeletes")}
                </span>
              ) : null}
            </span>
          ),
        },
      };
    }),
  };
}
