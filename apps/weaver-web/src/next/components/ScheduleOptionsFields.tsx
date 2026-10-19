// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { gql, useQuery } from "urql";
import type { Translate } from "@/lib/context/translate-context";
import type { FieldSpec } from "@/next/pages/settings/framework";
import { Segmented, TextField } from "./controls";
import {
  isOneShot,
  speedKey,
  speedProblem,
  type ScheduleOptionsForm,
  type ScheduleTargets,
  type SpeedEntry,
  type SpeedTargetKind,
} from "../data/schedule-options";

/**
 * The rows an action adds to the schedule editor.
 *
 * They are the same declarative fields the rest of the editor is built from,
 * so each option is a labelled row with a shared control: the extra times sit
 * under the clock, and an action's target and switches sit under the action.
 */

const TARGETS = gql`query ScheduleTargets { servers { id host } egressInterfaces { id name } }`;

export function useScheduleTargets() {
  const [{ data }] = useQuery<ScheduleTargets>({ query: TARGETS });
  return data;
}

interface ScheduleOptionFields {
  t: Translate;
  action: string;
  value: ScheduleOptionsForm;
  onChange: (value: ScheduleOptionsForm) => void;
}

/** What the chosen action does, for the line under the action picker. */
export function scheduleActionHelp(t: Translate, action: string): string | undefined {
  if (action === "pause_all") return t("next.schedules.pauseAllHelp");
  if (action === "pause_post_processing" || action === "resume_post_processing") return t("next.schedules.postHelp");
  return isOneShot(action) ? t("next.schedules.oneShotHelp") : undefined;
}

/** The times a rule fires at beyond its single time: every hour, or a list. */
export function scheduleTimingFields({ t, action, value, onChange }: ScheduleOptionFields): FieldSpec[] {
  // A speed rule takes the one time of day.
  if (action === "speed_limit") return [];
  const set = (patch: Partial<ScheduleOptionsForm>) => onChange({ ...value, ...patch });
  const hourly = isOneShot(action) && value.everyHourAtMinute !== null;
  const fields: FieldSpec[] = [];
  if (isOneShot(action)) {
    fields.push({
      id: "everyHour",
      label: t("next.schedules.hourly"),
      control: { kind: "toggle", value: hourly, onChange: (next) => set({ everyHourAtMinute: next ? 0 : null }) },
    });
  }
  fields.push(hourly
    ? {
        id: "everyHourAtMinute",
        label: t("next.schedules.minute"),
        control: { kind: "number", value: value.everyHourAtMinute ?? 0, min: 0, max: 59, onChange: (next) => set({ everyHourAtMinute: next }) },
      }
    : {
        id: "times",
        label: t("next.schedules.multipleTimes"),
        help: t("next.schedules.multipleTimesHelp"),
        control: { kind: "text", value: value.timesText, placeholder: "08:00, 18:00", onChange: (next) => set({ timesText: next }) },
      });
  return fields;
}

/** A select over the records an action can target, with a leading "none" choice. */
function targetOptions(none: string, unknown: string, selected: number | null, records: { id: number; label: string }[]) {
  const options = [{ value: "", label: none }, ...records.map((record) => ({ value: String(record.id), label: record.label }))];
  // A rule can outlive its target; name the id rather than show a bare number.
  if (selected !== null && !records.some((record) => record.id === selected)) {
    options.push({ value: String(selected), label: `${unknown} #${selected}` });
  }
  return options;
}

/** The target and switches that belong to one action. */
export function scheduleActionFields({ t, action, value, onChange, targets }: ScheduleOptionFields & {
  targets: ScheduleTargets | undefined;
}): FieldSpec[] {
  const set = (patch: Partial<ScheduleOptionsForm>) => onChange({ ...value, ...patch });
  if (action === "set_server_active") {
    return [
      {
        id: "serverId",
        label: t("next.schedules.server"),
        control: {
          kind: "select",
          value: value.serverId === null ? "" : String(value.serverId),
          options: targetOptions(
            t("next.schedules.chooseServer"),
            t("next.schedules.server"),
            value.serverId,
            (targets?.servers ?? []).map((server) => ({ id: server.id, label: server.host })),
          ),
          onChange: (next) => set({ serverId: next ? Number(next) : null }),
        },
      },
      {
        id: "serverActive",
        label: t("next.schedules.serverEnabled"),
        help: t("next.schedules.serverHelp"),
        control: { kind: "toggle", value: value.serverActive ?? true, onChange: (next) => set({ serverActive: next }) },
      },
    ];
  }
  if (action === "set_quota_metering") {
    return [
      {
        id: "quotaMeteringEnabled",
        label: t("next.schedules.quotaCounting"),
        help: t("next.schedules.quotaHelp"),
        control: {
          kind: "select",
          value: value.quotaMeteringEnabled ? "on" : "off",
          options: [
            { value: "on", label: t("next.schedules.quotaCountOn") },
            { value: "off", label: t("next.schedules.quotaCountOff") },
          ],
          onChange: (next) => set({ quotaMeteringEnabled: next === "on" }),
        },
      },
      {
        id: "quotaEgressId",
        label: t("next.schedules.egress"),
        control: {
          kind: "select",
          value: value.quotaEgressId === null ? "" : String(value.quotaEgressId),
          options: targetOptions(
            t("next.schedules.everyEgress"),
            t("next.schedules.egress"),
            value.quotaEgressId,
            (targets?.egressInterfaces ?? []).map((egress) => ({ id: egress.id, label: egress.name })),
          ),
          onChange: (next) => set({ quotaEgressId: next ? Number(next) : null }),
        },
      },
    ];
  }
  if (action === "bandwidth_split") {
    const rows: FieldSpec[] = [{
      id: "splitLift",
      label: t("next.schedules.splitLift"),
      control: { kind: "toggle", value: value.splitLift, onChange: (next) => set({ splitLift: next }) },
    }];
    if (!value.splitLift) {
      rows.push({
        id: "splitPct",
        label: t("next.bandwidth.splitUsenet"),
        help: t("next.bandwidth.splitTorrents", { pct: 100 - value.splitPct }),
        control: { kind: "number", value: value.splitPct, min: 1, max: 99, suffix: "%", onChange: (next) => set({ splitPct: next }) },
      });
    }
    return rows;
  }
  if (action === "prune_history") {
    return (["pruneFailed", "pruneCompleted", "pruneCancelled"] as const).flatMap((key) => {
      const choice = value[key];
      const kind = t(`next.schedules.${key}`);
      const rows: FieldSpec[] = [{
        id: key,
        label: kind,
        help: key === "pruneCompleted" ? t("next.schedules.pruneWarning") : undefined,
        control: {
          kind: "toggle",
          value: choice !== null,
          onChange: (next) => set({ [key]: next ? { deleteFiles: key !== "pruneCompleted" } : null }),
        },
      }];
      if (choice) {
        rows.push({
          id: `${key}Files`,
          label: t("next.schedules.deleteFilesOf", { kind }),
          help: key === "pruneCompleted" ? t("next.schedules.pruneFilesWarning") : undefined,
          control: { kind: "toggle", value: choice.deleteFiles, onChange: (next) => set({ [key]: { deleteFiles: next } }) },
        });
      }
      return rows;
    });
  }
  return [];
}

type RateChoice = "keep" | "unlimited" | "limit";

/**
 * One direction of a target: as it is (the download only, and only while the
 * rule does not set the target), no limit, or a rate in MB/s.
 */
function RateControl({
  t,
  label,
  text,
  keep,
  onChange,
}: {
  t: Translate;
  label: string;
  /** The rate typed, null for no limit; undefined while the target is left as it is. */
  text: string | null | undefined;
  /** Whether "as is" is offered. */
  keep: boolean;
  onChange: (next: string | null | undefined) => void;
}) {
  const choice: RateChoice = text === undefined ? "keep" : text === null ? "unlimited" : "limit";
  return (
    <div className="flex flex-wrap items-center gap-[10px]">
      <Segmented<RateChoice>
        label={label}
        value={choice}
        options={[
          ...(keep ? [{ value: "keep" as const, label: t("next.schedules.speedLeaveAsIs") }] : []),
          { value: "unlimited", label: t("next.schedules.unlimited") },
          { value: "limit", label: t("next.schedules.speedSet") },
        ]}
        onChange={(next) => onChange(next === "keep" ? undefined : next === "unlimited" ? null : (text ?? ""))}
      />
      {choice === "limit" ? (
        <TextField label={label} value={text ?? ""} placeholder="0" onChange={onChange} className="w-[110px]" />
      ) : null}
    </div>
  );
}

/**
 * The targets a speed rule can set, one group per heading: the global limit,
 * then each egress, then each provider. Each is left as it is, lifted, or
 * given a rate; the global one also takes the upload rate it sets with it.
 */
export function scheduleSpeedSections({ t, value, onChange, targets }: Omit<ScheduleOptionFields, "action"> & {
  targets: ScheduleTargets | undefined;
}): { id: string; title: string; note?: string; fields: FieldSpec[] }[] {
  const setEntry = (key: string, entry: SpeedEntry | undefined) => {
    const { [key]: _dropped, ...rest } = value.speeds;
    onChange({ ...value, speeds: entry ? { ...rest, [key]: entry } : rest });
  };
  const field = (kind: SpeedTargetKind, id: number | null, label: string): FieldSpec => {
    const key = speedKey(kind, id);
    const entry = value.speeds[key];
    const problem = entry && entry.download !== null ? speedProblem({ download: entry.download, upload: null }, kind) : null;
    return {
      id: `speed-${key}`,
      label,
      help: problem ? t(problem) : undefined,
      keywords: "MB/s",
      control: {
        kind: "custom",
        control: (
          <RateControl
            t={t}
            label={label}
            keep
            text={entry?.download}
            onChange={(download) =>
              setEntry(key, download === undefined ? undefined : { download, upload: entry?.upload ?? null })
            }
          />
        ),
      },
    };
  };
  const globalEntry = value.speeds[speedKey("GLOBAL", null)];
  const uploadProblem = globalEntry && globalEntry.upload !== null
    ? speedProblem({ download: null, upload: globalEntry.upload }, "GLOBAL")
    : null;
  const globalUpload: FieldSpec[] = globalEntry
    ? [
        {
          id: "speed-GLOBAL-upload",
          label: t("next.schedules.speedGlobalUpload"),
          help: uploadProblem ? t(uploadProblem) : undefined,
          keywords: "MB/s upload",
          control: {
            kind: "custom",
            control: (
              <RateControl
                t={t}
                label={t("next.schedules.speedGlobalUpload")}
                keep={false}
                text={globalEntry.upload}
                onChange={(upload) => setEntry(speedKey("GLOBAL", null), { ...globalEntry, upload: upload ?? null })}
              />
            ),
          },
        },
      ]
    : [];
  // A rule can outlive an egress or provider it names until the next load drops it.
  const listed = <T extends { id: number }>(kind: SpeedTargetKind, records: readonly T[], name: (record: T) => string, unknown: string) => [
    ...records.map((record) => field(kind, record.id, name(record))),
    ...Object.keys(value.speeds)
      .filter((key) => key.startsWith(`${kind}:`))
      .map((key) => Number(key.slice(kind.length + 1)))
      .filter((id) => !records.some((record) => record.id === id))
      .map((id) => field(kind, id, `${unknown} #${id}`)),
  ];
  const egresses = listed("EGRESS", targets?.egressInterfaces ?? [], (egress) => egress.name, t("next.schedules.egress"));
  const servers = listed("SERVER", targets?.servers ?? [], (server) => server.host, t("next.schedules.server"));
  return [
    {
      id: "speedGlobal",
      title: t("next.schedules.speedGlobal"),
      note: t("next.schedules.speedUnit"),
      fields: [field("GLOBAL", null, t("next.schedules.speedGlobalField")), ...globalUpload],
    },
    ...(egresses.length > 0 ? [{ id: "speedEgresses", title: t("next.schedules.speedEgresses"), fields: egresses }] : []),
    ...(servers.length > 0 ? [{ id: "speedServers", title: t("next.schedules.speedServers"), fields: servers }] : []),
  ];
}
