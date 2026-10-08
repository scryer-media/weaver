import { useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  CREATE_SCHEDULE_MUTATION,
  DELETE_SCHEDULE_MUTATION,
  HARDWARE_PROFILE_QUERY,
  SCHEDULES_QUERY,
  TOGGLE_SCHEDULE_MUTATION,
  UPDATE_SCHEDULE_MUTATION,
} from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor } from "../../../components/RecordEditor";
import { Tag } from "../../../components/chrome";
import { PrimaryButton, Toggle } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { formatRate } from "../../../data/format";
import { SCHEDULE_TRACKS, type ScheduleTrack } from "../../../data/schedule-tracks";
import {
  profileName,
  type HardwareProfileName,
  type HardwareProfileSettings,
} from "../../../data/hardware-profiles";
import { PanelControls, SettingsBlocks, usePanelStatus, type SettingsBlock } from "../framework";
import { scheduleActionFields, scheduleActionHelp, scheduleTimingFields, useScheduleTargets } from "../../../components/ScheduleOptionsFields";
import { ADDITIONAL_SCHEDULE_ACTIONS, NEW_SCHEDULE_OPTIONS, isOneShot, optionsFromSchedule, optionsInput, scheduleActionDetails, scheduleTimeLabel, type ScheduleOptions, type ScheduleOptionsForm, type ScheduleTargets } from "../../../data/schedule-options";

/**
 * Schedules: a clock that pauses, resumes or throttles the queue, or switches
 * the hardware profile.
 *
 * Weekdays are a set rather than a list of rules, so the editor draws them as
 * seven toggling chips — the one control in the design system that repeats
 * horizontally — and "no day selected" means every day.
 */

interface Schedule extends ScheduleOptions {
  script: string | null;
  runAtStartup: boolean;
  implicit: boolean;
  id: string;
  enabled: boolean;
  label: string | null;
  days: string[];
  time: string;
  actionType: string;
  track: ScheduleTrack;
  speedLimitBytes: number | null;
  hardwareProfile: HardwareProfileName | null;
}

interface ScheduleForm {
  options: ScheduleOptionsForm;
  script: string;
  runAtStartup: boolean;
  enabled: boolean;
  label: string;
  days: string[];
  time: string;
  actionType: string;
  speedMib: number;
  speedUnlimited: boolean;
  /** Null until one is picked; the editor then offers the recommendation. */
  hardwareProfile: HardwareProfileName | null;
}

const MIB = 1024 * 1024;

/** A stored limit as the editor shows it: mebibytes, to the two places its field keeps. */
const toMib = (bytes: number) => Math.round((bytes / MIB) * 100) / 100;

/** Labels are translation keys, resolved when the panel renders. */
const DAYS = [
  { key: "mon", label: "next.weekday.monShort" },
  { key: "tue", label: "next.weekday.tueShort" },
  { key: "wed", label: "next.weekday.wedShort" },
  { key: "thu", label: "next.weekday.thuShort" },
  { key: "fri", label: "next.weekday.friShort" },
  { key: "sat", label: "next.weekday.satShort" },
  { key: "sun", label: "next.weekday.sunShort" },
];

const ACTIONS: { value: string; label: string }[] = [
  ...ADDITIONAL_SCHEDULE_ACTIONS,
  { value: "run_script", label: "next.schedules.runScript" },
  { value: "pause", label: "next.schedules.pause" },
  { value: "resume", label: "next.schedules.resume" },
  { value: "speed_limit", label: "next.schedules.setLimit" },
  { value: "configured_speed_limit", label: "next.schedules.useConfiguredLimit" },
  { value: "pause_watch_folder_scanning", label: "next.schedules.pauseWatchFolder" },
  { value: "resume_watch_folder_scanning", label: "next.schedules.resumeWatchFolder" },
  { value: "hardware_profile", label: "next.schedules.setProfile" },
];

const NEW_SCHEDULE: ScheduleForm = {
  options: NEW_SCHEDULE_OPTIONS,
  script: "",
  runAtStartup: false,
  enabled: true,
  label: "",
  days: [],
  time: "08:00",
  actionType: "pause",
  speedMib: 5,
  speedUnlimited: false,
  hardwareProfile: null,
};

function actionLabel(t: Translate, schedule: Schedule, targets?: ScheduleTargets): string {
  if (schedule.actionType === "run_script") return t("next.schedules.runScriptNamed", { script: schedule.script ?? "" });
  const details = scheduleActionDetails(t, schedule, targets);
  if (details) return details;
  if (schedule.actionType === "speed_limit") {
    return schedule.speedLimitBytes
      ? t("next.schedules.limitTo", { rate: formatRate(schedule.speedLimitBytes) })
      : t("next.schedules.removeLimit");
  }
  if (schedule.actionType === "hardware_profile" && schedule.hardwareProfile) {
    return t("next.schedules.profileTo", { profile: profileName(t, schedule.hardwareProfile) });
  }
  const action = ACTIONS.find((option) => option.value === schedule.actionType);
  return action ? t(action.label) : schedule.actionType;
}

function daysLabel(t: Translate, days: string[]): string {
  if (days.length === 0 || days.length === DAYS.length) {
    return t("next.schedules.everyDay");
  }
  return DAYS.filter((day) => days.includes(day.key))
    .map((day) => t(day.label))
    .join(" ");
}

export function SchedulesPanel() {
  const t = useTranslate();
  const targets = useScheduleTargets();
  const [{ data, fetching }, reexecute] = useQuery<{ schedules: Schedule[] }>({ query: SCHEDULES_QUERY });
  const [, createSchedule] = useMutation(CREATE_SCHEDULE_MUTATION);
  const [, updateSchedule] = useMutation(UPDATE_SCHEDULE_MUTATION);
  const [, deleteSchedule] = useMutation(DELETE_SCHEDULE_MUTATION);
  const [, toggleSchedule] = useMutation(TOGGLE_SCHEDULE_MUTATION);
  const [{ data: profileData }] = useQuery<{ hardwareProfile: HardwareProfileSettings }>({
    query: HARDWARE_PROFILE_QUERY,
  });

  const [editingId, setEditingId] = useState<string | "new" | null>(null);
  const [form, setForm] = useState<ScheduleForm>(NEW_SCHEDULE);
  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmRemove, setConfirmRemove] = useState<Schedule | null>(null);

  const schedules = data?.schedules ?? [];
  const editing = schedules.find((entry) => entry.id === editingId) ?? null;

  usePanelStatus(error ?? status, error !== null);

  // A rule may only name a profile this machine can honour.
  const offeredProfiles = profileData?.hardwareProfile.available ?? [];
  const formProfile =
    form.hardwareProfile && offeredProfiles.includes(form.hardwareProfile)
      ? form.hardwareProfile
      : offeredProfiles.find((profile) => profile === profileData?.hardwareProfile.recommended) ??
        offeredProfiles[0] ??
        null;

  const open = (schedule: Schedule | null, actionType = NEW_SCHEDULE.actionType) => {
    setError(null);
    // A script's own task times are the script's to change, not the panel's.
    setStatus(schedule?.implicit ? t("next.schedules.manifestReadOnly") : null);
    if (schedule?.implicit) return;
    setForm(
      schedule
        ? {
            enabled: schedule.enabled,
            script: schedule.script ?? "",
            runAtStartup: schedule.runAtStartup,
            label: schedule.label ?? "",
            days: schedule.days,
            time: schedule.time,
            actionType: schedule.actionType,
            speedUnlimited:
              schedule.actionType === "speed_limit" && !schedule.speedLimitBytes,
            speedMib: schedule.speedLimitBytes ? toMib(schedule.speedLimitBytes) : NEW_SCHEDULE.speedMib,
            hardwareProfile: schedule.hardwareProfile,
            options: optionsFromSchedule(schedule),
          }
        : { ...NEW_SCHEDULE, actionType },
    );
    setEditingId(schedule ? schedule.id : "new");
  };

  const save = async () => {
    setBusy(true);
    const input: Record<string, unknown> = {
      time: form.time,
      actionType: form.actionType,
      days: form.days.length > 0 ? form.days : null,
      label: form.label.trim() || null,
      enabled: form.enabled,
      ...optionsInput(form.options, form.actionType),
    };
    if (form.actionType === "speed_limit") {
      // A limit the field only rounded for display goes back exactly as it was stored.
      const stored = editing?.speedLimitBytes ?? 0;
      input.speedLimitBytes = form.speedUnlimited
        ? 0
        : stored > 0 && toMib(stored) === form.speedMib
          ? stored
          : Math.round(form.speedMib * MIB);
    }
    if (form.actionType === "hardware_profile") {
      input.hardwareProfile = formProfile;
    }
    if (form.actionType === "run_script") { input.script = form.script; input.runAtStartup = form.runAtStartup; }
    const result =
      editingId === "new"
        ? await createSchedule({ input })
        : await updateSchedule({ id: editingId, input });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    setError(null);
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const remove = async () => {
    if (!confirmRemove) {
      return;
    }
    setBusy(true);
    const result = await deleteSchedule({ id: confirmRemove.id });
    setBusy(false);
    setConfirmRemove(null);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    setError(null);
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const toggle = async (schedule: Schedule, enabled: boolean) => {
    setStatus(null);
    const result = await toggleSchedule({ id: schedule.id, enabled });
    setError(result.error ? (result.error.graphQLErrors[0]?.message ?? result.error.message) : null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const blocks: SettingsBlock[] = SCHEDULE_TRACKS.map((track) => ({
      kind: "table",
      id: `schedules-${track.value}`,
      title: t(track.label),
      note: t(track.value === "ONE_SHOT" ? "next.schedules.oneShotNote" : "next.schedules.trackNote"),
      columns: "96px minmax(0, 0.8fr) minmax(0, 1.6fr) minmax(0, 1fr) 44px",
      headers: [
        t("next.schedules.time"),
        t("next.schedules.days"),
        t("next.schedules.action"),
        t("next.schedules.label"),
        "",
      ],
      empty: t("next.schedules.trackEmpty"),
      emptyAction: { label: t("next.schedules.add"), onClick: () => open(null, track.action) },
      onRowClick: (id) => {
        const schedule = schedules.find((entry) => entry.id === id);
        if (schedule) {
          open(schedule);
        }
      },
      rows: schedules.filter((schedule) => schedule.track === track.value).map((schedule) => ({
        id: schedule.id,
        searchText: `${scheduleTimeLabel(schedule)} ${daysLabel(t, schedule.days)} ${actionLabel(t, schedule, targets)} ${schedule.implicit ? t("next.schedules.manifest") : ""} ${schedule.label ?? ""}`,
        cells: [
          <Cell key="time" mono className="text-wv-fg" title={scheduleTimeLabel(schedule)}>
            {scheduleTimeLabel(schedule)}
          </Cell>,
          <Cell key="days" mono className="text-wv-secondary">
            {daysLabel(t, schedule.days)}
          </Cell>,
          <div key="action" className="flex min-w-0 items-center gap-[10px]">
            <Cell title={actionLabel(t, schedule, targets)}>{actionLabel(t, schedule, targets)}</Cell>
            {/* A rule a script's manifest declares: listed here, changed in the script. */}
            {schedule.implicit ? <Tag>{t("next.schedules.manifest")}</Tag> : null}
          </div>,
          <Cell key="label" className="text-wv-muted">
            {schedule.label || "—"}
          </Cell>,
          <span key="enabled" onClick={(event) => event.stopPropagation()}>
            <Toggle
              size="table"
              checked={schedule.enabled}
              disabled={schedule.implicit}
              label={t("next.schedules.enabledAria", { time: schedule.time })}
              onChange={(next) => void toggle(schedule, next)}
            />
          </span>,
        ],
      })),
  }));

  const optionFields = {
    t,
    action: form.actionType,
    value: form.options,
    onChange: (options: ScheduleOptionsForm) => setForm((current) => ({ ...current, options })),
  };
  // The editor says how long a rule lasts in the words its group uses in the list.
  const runsOnce = form.actionType === "run_script" || isOneShot(form.actionType);

  return (
    <>
      <PanelControls>
        <PrimaryButton icon="add" onClick={() => open(null)}>{t("next.schedules.add")}</PrimaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} loading={fetching && !data} />

      <RecordEditor
        open={editingId !== null}
        title={
          editingId === "new"
            ? t("next.schedules.add")
            : editing?.label || editing?.time || t("next.schedules.schedule")
        }
        note={editingId === "new" ? t("next.schedules.newNote") : daysLabel(t, editing?.days ?? [])}
        error={error}
        busy={busy}
        onSave={() => void save()}
        onDismiss={() => setEditingId(null)}
        onDelete={editing ? () => setConfirmRemove(editing) : undefined}
        deleteLabel={t("next.schedules.remove")}
        sections={[
          {
            id: "when",
            title: t("next.schedules.when"),
            note: t(runsOnce ? "next.schedules.oneShotNote" : "next.schedules.trackNote"),
            fields: [
              {
                id: "time",
                label: t("next.schedules.time"),
                // A script rule's time field takes the script evaluator's own notation.
                help: form.actionType === "run_script" ? t("next.schedules.scriptTimeHelp") : undefined,
                control: {
                  kind: form.actionType === "run_script" ? "text" : "time",
                  value: form.time,
                  onChange: (next) => setForm((current) => ({ ...current, time: next })),
                },
              },
              ...scheduleTimingFields(optionFields),
              {
                id: "days",
                label: t("next.schedules.days"),
                help: t("next.schedules.daysHelp"),
                control: {
                  kind: "custom",
                  control: (
                    <div role="group" aria-label={t("next.schedules.days")} className="flex flex-wrap justify-end gap-1.5">
                      {DAYS.map((day) => {
                        const active = form.days.includes(day.key);
                        return (
                          <button
                            key={day.key}
                            type="button"
                            aria-pressed={active}
                            onClick={() =>
                              setForm((current) => ({
                                ...current,
                                days: active
                                  ? current.days.filter((entry) => entry !== day.key)
                                  : [...current.days, day.key],
                              }))
                            }
                            className={`flex h-8 w-[46px] cursor-pointer items-center justify-center border font-wv-mono text-[11.5px] ${
                              active
                                ? "border-wv-accent bg-wv-segment-active font-medium text-wv-strong"
                                : "border-wv-control bg-wv-input text-wv-muted hover:border-wv-control-hover"
                            }`}
                          >
                            {t(day.label)}
                          </button>
                        );
                      })}
                    </div>
                  ),
                },
              },
            ],
          },
          {
            id: "what",
            title: t("next.schedules.whatHappens"),
            fields: [
              {
                id: "actionType",
                label: t("next.schedules.action"),
                help: scheduleActionHelp(t, form.actionType),
                control: {
                  kind: "select",
                  value: form.actionType,
                  options: ACTIONS.map((option) => ({ ...option, label: t(option.label) })),
                  onChange: (next) => setForm((current) => ({ ...current, actionType: next })),
                },
              },
              ...(form.actionType === "run_script" ? [
                { id: "script", label: t("next.schedules.script"), help: t("next.schedules.scriptHelp"), control: {
                  kind: "text" as const, value: form.script, onChange: (script: string) => setForm((current) => ({ ...current, script })),
                } },
                { id: "runAtStartup", label: t("next.schedules.runAtStartup"), help: t("next.schedules.runAtStartupHelp"), control: {
                  kind: "toggle" as const, value: form.runAtStartup, onChange: (runAtStartup: boolean) => setForm((current) => ({ ...current, runAtStartup })),
                } },
              ] : []),
              ...(form.actionType === "speed_limit"
                ? [
                    {
                      id: "speedUnlimited",
                      label: t("next.schedules.removeLimitInstead"),
                      help: t("next.schedules.removeLimitInsteadHelp"),
                      control: {
                        kind: "toggle" as const,
                        value: form.speedUnlimited,
                        onChange: (next: boolean) =>
                          setForm((current) => ({ ...current, speedUnlimited: next })),
                      },
                    },
                    ...(form.speedUnlimited
                      ? []
                      : [
                          {
                            id: "speedMib",
                            label: t("next.schedules.speedLimit"),
                            help: t("next.schedules.speedLimitHelp"),
                            // The same field as the bandwidth panel's ceiling, with
                            // the fractions a rule could already be saved with.
                            control: {
                              kind: "number" as const,
                              value: form.speedMib,
                              min: 0,
                              precision: 2,
                              suffix: "MB/s",
                              onChange: (next: number) =>
                                setForm((current) => ({ ...current, speedMib: next })),
                            },
                          },
                        ]),
                  ]
                : []),
              ...scheduleActionFields({ ...optionFields, targets }),
              ...(form.actionType === "hardware_profile"
                ? [
                    {
                      id: "hardwareProfile",
                      label: t("next.schedules.profile"),
                      help: t("next.schedules.profileHelp"),
                      control: {
                        kind: "select" as const,
                        value: formProfile ?? "",
                        options: offeredProfiles.map((profile) => ({
                          value: profile,
                          label: profileName(t, profile),
                        })),
                        onChange: (next: string) =>
                          setForm((current) => ({
                            ...current,
                            hardwareProfile: next as HardwareProfileName,
                          })),
                      },
                    },
                  ]
                : []),
              {
                id: "label",
                label: t("next.schedules.label"),
                help: t("next.schedules.labelHelp"),
                control: {
                  kind: "text",
                  mono: false,
                  value: form.label,
                  placeholder: t("next.schedules.labelPlaceholder"),
                  onChange: (next) => setForm((current) => ({ ...current, label: next })),
                },
              },
              {
                id: "enabled",
                label: t("next.schedules.enabled"),
                control: {
                  kind: "toggle",
                  value: form.enabled,
                  onChange: (next) => setForm((current) => ({ ...current, enabled: next })),
                },
              },
            ],
          },
        ]}
      />

      <ConfirmDialog
        open={confirmRemove !== null}
        title={t("next.schedules.remove")}
        note={confirmRemove?.time}
        busy={busy}
        confirmLabel={t("next.schedules.remove")}
        body={t("next.schedules.removeBody")}
        onConfirm={() => void remove()}
        onDismiss={() => setConfirmRemove(null)}
      />
    </>
  );
}
