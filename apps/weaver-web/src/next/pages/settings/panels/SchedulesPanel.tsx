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
import { PrimaryButton, Toggle } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { SCHEDULE_TRACKS, type ScheduleTrack } from "../../../data/schedule-tracks";
import {
  profileName,
  type HardwareProfileName,
  type HardwareProfileSettings,
} from "../../../data/hardware-profiles";
import { PanelControls, SettingsBlocks, usePanelStatus, type SettingsBlock } from "../framework";
import { scheduleActionFields, scheduleActionHelp, scheduleSpeedSections, scheduleTimingFields, useScheduleTargets } from "../../../components/ScheduleOptionsFields";
import { NEW_SCHEDULE_OPTIONS, SCHEDULE_ACTIONS, SCHEDULE_DAYS, isOneShot, optionsFromSchedule, optionsInput, scheduleActionDetails, scheduleDaysLabel, scheduleTimeLabel, speedProblem, type ScheduleOptions, type ScheduleOptionsForm, type ScheduleTargets } from "../../../data/schedule-options";

/**
 * Schedules: a clock that pauses, resumes or throttles the queue, or switches
 * a setting. Scripts keep their run times on their own jobs and never show here.
 *
 * Weekdays are a set rather than a list of rules, so the editor draws them as
 * seven toggling chips — the one control in the design system that repeats
 * horizontally — and "no day selected" means every day.
 */

interface Schedule extends ScheduleOptions {
  id: string;
  enabled: boolean;
  label: string | null;
  days: string[];
  time: string;
  actionType: string;
  track: ScheduleTrack;
  hardwareProfile: HardwareProfileName | null;
}

interface ScheduleForm {
  options: ScheduleOptionsForm;
  enabled: boolean;
  label: string;
  days: string[];
  time: string;
  actionType: string;
  /** Null until one is picked; the editor then offers the recommendation. */
  hardwareProfile: HardwareProfileName | null;
}

const NEW_SCHEDULE: ScheduleForm = {
  options: NEW_SCHEDULE_OPTIONS,
  enabled: true,
  label: "",
  days: [],
  time: "08:00",
  actionType: "pause",
  hardwareProfile: null,
};

function actionLabel(t: Translate, schedule: Schedule, targets?: ScheduleTargets): string {
  const details = scheduleActionDetails(t, schedule, targets);
  if (details) return details;
  if (schedule.actionType === "hardware_profile" && schedule.hardwareProfile) {
    return t("next.schedules.profileTo", { profile: profileName(t, schedule.hardwareProfile) });
  }
  const action = SCHEDULE_ACTIONS.find((option) => option.value === schedule.actionType);
  return action ? t(action.label) : schedule.actionType;
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

  const open = (schedule: Schedule | null) => {
    setError(null);
    setStatus(null);
    setForm(
      schedule
        ? {
            enabled: schedule.enabled,
            label: schedule.label ?? "",
            days: schedule.days,
            time: schedule.time,
            actionType: schedule.actionType,
            hardwareProfile: schedule.hardwareProfile,
            options: optionsFromSchedule(schedule),
          }
        : NEW_SCHEDULE,
    );
    setEditingId(schedule ? schedule.id : "new");
  };

  const save = async () => {
    if (form.actionType === "speed_limit") {
      // What is wrong with a rate, or a rule that sets nothing, is said before saving.
      const typed = Object.values(form.options.speeds);
      if (typed.some((text) => speedProblem(text) !== null)) {
        setError(t("next.schedules.speedInvalid"));
        return;
      }
      if (typed.every((text) => text.trim() === "")) {
        setError(t("next.schedules.speedNeeded"));
        return;
      }
    }
    setBusy(true);
    const input: Record<string, unknown> = {
      time: form.time,
      actionType: form.actionType,
      days: form.days.length > 0 ? form.days : null,
      label: form.label.trim() || null,
      enabled: form.enabled,
      ...optionsInput(form.options, form.actionType, editing?.speedLimits ?? []),
    };
    if (form.actionType === "hardware_profile") {
      input.hardwareProfile = formProfile;
    }
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

  const row = (schedule: Schedule) => ({
    id: schedule.id,
    searchText: `${scheduleTimeLabel(schedule)} ${scheduleDaysLabel(t, schedule.days)} ${actionLabel(t, schedule, targets)} ${schedule.label ?? ""}`,
    cells: [
      <Cell key="time" mono className="text-wv-fg" title={scheduleTimeLabel(schedule)}>
        {scheduleTimeLabel(schedule)}
      </Cell>,
      <Cell key="days" mono className="text-wv-secondary">
        {scheduleDaysLabel(t, schedule.days)}
      </Cell>,
      <Cell key="action" title={actionLabel(t, schedule, targets)}>
        {actionLabel(t, schedule, targets)}
      </Cell>,
      <Cell key="label" className="text-wv-muted">
        {schedule.label || "—"}
      </Cell>,
      <span key="enabled" onClick={(event) => event.stopPropagation()}>
        <Toggle
          size="table"
          checked={schedule.enabled}
          label={t("next.schedules.enabledAria", { time: schedule.time })}
          onChange={(next) => void toggle(schedule, next)}
        />
      </span>,
    ],
  });

  // One list of every rule. Rules that replace one another sit together under
  // their group's heading, because a rule holds until the next one in its group.
  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "schedules",
      title: t("next.settings.panel.schedules"),
      note: t("next.schedules.trackNote"),
      columns: "96px minmax(0, 0.8fr) minmax(0, 1.6fr) minmax(0, 1fr) 44px",
      headers: [
        t("next.schedules.time"),
        t("next.schedules.days"),
        t("next.schedules.action"),
        t("next.schedules.label"),
        "",
      ],
      empty: t("next.schedules.empty"),
      onRowClick: (id) => {
        const schedule = schedules.find((entry) => entry.id === id);
        if (schedule) {
          open(schedule);
        }
      },
      rows: [],
      groups: SCHEDULE_TRACKS.map((track) => ({
        id: track.value,
        title: t(track.label),
        // The one group whose rules do not hold says so in place of the list's note.
        note: track.value === "ONE_SHOT" ? t("next.schedules.oneShotNote") : undefined,
        rows: schedules
          .filter((schedule) => schedule.track === track.value)
          .map((schedule) => row(schedule)),
      })),
    },
  ];

  const optionFields = {
    t,
    action: form.actionType,
    value: form.options,
    onChange: (options: ScheduleOptionsForm) => setForm((current) => ({ ...current, options })),
  };
  // The editor says how long a rule lasts in the words its group uses in the list.
  const runsOnce = isOneShot(form.actionType);

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
        note={editingId === "new" ? t("next.schedules.newNote") : scheduleDaysLabel(t, editing?.days ?? [])}
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
                control: {
                  kind: "time",
                  value: form.time,
                  onChange: (next: string) => setForm((current) => ({ ...current, time: next })),
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
                      {SCHEDULE_DAYS.map((day) => {
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
                  options: SCHEDULE_ACTIONS.map((option) => ({ value: option.value, label: t(option.label) })),
                  onChange: (next) => setForm((current) => ({ ...current, actionType: next })),
                },
              },
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
          ...(form.actionType === "speed_limit"
            ? scheduleSpeedSections({
                ...optionFields,
                targets,
                // A rate typed after a refused save takes back what was said about it.
                onChange: (options) => {
                  setError(null);
                  optionFields.onChange(options);
                },
              })
            : []),
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
