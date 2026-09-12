import { useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  CREATE_SCHEDULE_MUTATION,
  DELETE_SCHEDULE_MUTATION,
  SCHEDULES_QUERY,
  TOGGLE_SCHEDULE_MUTATION,
  UPDATE_SCHEDULE_MUTATION,
} from "@/graphql/queries";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor } from "../../../components/RecordEditor";
import { SecondaryButton, Toggle } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { formatRate } from "../../../data/format";
import { PanelControls, SettingsBlocks, type SettingsBlock } from "../framework";

/**
 * Schedules: a clock that pauses, resumes or throttles the queue.
 *
 * Weekdays are a set rather than a list of rules, so the editor draws them as
 * seven toggling chips — the one control in the design system that repeats
 * horizontally — and "no day selected" means every day.
 */

interface Schedule {
  id: string;
  enabled: boolean;
  label: string | null;
  days: string[];
  time: string;
  actionType: string;
  speedLimitBytes: number | null;
}

interface ScheduleForm {
  enabled: boolean;
  label: string;
  days: string[];
  time: string;
  actionType: string;
  speedMib: string;
  speedUnlimited: boolean;
}

const MIB = 1024 * 1024;

const DAYS = [
  { key: "mon", label: "Mon" },
  { key: "tue", label: "Tue" },
  { key: "wed", label: "Wed" },
  { key: "thu", label: "Thu" },
  { key: "fri", label: "Fri" },
  { key: "sat", label: "Sat" },
  { key: "sun", label: "Sun" },
];

const ACTIONS: { value: string; label: string }[] = [
  { value: "pause", label: "Pause downloads" },
  { value: "resume", label: "Resume downloads" },
  { value: "speed_limit", label: "Set a speed limit" },
];

const NEW_SCHEDULE: ScheduleForm = {
  enabled: true,
  label: "",
  days: [],
  time: "08:00",
  actionType: "pause",
  speedMib: "5",
  speedUnlimited: false,
};

function actionLabel(schedule: Schedule): string {
  if (schedule.actionType === "speed_limit") {
    return schedule.speedLimitBytes ? `Limit to ${formatRate(schedule.speedLimitBytes)}` : "Remove the limit";
  }
  return schedule.actionType === "pause" ? "Pause downloads" : "Resume downloads";
}

function daysLabel(days: string[]): string {
  if (days.length === 0 || days.length === DAYS.length) {
    return "Every day";
  }
  return DAYS.filter((day) => days.includes(day.key))
    .map((day) => day.label)
    .join(" ");
}

export function SchedulesPanel() {
  const [{ data }, reexecute] = useQuery<{ schedules: Schedule[] }>({ query: SCHEDULES_QUERY });
  const [, createSchedule] = useMutation(CREATE_SCHEDULE_MUTATION);
  const [, updateSchedule] = useMutation(UPDATE_SCHEDULE_MUTATION);
  const [, deleteSchedule] = useMutation(DELETE_SCHEDULE_MUTATION);
  const [, toggleSchedule] = useMutation(TOGGLE_SCHEDULE_MUTATION);

  const [editingId, setEditingId] = useState<string | "new" | null>(null);
  const [form, setForm] = useState<ScheduleForm>(NEW_SCHEDULE);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmRemove, setConfirmRemove] = useState<Schedule | null>(null);

  const schedules = data?.schedules ?? [];
  const editing = schedules.find((entry) => entry.id === editingId) ?? null;

  const open = (schedule: Schedule | null) => {
    setError(null);
    setForm(
      schedule
        ? {
            enabled: schedule.enabled,
            label: schedule.label ?? "",
            days: schedule.days,
            time: schedule.time,
            actionType: schedule.actionType,
            speedUnlimited:
              schedule.actionType === "speed_limit" && !schedule.speedLimitBytes,
            speedMib: schedule.speedLimitBytes ? String(schedule.speedLimitBytes / MIB) : "5",
          }
        : NEW_SCHEDULE,
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
    };
    if (form.actionType === "speed_limit") {
      input.speedLimitBytes = form.speedUnlimited
        ? 0
        : Math.round(Number.parseFloat(form.speedMib || "0") * MIB);
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
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const remove = async () => {
    if (!confirmRemove) {
      return;
    }
    setBusy(true);
    await deleteSchedule({ id: confirmRemove.id });
    setBusy(false);
    setConfirmRemove(null);
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "schedules",
      title: "Schedules",
      note: "local time, applied in order",
      columns: "84px minmax(0, 1.1fr) minmax(0, 1fr) minmax(0, 1fr) 44px",
      headers: ["Time", "Days", "Action", "Label", ""],
      empty: "No schedules yet. Weaver downloads whenever there is work.",
      onRowClick: (id) => {
        const schedule = schedules.find((entry) => entry.id === id);
        if (schedule) {
          open(schedule);
        }
      },
      rows: schedules.map((schedule) => ({
        id: schedule.id,
        searchText: `${schedule.time} ${daysLabel(schedule.days)} ${actionLabel(schedule)} ${schedule.label ?? ""}`,
        cells: [
          <Cell key="time" mono className="text-wv-fg">
            {schedule.time}
          </Cell>,
          <Cell key="days" mono className="text-wv-secondary">
            {daysLabel(schedule.days)}
          </Cell>,
          <Cell key="action">{actionLabel(schedule)}</Cell>,
          <Cell key="label" className="text-wv-muted">
            {schedule.label || "—"}
          </Cell>,
          <span key="enabled" onClick={(event) => event.stopPropagation()}>
            <Toggle
              size="table"
              checked={schedule.enabled}
              label={`${schedule.time} schedule enabled`}
              onChange={(next) => {
                void toggleSchedule({ id: schedule.id, enabled: next }).then(() =>
                  reexecute({ requestPolicy: "network-only" }),
                );
              }}
            />
          </span>,
        ],
      })),
    },
  ];

  return (
    <>
      <PanelControls>
        <SecondaryButton onClick={() => open(null)}>Add schedule</SecondaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} />

      <RecordEditor
        open={editingId !== null}
        title={editingId === "new" ? "Add schedule" : (editing?.label || editing?.time || "Schedule")}
        note={editingId === "new" ? "new schedule" : daysLabel(editing?.days ?? [])}
        error={error}
        busy={busy}
        onSave={() => void save()}
        onDismiss={() => setEditingId(null)}
        onDelete={editing ? () => setConfirmRemove(editing) : undefined}
        deleteLabel="Remove schedule"
        sections={[
          {
            id: "when",
            title: "When",
            fields: [
              {
                id: "time",
                label: "Time",
                help: "Local time, 24-hour.",
                control: {
                  kind: "time",
                  value: form.time,
                  onChange: (next) => setForm((current) => ({ ...current, time: next })),
                },
              },
              {
                id: "days",
                label: "Days",
                help: "Leave every day off to run this on all of them.",
                control: {
                  kind: "custom",
                  control: (
                    <div className="flex flex-wrap justify-end gap-1.5">
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
                                ? "border-wv-accent bg-wv-segment-active font-semibold text-wv-strong"
                                : "border-wv-control bg-wv-input text-wv-muted hover:border-wv-control-hover"
                            }`}
                          >
                            {day.label}
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
            title: "What happens",
            fields: [
              {
                id: "actionType",
                label: "Action",
                control: {
                  kind: "select",
                  value: form.actionType,
                  options: ACTIONS,
                  onChange: (next) => setForm((current) => ({ ...current, actionType: next })),
                },
              },
              ...(form.actionType === "speed_limit"
                ? [
                    {
                      id: "speedUnlimited",
                      label: "Remove the limit instead",
                      help: "Use this for the entry that ends an off-peak window.",
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
                            label: "Speed limit",
                            help: "Mebibytes per second.",
                            control: {
                              kind: "text" as const,
                              value: form.speedMib,
                              className: "w-[110px]",
                              onChange: (next: string) =>
                                setForm((current) => ({ ...current, speedMib: next })),
                            },
                          },
                        ]),
                  ]
                : []),
              {
                id: "label",
                label: "Label",
                help: "Optional. Shown in the table so a pair of entries reads as one window.",
                control: {
                  kind: "text",
                  mono: false,
                  value: form.label,
                  placeholder: "Off-peak start",
                  onChange: (next) => setForm((current) => ({ ...current, label: next })),
                },
              },
              {
                id: "enabled",
                label: "Enabled",
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
        title="Remove schedule"
        note={confirmRemove?.time}
        busy={busy}
        confirmLabel="Remove schedule"
        body="The queue keeps whatever state it is in now; nothing else changes."
        onConfirm={() => void remove()}
        onDismiss={() => setConfirmRemove(null)}
      />
    </>
  );
}
