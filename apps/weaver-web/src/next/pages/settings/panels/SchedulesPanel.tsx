import { useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  CREATE_SCHEDULE_MUTATION,
  DELETE_SCHEDULE_MUTATION,
  SCHEDULES_QUERY,
  TOGGLE_SCHEDULE_MUTATION,
  UPDATE_SCHEDULE_MUTATION,
} from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor } from "../../../components/RecordEditor";
import { PrimaryButton, Toggle } from "../../../components/controls";
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
  { value: "pause", label: "next.schedules.pause" },
  { value: "resume", label: "next.schedules.resume" },
  { value: "speed_limit", label: "next.schedules.setLimit" },
  { value: "pause_watch_folder_scanning", label: "next.schedules.pauseWatchFolder" },
  { value: "resume_watch_folder_scanning", label: "next.schedules.resumeWatchFolder" },
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

function actionLabel(t: Translate, schedule: Schedule): string {
  if (schedule.actionType === "speed_limit") {
    return schedule.speedLimitBytes
      ? t("next.schedules.limitTo", { rate: formatRate(schedule.speedLimitBytes) })
      : t("next.schedules.removeLimit");
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
  const [{ data, fetching }, reexecute] = useQuery<{ schedules: Schedule[] }>({ query: SCHEDULES_QUERY });
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
      title: t("next.settings.panel.schedules"),
      note: t("next.schedules.note"),
      columns: "84px minmax(0, 1.1fr) minmax(0, 1fr) minmax(0, 1fr) 44px",
      headers: [
        t("next.schedules.time"),
        t("next.schedules.days"),
        t("next.schedules.action"),
        t("next.schedules.label"),
        "",
      ],
      empty: t("next.schedules.empty"),
      emptyAction: { label: t("next.schedules.add"), onClick: () => open(null) },
      onRowClick: (id) => {
        const schedule = schedules.find((entry) => entry.id === id);
        if (schedule) {
          open(schedule);
        }
      },
      rows: schedules.map((schedule) => ({
        id: schedule.id,
        searchText: `${schedule.time} ${daysLabel(t, schedule.days)} ${actionLabel(t, schedule)} ${schedule.label ?? ""}`,
        cells: [
          <Cell key="time" mono className="text-wv-fg">
            {schedule.time}
          </Cell>,
          <Cell key="days" mono className="text-wv-secondary">
            {daysLabel(t, schedule.days)}
          </Cell>,
          <Cell key="action">{actionLabel(t, schedule)}</Cell>,
          <Cell key="label" className="text-wv-muted">
            {schedule.label || "—"}
          </Cell>,
          <span key="enabled" onClick={(event) => event.stopPropagation()}>
            <Toggle
              size="table"
              checked={schedule.enabled}
              label={t("next.schedules.enabledAria", { time: schedule.time })}
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
            fields: [
              {
                id: "time",
                label: t("next.schedules.time"),
                help: t("next.schedules.timeHelp"),
                control: {
                  kind: "time",
                  value: form.time,
                  onChange: (next) => setForm((current) => ({ ...current, time: next })),
                },
              },
              {
                id: "days",
                label: t("next.schedules.days"),
                help: t("next.schedules.daysHelp"),
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
                control: {
                  kind: "select",
                  value: form.actionType,
                  options: ACTIONS.map((option) => ({ ...option, label: t(option.label) })),
                  onChange: (next) => setForm((current) => ({ ...current, actionType: next })),
                },
              },
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
