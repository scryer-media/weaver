import { useState } from "react";
import { useMutation, useQuery } from "urql";
import { Pencil, Plus, Trash2 } from "lucide-react";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { formatSpeed } from "@/components/SpeedDisplay";
import { cn } from "@/lib/utils";
import {
  SCHEDULES_QUERY,
  CREATE_SCHEDULE_MUTATION,
  UPDATE_SCHEDULE_MUTATION,
  DELETE_SCHEDULE_MUTATION,
  TOGGLE_SCHEDULE_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

type Schedule = {
  id: string;
  enabled: boolean;
  label: string;
  days: string[];
  time: string;
  actionType: string;
  speedLimitBytes: number | null;
};

const ALL_DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"] as const;
const DAY_LABELS: Record<string, string> = {
  mon: "Mon",
  tue: "Tue",
  wed: "Wed",
  thu: "Thu",
  fri: "Fri",
  sat: "Sat",
  sun: "Sun",
};

export function ScheduleSettingsPage() {
  const t = useTranslate();
  const [result, reexecute] = useQuery({ query: SCHEDULES_QUERY });
  const [, createSchedule] = useMutation(CREATE_SCHEDULE_MUTATION);
  const [, updateSchedule] = useMutation(UPDATE_SCHEDULE_MUTATION);
  const [, deleteSchedule] = useMutation(DELETE_SCHEDULE_MUTATION);
  const [, toggleSchedule] = useMutation(TOGGLE_SCHEDULE_MUTATION);

  const [showForm, setShowForm] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [formEnabled, setFormEnabled] = useState(true);
  const [formTime, setFormTime] = useState("08:00");
  const [formAction, setFormAction] = useState("pause");
  const [formDays, setFormDays] = useState<string[]>([]);
  const [formLabel, setFormLabel] = useState("");
  const [formSpeed, setFormSpeed] = useState("5");
  const [formSpeedUnlimited, setFormSpeedUnlimited] = useState(false);

  const schedules: Schedule[] = result.data?.schedules ?? [];

  const resetForm = () => {
    setShowForm(false);
    setEditingId(null);
    setFormEnabled(true);
    setFormTime("08:00");
    setFormAction("pause");
    setFormLabel("");
    setFormDays([]);
    setFormSpeed("5");
    setFormSpeedUnlimited(false);
  };

  const openCreate = () => {
    resetForm();
    setShowForm(true);
  };

  const openEdit = (entry: Schedule) => {
    setEditingId(entry.id);
    setFormEnabled(entry.enabled);
    setFormTime(entry.time);
    setFormAction(entry.actionType);
    setFormDays(entry.days);
    setFormLabel(entry.label ?? "");
    if (entry.actionType === "speed_limit") {
      if (entry.speedLimitBytes === 0 || entry.speedLimitBytes == null) {
        setFormSpeedUnlimited(true);
        setFormSpeed("5");
      } else {
        setFormSpeedUnlimited(false);
        setFormSpeed(String(entry.speedLimitBytes / (1024 * 1024)));
      }
    } else {
      setFormSpeedUnlimited(false);
      setFormSpeed("5");
    }
    setShowForm(true);
  };

  const buildInput = () => {
    const input: Record<string, unknown> = {
      time: formTime,
      actionType: formAction,
      days: formDays.length > 0 ? formDays : null,
      label: formLabel || null,
      enabled: formEnabled,
    };
    if (formAction === "speed_limit") {
      input.speedLimitBytes = formSpeedUnlimited ? 0 : parseFloat(formSpeed) * 1024 * 1024;
    }
    return input;
  };

  const handleSave = async () => {
    const input = buildInput();
    if (editingId) {
      await updateSchedule({ id: editingId, input });
    } else {
      await createSchedule({ input });
    }
    reexecute({ requestPolicy: "network-only" });
    resetForm();
  };

  const handleDelete = async (id: string) => {
    await deleteSchedule({ id });
    reexecute({ requestPolicy: "network-only" });
  };

  const handleToggle = async (id: string, enabled: boolean) => {
    await toggleSchedule({ id, enabled });
    reexecute({ requestPolicy: "network-only" });
  };

  const toggleDay = (day: string) => {
    setFormDays((prev) =>
      prev.includes(day) ? prev.filter((d) => d !== day) : [...prev, day],
    );
  };

  return (
    <div className="max-w-[1180px] space-y-6">
      <PageHeader
        title={t("schedule.title")}
        description={t("schedule.desc")}
      />

      <SectionCard
        title={t("schedule.entries")}
        actions={
          <Button size="sm" onClick={openCreate}>
            <Plus className="mr-1 h-4 w-4" />
            {t("schedule.add")}
          </Button>
        }
      >
        <div className="space-y-4">
          {showForm && (
            <div className="space-y-4 rounded-inner border border-border bg-background/40 p-5">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="schedule-time" className="text-sm font-semibold">
                    {t("schedule.time")}
                  </Label>
                  <Input
                    id="schedule-time"
                    type="time"
                    value={formTime}
                    onChange={(e) => setFormTime(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="schedule-action" className="text-sm font-semibold">
                    {t("schedule.action")}
                  </Label>
                  <Select value={formAction} onValueChange={setFormAction}>
                    <SelectTrigger id="schedule-action">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="pause">{t("schedule.actionPause")}</SelectItem>
                      <SelectItem value="resume">{t("schedule.actionResume")}</SelectItem>
                      <SelectItem value="speed_limit">{t("schedule.actionSpeedLimit")}</SelectItem>
                      <SelectItem value="pause_watch_folder_scanning">
                        {t("schedule.actionPauseWatchFolder")}
                      </SelectItem>
                      <SelectItem value="resume_watch_folder_scanning">
                        {t("schedule.actionResumeWatchFolder")}
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {formAction === "speed_limit" && (
                <div className="space-y-2">
                  <Label className="text-sm font-semibold">{t("schedule.speedLimit")}</Label>
                  <div className="flex items-center gap-2">
                    <Input
                      type="number"
                      inputMode="decimal"
                      min="0"
                      value={formSpeed}
                      onChange={(e) => {
                        const val = e.target.value;
                        if (val === "" || Number(val) >= 0) setFormSpeed(val);
                      }}
                      className="w-24"
                      disabled={formSpeedUnlimited}
                    />
                    <span className="text-sm text-muted-foreground">MB/s</span>
                    <label className="ml-2 flex cursor-pointer items-center gap-1.5 text-sm">
                      <Checkbox
                        checked={formSpeedUnlimited}
                        onCheckedChange={(checked) => setFormSpeedUnlimited(checked === true)}
                      />
                      {t("settings.unlimited")}
                    </label>
                  </div>
                </div>
              )}

              <div className="space-y-2">
                <Label className="text-sm font-semibold">{t("schedule.days")}</Label>
                <div className="flex gap-2">
                  {ALL_DAYS.map((day) => (
                    <label
                      key={day}
                      className="flex cursor-pointer items-center gap-1.5 text-sm"
                    >
                      <Checkbox
                        checked={formDays.includes(day)}
                        onCheckedChange={() => toggleDay(day)}
                      />
                      {DAY_LABELS[day]}
                    </label>
                  ))}
                </div>
                <p className="text-[12.5px] text-muted-foreground">{t("schedule.daysHint")}</p>
              </div>

              <div className="space-y-2">
                <Label htmlFor="schedule-label" className="text-sm font-semibold">
                  {t("schedule.label")}
                </Label>
                <Input
                  id="schedule-label"
                  value={formLabel}
                  onChange={(e) => setFormLabel(e.target.value)}
                  placeholder={t("schedule.labelPlaceholder")}
                />
              </div>

              <div className="flex gap-2">
                <Button size="sm" onClick={handleSave}>
                  {editingId ? t("action.save") : t("schedule.create")}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={resetForm}
                >
                  {t("action.cancel")}
                </Button>
              </div>
            </div>
          )}

          {schedules.length === 0 && !showForm && (
            <EmptyState
              title={t("schedule.empty")}
              description={t("schedule.emptyDesc")}
            />
          )}

          {schedules.map((entry) => (
            <div
              key={entry.id}
              role="group"
              aria-label={entry.label || entry.time}
              className="flex items-center justify-between gap-4 rounded-inner border border-border p-4"
            >
              <div className="flex items-center gap-3">
                <span
                  className={cn(
                    "inline-flex items-center rounded-pill px-2.5 py-[3px] text-[10.5px] font-bold uppercase tracking-[0.06em]",
                    entry.enabled
                      ? "bg-status-completed/15 text-status-completed"
                      : "bg-muted text-muted-foreground",
                  )}
                >
                  {entry.enabled ? t("label.enabled") : t("label.disabled")}
                </span>
                <div>
                  <div className="flex items-center gap-2 text-sm font-semibold">
                    <span className="font-mono">{entry.time}</span>
                    <span className="capitalize">
                      {entry.actionType === "speed_limit"
                        ? `${t("schedule.actionSpeedLimit")}: ${entry.speedLimitBytes === 0 || entry.speedLimitBytes == null ? t("settings.unlimited") : formatSpeed(entry.speedLimitBytes)}`
                        : entry.actionType === "pause"
                          ? t("schedule.actionPause")
                          : entry.actionType === "resume"
                            ? t("schedule.actionResume")
                            : entry.actionType === "pause_watch_folder_scanning"
                              ? t("schedule.actionPauseWatchFolder")
                              : t("schedule.actionResumeWatchFolder")}
                    </span>
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    {entry.days.length > 0
                      ? entry.days.map((d) => DAY_LABELS[d] ?? d).join(", ")
                      : t("schedule.everyDay")}
                    {entry.label ? ` \u2014 ${entry.label}` : ""}
                  </div>
                </div>
              </div>
              <div className="flex items-center gap-1">
                <Switch
                  aria-label={entry.label || entry.time}
                  checked={entry.enabled}
                  onCheckedChange={(checked) =>
                    handleToggle(entry.id, checked)
                  }
                />
                <Button
                  aria-label={t("action.edit")}
                  size="icon"
                  variant="ghost"
                  onClick={() => openEdit(entry)}
                >
                  <Pencil className="h-4 w-4" />
                </Button>
                <Button
                  aria-label={t("action.delete")}
                  size="icon"
                  variant="ghost"
                  onClick={() => handleDelete(entry.id)}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            </div>
          ))}
        </div>
      </SectionCard>
    </div>
  );
}
