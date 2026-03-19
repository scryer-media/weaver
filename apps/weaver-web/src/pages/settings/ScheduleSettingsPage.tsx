import { useState } from "react";
import { useMutation, useQuery } from "urql";
import { Clock, Plus, Trash2 } from "lucide-react";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
import {
  SCHEDULES_QUERY,
  CREATE_SCHEDULE_MUTATION,
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
  const [, deleteSchedule] = useMutation(DELETE_SCHEDULE_MUTATION);
  const [, toggleSchedule] = useMutation(TOGGLE_SCHEDULE_MUTATION);

  const [showForm, setShowForm] = useState(false);
  const [formTime, setFormTime] = useState("08:00");
  const [formAction, setFormAction] = useState("pause");
  const [formDays, setFormDays] = useState<string[]>([]);
  const [formLabel, setFormLabel] = useState("");
  const [formSpeed, setFormSpeed] = useState("5");

  const schedules: Schedule[] = result.data?.schedules ?? [];

  const handleCreate = async () => {
    const input: Record<string, unknown> = {
      time: formTime,
      actionType: formAction,
      days: formDays.length > 0 ? formDays : null,
      label: formLabel || null,
      enabled: true,
    };
    if (formAction === "speed_limit") {
      input.speedLimitBytes = parseFloat(formSpeed) * 1024 * 1024;
    }
    await createSchedule({ input });
    reexecute({ requestPolicy: "network-only" });
    setShowForm(false);
    setFormLabel("");
    setFormDays([]);
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
    <div className="space-y-6">
      <PageHeader
        title={t("schedule.title")}
        description={t("schedule.desc")}
      />

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>{t("schedule.entries")}</CardTitle>
          <Button size="sm" onClick={() => setShowForm(!showForm)}>
            <Plus className="mr-1 h-4 w-4" />
            {t("schedule.add")}
          </Button>
        </CardHeader>
        <CardContent className="space-y-4">
          {showForm && (
            <div className="rounded-lg border border-border bg-muted/30 p-4 space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label>{t("schedule.time")}</Label>
                  <Input
                    type="time"
                    value={formTime}
                    onChange={(e) => setFormTime(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label>{t("schedule.action")}</Label>
                  <Select value={formAction} onValueChange={setFormAction}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="pause">{t("schedule.actionPause")}</SelectItem>
                      <SelectItem value="resume">{t("schedule.actionResume")}</SelectItem>
                      <SelectItem value="speed_limit">{t("schedule.actionSpeedLimit")}</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {formAction === "speed_limit" && (
                <div className="space-y-2">
                  <Label>{t("schedule.speedLimit")}</Label>
                  <div className="flex items-center gap-2">
                    <Input
                      type="number"
                      value={formSpeed}
                      onChange={(e) => setFormSpeed(e.target.value)}
                      className="w-24"
                    />
                    <span className="text-sm text-muted-foreground">MB/s</span>
                  </div>
                </div>
              )}

              <div className="space-y-2">
                <Label>{t("schedule.days")}</Label>
                <div className="flex gap-2">
                  {ALL_DAYS.map((day) => (
                    <label
                      key={day}
                      className="flex items-center gap-1.5 text-sm cursor-pointer"
                    >
                      <Checkbox
                        checked={formDays.includes(day)}
                        onCheckedChange={() => toggleDay(day)}
                      />
                      {DAY_LABELS[day]}
                    </label>
                  ))}
                </div>
                <p className="text-xs text-muted-foreground">{t("schedule.daysHint")}</p>
              </div>

              <div className="space-y-2">
                <Label>{t("schedule.label")}</Label>
                <Input
                  value={formLabel}
                  onChange={(e) => setFormLabel(e.target.value)}
                  placeholder={t("schedule.labelPlaceholder")}
                />
              </div>

              <div className="flex gap-2">
                <Button size="sm" onClick={handleCreate}>
                  {t("schedule.create")}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => setShowForm(false)}
                >
                  {t("action.cancel")}
                </Button>
              </div>
            </div>
          )}

          {schedules.length === 0 && !showForm && (
            <EmptyState
              icon={<Clock className="h-10 w-10" />}
              title={t("schedule.empty")}
              description={t("schedule.emptyDesc")}
            />
          )}

          {schedules.map((entry) => (
            <div
              key={entry.id}
              className="flex items-center justify-between rounded-lg border border-border p-3"
            >
              <div className="flex items-center gap-3">
                <Switch
                  checked={entry.enabled}
                  onCheckedChange={(checked) =>
                    handleToggle(entry.id, checked)
                  }
                />
                <div>
                  <div className="flex items-center gap-2 text-sm font-medium">
                    <span className="font-mono">{entry.time}</span>
                    <span className="capitalize">
                      {entry.actionType === "speed_limit"
                        ? `${t("schedule.actionSpeedLimit")}: ${formatSpeed(entry.speedLimitBytes ?? 0)}`
                        : entry.actionType === "pause"
                          ? t("schedule.actionPause")
                          : t("schedule.actionResume")}
                    </span>
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {entry.days.length > 0
                      ? entry.days.map((d) => DAY_LABELS[d] ?? d).join(", ")
                      : t("schedule.everyDay")}
                    {entry.label ? ` \u2014 ${entry.label}` : ""}
                  </div>
                </div>
              </div>
              <Button
                size="icon"
                variant="ghost"
                onClick={() => handleDelete(entry.id)}
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}
