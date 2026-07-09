import { useEffect, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "urql";
import { formatBytes } from "@/components/SpeedDisplay";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { SettingsInnerBox } from "@/pages/settings/shared";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  SETTINGS_QUERY,
  UPDATE_SETTINGS_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

type GeneralSettings = {
  dataDir: string;
  intermediateDir: string;
  completeDir: string;
  cleanupAfterExtract: boolean;
  maxDownloadSpeed: number;
  maxRetries: number;
  ispBandwidthCap?: {
    enabled: boolean;
    period: "DAILY" | "WEEKLY" | "MONTHLY";
    limitBytes: number;
    resetTimeMinutesLocal: number;
    weeklyResetWeekday: "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN";
    monthlyResetDay: number;
  } | null;
};

type DownloadBlock = {
  kind: "NONE" | "MANUAL_PAUSE" | "ISP_CAP";
  capEnabled: boolean;
  usedBytes: number;
  limitBytes: number;
  remainingBytes: number;
  reservedBytes: number;
  windowEndsAtEpochMs?: number | null;
  timezoneName: string;
};

type SettingsQueryData = {
  settings: GeneralSettings;
  globalState?: {
    downloadBlock: DownloadBlock;
  } | null;
};

const BANDWIDTH_UNITS = [
  { label: "GB", value: 1024 ** 3 },
  { label: "TB", value: 1024 ** 4 },
] as const;

function bytesToDisplay(bytes: number) {
  if (bytes >= 1024 ** 4) {
    return { value: Number((bytes / 1024 ** 4).toFixed(2)), unit: "TB" as const };
  }
  return { value: Number((bytes / 1024 ** 3).toFixed(2)), unit: "GB" as const };
}

function displayToBytes(value: number, unit: (typeof BANDWIDTH_UNITS)[number]["label"]) {
  const multiplier = BANDWIDTH_UNITS.find((entry) => entry.label === unit)?.value ?? 1024 ** 3;
  return Math.max(0, Math.round(value * multiplier));
}

function minutesToTimeInput(minutes: number) {
  const clamped = Math.max(0, Math.min(23 * 60 + 59, minutes));
  const hours = Math.floor(clamped / 60)
    .toString()
    .padStart(2, "0");
  const mins = (clamped % 60).toString().padStart(2, "0");
  return `${hours}:${mins}`;
}

function timeInputToMinutes(raw: string) {
  const [hours, minutes] = raw.split(":").map((value) => Number(value));
  if (Number.isNaN(hours) || Number.isNaN(minutes)) return 0;
  return Math.max(0, Math.min(23 * 60 + 59, hours * 60 + minutes));
}

export function BandwidthCapSettingsPage() {
  const t = useTranslate();
  const [{ data }, reexecuteQuery] = useQuery<SettingsQueryData>({
    query: SETTINGS_QUERY,
  });
  const [updateState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);

  const [settings, setSettings] = useState<GeneralSettings | null>(null);
  const [capEnabled, setCapEnabled] = useState(false);
  const [capPeriod, setCapPeriod] = useState<"DAILY" | "WEEKLY" | "MONTHLY">("MONTHLY");
  const [capLimitValue, setCapLimitValue] = useState(1);
  const [capLimitUnit, setCapLimitUnit] = useState<"GB" | "TB">("GB");
  const [capResetTime, setCapResetTime] = useState("00:00");
  const [capWeeklyResetWeekday, setCapWeeklyResetWeekday] = useState<
    "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN"
  >("MON");
  const [capMonthlyResetDay, setCapMonthlyResetDay] = useState(1);
  const [settingsSaved, setSettingsSaved] = useState(false);

  useEffect(() => {
    if (data?.settings) {
      setSettings(data.settings);
    }
  }, [data?.settings]);

  useEffect(() => {
    if (!settings) return;
    const cap = settings.ispBandwidthCap ?? null;
    if (cap) {
      const display = bytesToDisplay(cap.limitBytes);
      setCapEnabled(cap.enabled);
      setCapPeriod(cap.period);
      setCapLimitValue(display.value);
      setCapLimitUnit(display.unit);
      setCapResetTime(minutesToTimeInput(cap.resetTimeMinutesLocal));
      setCapWeeklyResetWeekday(cap.weeklyResetWeekday);
      setCapMonthlyResetDay(cap.monthlyResetDay);
    }
  }, [settings]);

  useEffect(() => {
    if (updateState.data?.updateSettings) {
      setSettings(updateState.data.updateSettings);
      setSettingsSaved(true);
      reexecuteQuery({ requestPolicy: "network-only" });
      const timeout = window.setTimeout(() => setSettingsSaved(false), 2000);
      return () => window.clearTimeout(timeout);
    }
  }, [updateState.data, reexecuteQuery]);

  const persistSettings = async () => {
    await updateSettings({
      input: {
        ispBandwidthCap: {
          enabled: capEnabled,
          period: capPeriod,
          limitBytes: displayToBytes(capLimitValue, capLimitUnit),
          resetTimeMinutesLocal: timeInputToMinutes(capResetTime),
          weeklyResetWeekday: capWeeklyResetWeekday,
          monthlyResetDay: capMonthlyResetDay,
        },
      },
    });
  };

  const downloadBlock = data?.globalState?.downloadBlock;
  const capResetAt = downloadBlock?.windowEndsAtEpochMs
    ? new Date(downloadBlock.windowEndsAtEpochMs).toLocaleString([], {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      })
    : "\u2014";

  const usageFillPct = downloadBlock?.capEnabled
    ? Math.min(
        100,
        Math.max(
          0,
          ((downloadBlock?.usedBytes ?? 0) / Math.max(downloadBlock?.limitBytes ?? 1, 1)) * 100,
        ),
      )
    : 0;
  const usageBarClass =
    usageFillPct >= 90
      ? "bg-status-failed"
      : usageFillPct >= 70
        ? "bg-status-paused"
        : "bg-status-completed";

  return (
    <div className="max-w-[1180px] space-y-6">
      <PageHeader
        title={t("settings.bandwidthCap")}
        description={t("settings.bandwidthCapDesc")}
      />

      <SectionCard title={t("settings.bandwidthCapCurrentWindow")}>
        <div className="text-xs text-muted-foreground">
          {t("settings.bandwidthCapTimezone", {
            timezone: downloadBlock?.timezoneName ?? "\u2014",
          })}
        </div>
        <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div>
            <div className="text-[10.5px] font-semibold uppercase tracking-[0.13em] text-muted-foreground">
              {t("settings.bandwidthCapUsed")}
            </div>
            <div className="mt-1 text-base font-semibold text-foreground">
              {formatBytes(downloadBlock?.usedBytes ?? 0)}
            </div>
          </div>
          <div>
            <div className="text-[10.5px] font-semibold uppercase tracking-[0.13em] text-muted-foreground">
              {t("settings.bandwidthCapRemaining")}
            </div>
            <div className="mt-1 text-base font-semibold text-foreground">
              {downloadBlock?.capEnabled
                ? formatBytes(downloadBlock?.remainingBytes ?? 0)
                : "\u2014"}
            </div>
          </div>
          <div>
            <div className="text-[10.5px] font-semibold uppercase tracking-[0.13em] text-muted-foreground">
              {t("settings.bandwidthCapLimit")}
            </div>
            <div className="mt-1 text-base font-semibold text-foreground">
              {downloadBlock?.capEnabled
                ? formatBytes(downloadBlock?.limitBytes ?? 0)
                : "\u2014"}
            </div>
          </div>
          <div>
            <div className="text-[10.5px] font-semibold uppercase tracking-[0.13em] text-muted-foreground">
              {t("settings.bandwidthCapReset")}
            </div>
            <div className="mt-1 text-base font-semibold text-foreground">
              {capResetAt}
            </div>
          </div>
        </div>
        <div className="mt-4 h-2 overflow-hidden rounded-pill bg-muted">
          <div
            className={cn("h-full transition-all", usageBarClass)}
            style={{
              width: downloadBlock?.capEnabled ? `${usageFillPct}%` : "0%",
            }}
          />
        </div>
      </SectionCard>

      <SectionCard
        title={t("settings.bandwidthCapConfiguration")}
        description={t("settings.bandwidthCapEnabledDesc")}
      >
        <div className="space-y-5">
          <div className="flex items-center justify-between gap-4 rounded-inner border border-border p-5">
            <div>
              <div className="text-sm font-semibold text-foreground">
                {t("settings.bandwidthCapEnabled")}
              </div>
              <div className="mt-1 text-[12.5px] text-muted-foreground">
                {t("settings.bandwidthCapEnabledDesc")}
              </div>
            </div>
            <Switch checked={capEnabled} onCheckedChange={setCapEnabled} />
          </div>

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            <SettingField
              label={t("settings.bandwidthCapPeriod")}
              description={t("settings.bandwidthCapPeriodDesc")}
            >
              <Select
                value={capPeriod}
                onValueChange={(value: "DAILY" | "WEEKLY" | "MONTHLY") => setCapPeriod(value)}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="DAILY">{t("settings.bandwidthCapDaily")}</SelectItem>
                  <SelectItem value="WEEKLY">{t("settings.bandwidthCapWeekly")}</SelectItem>
                  <SelectItem value="MONTHLY">{t("settings.bandwidthCapMonthly")}</SelectItem>
                </SelectContent>
              </Select>
            </SettingField>
            <SettingField
              label={t("settings.bandwidthCapLimit")}
              description={t("settings.bandwidthCapLimitDesc")}
            >
              <div className="flex gap-2">
                <Input
                  type="number"
                  min={0}
                  step="0.1"
                  value={capLimitValue}
                  onChange={(event) => setCapLimitValue(Number(event.target.value))}
                />
                <Select
                  value={capLimitUnit}
                  onValueChange={(value: "GB" | "TB") => setCapLimitUnit(value)}
                >
                  <SelectTrigger className="w-28">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {BANDWIDTH_UNITS.map((unit) => (
                      <SelectItem key={unit.label} value={unit.label}>
                        {unit.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </SettingField>
            <SettingField
              label={t("settings.bandwidthCapResetTime")}
              description={t("settings.bandwidthCapResetTimeDesc")}
            >
              <Input
                type="time"
                value={capResetTime}
                onChange={(event) => setCapResetTime(event.target.value)}
              />
            </SettingField>
            {capPeriod === "WEEKLY" ? (
              <SettingField
                label={t("settings.bandwidthCapWeeklyDay")}
                description={t("settings.bandwidthCapWeeklyDayDesc")}
              >
                <Select
                  value={capWeeklyResetWeekday}
                  onValueChange={(
                    value: "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN",
                  ) => setCapWeeklyResetWeekday(value)}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="MON">Monday</SelectItem>
                    <SelectItem value="TUE">Tuesday</SelectItem>
                    <SelectItem value="WED">Wednesday</SelectItem>
                    <SelectItem value="THU">Thursday</SelectItem>
                    <SelectItem value="FRI">Friday</SelectItem>
                    <SelectItem value="SAT">Saturday</SelectItem>
                    <SelectItem value="SUN">Sunday</SelectItem>
                  </SelectContent>
                </Select>
              </SettingField>
            ) : null}
            {capPeriod === "MONTHLY" ? (
              <SettingField
                label={t("settings.bandwidthCapMonthlyDay")}
                description={t("settings.bandwidthCapMonthlyDayDesc")}
              >
                <Input
                  type="number"
                  min={1}
                  max={31}
                  value={capMonthlyResetDay}
                  onChange={(event) => setCapMonthlyResetDay(Number(event.target.value))}
                  className="max-w-32"
                />
              </SettingField>
            ) : null}
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Button onClick={() => void persistSettings()} disabled={updateState.fetching}>
              {t("settings.save")}
            </Button>
            {settingsSaved ? (
              <span className="text-sm text-status-completed">
                {t("settings.saved")}
              </span>
            ) : null}
          </div>
        </div>
      </SectionCard>
    </div>
  );
}

function SettingField({
  label,
  description,
  children,
}: {
  label: string;
  description: string;
  children?: ReactNode;
}) {
  return (
    <SettingsInnerBox>
      <Label className="mb-2 text-sm font-semibold">{label}</Label>
      {children}
      <p className="mt-2 text-[12.5px] text-muted-foreground">{description}</p>
    </SettingsInnerBox>
  );
}
