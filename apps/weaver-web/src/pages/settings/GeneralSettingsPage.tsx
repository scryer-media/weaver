import { useEffect, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "urql";
import { FolderOpen } from "lucide-react";
import { DirectoryBrowserDialog } from "@/components/DirectoryBrowserDialog";
import { formatSpeed } from "@/components/SpeedDisplay";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
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
import { useTranslate, useLanguageSettings } from "@/lib/context/translate-context";
import { AVAILABLE_LANGUAGES } from "@/lib/i18n";

const MAX_SPEED = 100 * 1024 * 1024;

type GeneralSettings = {
  dataDir: string;
  intermediateDir: string;
  completeDir: string;
  cleanupAfterExtract: boolean;
  maxDownloadSpeed: number;
  maxRetries: number;
};

export function GeneralSettingsPage() {
  const t = useTranslate();
  const { uiLanguage, setLanguagePreference } = useLanguageSettings();
  const [{ data }, reexecuteQuery] = useQuery<{ settings: GeneralSettings }>({
    query: SETTINGS_QUERY,
  });
  const [updateState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);

  const [settings, setSettings] = useState<GeneralSettings | null>(null);
  const [speedValue, setSpeedValue] = useState(0);
  const [intermediateDir, setIntermediateDir] = useState("");
  const [completeDir, setCompleteDir] = useState("");
  const [cleanup, setCleanup] = useState(true);
  const [maxRetries, setMaxRetries] = useState(3);
  const [settingsSaved, setSettingsSaved] = useState(false);
  const [browserTarget, setBrowserTarget] = useState<"intermediate" | "complete" | null>(null);
  const [browsePath, setBrowsePath] = useState<string | null>(null);

  useEffect(() => {
    if (data?.settings) {
      setSettings(data.settings);
    }
  }, [data?.settings]);

  useEffect(() => {
    if (!settings) return;
    setSpeedValue(settings.maxDownloadSpeed ?? 0);
    setIntermediateDir(settings.intermediateDir ?? "");
    setCompleteDir(settings.completeDir ?? "");
    setCleanup(settings.cleanupAfterExtract ?? true);
    setMaxRetries(settings.maxRetries ?? 3);
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
    const nextIntermediateDir = intermediateDir.trim();
    const nextCompleteDir = completeDir.trim();
    await updateSettings({
      input: {
        intermediateDir: nextIntermediateDir ? nextIntermediateDir : null,
        completeDir: nextCompleteDir ? nextCompleteDir : null,
        cleanupAfterExtract: cleanup,
        maxDownloadSpeed: speedValue,
        maxRetries,
      },
    });
  };

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("settings.general")}
        description={t("settings.generalPageDesc")}
      />

      <Card>
        <CardHeader>
          <CardTitle>{t("settings.language")}</CardTitle>
          <CardDescription>{t("settings.languageDesc")}</CardDescription>
        </CardHeader>
        <CardContent>
          <Select value={uiLanguage} onValueChange={setLanguagePreference}>
            <SelectTrigger className="w-[220px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {AVAILABLE_LANGUAGES.map((lang) => (
                <SelectItem key={lang.code} value={lang.code}>
                  {lang.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>{t("settings.speedLimit")}</CardTitle>
          <CardDescription>{t("settings.speedLimitDesc")}</CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">
          <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-4">
            <div className="text-xs uppercase tracking-[0.18em] text-muted-foreground">
              {t("metrics.downloadSpeed")}
            </div>
            <div className="mt-2 text-2xl font-semibold text-foreground">
              {speedValue === 0 ? t("settings.unlimited") : formatSpeed(speedValue)}
            </div>
          </div>

          <div>
            <input
              type="range"
              min={0}
              max={MAX_SPEED}
              step={1024 * 1024}
              value={speedValue}
              onChange={(event) => setSpeedValue(Number(event.target.value))}
              className="w-full accent-primary"
            />
            <div className="mt-2 flex justify-between text-xs text-muted-foreground">
              <span>{t("settings.unlimited")}</span>
              <span>100 MB/s</span>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Button onClick={() => void persistSettings()} disabled={updateState.fetching}>
              {t("settings.applySpeedNow")}
            </Button>
            <Button variant="outline" onClick={() => setSpeedValue(0)}>
              {t("settings.resetSpeedLimit")}
            </Button>
            {settingsSaved ? (
              <span className="text-sm text-emerald-600 dark:text-emerald-300">
                {t("settings.saved")}
              </span>
            ) : null}
          </div>
        </CardContent>
      </Card>

      {settings ? (
        <Card>
          <CardHeader>
            <CardTitle>{t("settings.storageAndBehavior")}</CardTitle>
            <CardDescription>{t("settings.storageAndBehaviorDesc")}</CardDescription>
          </CardHeader>
          <CardContent className="space-y-5">
            <div className="grid gap-4 xl:grid-cols-2">
              <SettingField
                label={t("settings.dataDir")}
                description={t("settings.dataDirDesc")}
                staticValue={settings.dataDir}
              />
              <SettingField label={t("settings.intermediateDir")} description={t("settings.intermediateDirDesc")}>
                <div className="flex gap-2">
                  <Input
                    value={intermediateDir}
                    onChange={(event) => setIntermediateDir(event.target.value)}
                    placeholder={`${settings.dataDir}/intermediate`}
                  />
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => {
                      setBrowsePath(intermediateDir.trim() || null);
                      setBrowserTarget("intermediate");
                    }}
                  >
                    <FolderOpen className="size-4" />
                    {t("categories.browse")}
                  </Button>
                </div>
              </SettingField>
              <SettingField label={t("settings.completeDir")} description={t("settings.completeDirDesc")}>
                <div className="flex gap-2">
                  <Input
                    value={completeDir}
                    onChange={(event) => setCompleteDir(event.target.value)}
                    placeholder={`${settings.dataDir}/complete`}
                  />
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => {
                      setBrowsePath(completeDir.trim() || null);
                      setBrowserTarget("complete");
                    }}
                  >
                    <FolderOpen className="size-4" />
                    {t("categories.browse")}
                  </Button>
                </div>
              </SettingField>
              <SettingField label={t("settings.maxRetries")} description={t("settings.maxRetriesDesc")}>
                <Input
                  type="number"
                  min={0}
                  max={20}
                  value={maxRetries}
                  onChange={(event) => setMaxRetries(Number(event.target.value))}
                  className="max-w-32"
                />
              </SettingField>
            </div>

            <div className="flex flex-col gap-4 rounded-2xl border border-border/70 bg-background/70 p-4 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <div className="text-sm font-medium text-foreground">
                  {t("settings.cleanupAfterExtract")}
                </div>
                <div className="text-xs text-muted-foreground">
                  {t("settings.cleanupDesc")}
                </div>
              </div>
              <Switch checked={cleanup} onCheckedChange={setCleanup} />
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <Button onClick={() => void persistSettings()} disabled={updateState.fetching}>
                {t("settings.save")}
              </Button>
              {settingsSaved ? (
                <span className="text-sm text-emerald-600 dark:text-emerald-300">
                  {t("settings.saved")}
                </span>
              ) : null}
            </div>

            <DirectoryBrowserDialog
              open={browserTarget != null}
              path={browsePath}
              onPathChange={setBrowsePath}
              onClose={() => setBrowserTarget(null)}
              onChoose={(nextPath) => {
                if (browserTarget === "intermediate") setIntermediateDir(nextPath);
                else if (browserTarget === "complete") setCompleteDir(nextPath);
                setBrowserTarget(null);
              }}
            />
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}

function SettingField({
  label,
  description,
  children,
  staticValue,
}: {
  label: string;
  description: string;
  children?: ReactNode;
  staticValue?: string;
}) {
  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
      <Label className="mb-2">{label}</Label>
      {staticValue ? (
        <div className="rounded-md border border-input bg-field/50 px-3 py-2 text-sm text-muted-foreground">
          {staticValue}
        </div>
      ) : (
        children
      )}
      <p className="mt-2 text-xs text-muted-foreground">{description}</p>
    </div>
  );
}
