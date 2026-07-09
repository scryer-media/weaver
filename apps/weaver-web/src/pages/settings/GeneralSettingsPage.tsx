import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "urql";
import { FolderPathInput } from "@/components/FolderPathInput";
import { formatSpeed } from "@/components/SpeedDisplay";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { SettingsInnerBox } from "@/pages/settings/shared";
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
  ipReplacementTrialExtraConnections: number;
};

type StorageBehaviorDraft = {
  intermediateDir: string;
  completeDir: string;
  cleanupAfterExtract: boolean;
  maxRetries: number;
};

function normalizeStorageBehaviorDraft(draft: StorageBehaviorDraft): StorageBehaviorDraft {
  return {
    intermediateDir: draft.intermediateDir.trim(),
    completeDir: draft.completeDir.trim(),
    cleanupAfterExtract: draft.cleanupAfterExtract,
    maxRetries: Number.isFinite(draft.maxRetries) ? Math.max(0, Math.min(20, draft.maxRetries)) : 0,
  };
}

function storageBehaviorDraftFromSettings(settings: GeneralSettings): StorageBehaviorDraft {
  return normalizeStorageBehaviorDraft({
    intermediateDir: settings.intermediateDir ?? "",
    completeDir: settings.completeDir ?? "",
    cleanupAfterExtract: settings.cleanupAfterExtract ?? true,
    maxRetries: settings.maxRetries ?? 3,
  });
}

function storageBehaviorDraftKey(draft: StorageBehaviorDraft | null): string {
  return draft ? JSON.stringify(draft) : "";
}

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
  const [ipReplacementBurst, setIpReplacementBurst] = useState(0);
  const [speedSaved, setSpeedSaved] = useState(false);
  const [ipReplacementBurstSaved, setIpReplacementBurstSaved] = useState(false);
  const [storageSaveStatus, setStorageSaveStatus] = useState<"idle" | "saving" | "saved" | "error">("idle");
  const [storageSaveError, setStorageSaveError] = useState<string | null>(null);
  const speedSavedTimerRef = useRef<number | null>(null);
  const ipReplacementBurstSavedTimerRef = useRef<number | null>(null);
  const storageSaveTimerRef = useRef<number | null>(null);
  const storageSavedTimerRef = useRef<number | null>(null);
  const pendingStorageSaveRef = useRef<StorageBehaviorDraft | null>(null);
  const storageSaveInFlightRef = useRef(false);

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
    setIpReplacementBurst(settings.ipReplacementTrialExtraConnections ?? 0);
  }, [settings]);

  useEffect(() => {
    return () => {
      if (storageSaveTimerRef.current !== null) {
        window.clearTimeout(storageSaveTimerRef.current);
      }
      if (speedSavedTimerRef.current !== null) {
        window.clearTimeout(speedSavedTimerRef.current);
      }
      if (ipReplacementBurstSavedTimerRef.current !== null) {
        window.clearTimeout(ipReplacementBurstSavedTimerRef.current);
      }
      if (storageSavedTimerRef.current !== null) {
        window.clearTimeout(storageSavedTimerRef.current);
      }
    };
  }, []);

  const applyUpdatedSettings = useCallback((nextSettings: GeneralSettings | null | undefined) => {
    if (!nextSettings) {
      return;
    }
    setSettings(nextSettings);
    reexecuteQuery({ requestPolicy: "network-only" });
  }, [reexecuteQuery]);

  const pulseSpeedSaved = useCallback(() => {
    if (speedSavedTimerRef.current !== null) {
      window.clearTimeout(speedSavedTimerRef.current);
    }
    setSpeedSaved(true);
    speedSavedTimerRef.current = window.setTimeout(() => setSpeedSaved(false), 2000);
  }, []);

  const pulseIpReplacementBurstSaved = useCallback(() => {
    if (ipReplacementBurstSavedTimerRef.current !== null) {
      window.clearTimeout(ipReplacementBurstSavedTimerRef.current);
    }
    setIpReplacementBurstSaved(true);
    ipReplacementBurstSavedTimerRef.current = window.setTimeout(
      () => setIpReplacementBurstSaved(false),
      2000,
    );
  }, []);

  const currentStorageDraft = useMemo(
    () => normalizeStorageBehaviorDraft({
      intermediateDir,
      completeDir,
      cleanupAfterExtract: cleanup,
      maxRetries,
    }),
    [cleanup, completeDir, intermediateDir, maxRetries],
  );

  const persistedStorageDraft = useMemo(
    () => (settings ? storageBehaviorDraftFromSettings(settings) : null),
    [settings],
  );

  const setStorageSavedState = useCallback(() => {
    if (storageSavedTimerRef.current !== null) {
      window.clearTimeout(storageSavedTimerRef.current);
    }
    setStorageSaveStatus("saved");
    storageSavedTimerRef.current = window.setTimeout(() => {
      setStorageSaveStatus("idle");
    }, 2000);
  }, []);

  const flushPendingStorageSave = useCallback(async () => {
    if (storageSaveInFlightRef.current || pendingStorageSaveRef.current == null) {
      return;
    }

    storageSaveInFlightRef.current = true;

    while (pendingStorageSaveRef.current != null) {
      const nextDraft = pendingStorageSaveRef.current;
      pendingStorageSaveRef.current = null;

      if (storageSavedTimerRef.current !== null) {
        window.clearTimeout(storageSavedTimerRef.current);
      }
      setStorageSaveStatus("saving");
      setStorageSaveError(null);

      const result = await updateSettings({
        input: {
          intermediateDir: nextDraft.intermediateDir || null,
          completeDir: nextDraft.completeDir || null,
          cleanupAfterExtract: nextDraft.cleanupAfterExtract,
          maxRetries: nextDraft.maxRetries,
        },
      });

      if (result.error) {
        setStorageSaveStatus("error");
        setStorageSaveError(result.error.message ?? "Unable to save settings.");
        break;
      }

      applyUpdatedSettings(result.data?.updateSettings);
      setStorageSavedState();
    }

    storageSaveInFlightRef.current = false;
  }, [applyUpdatedSettings, setStorageSavedState, updateSettings]);

  useEffect(() => {
    if (!settings) {
      return;
    }

    if (storageBehaviorDraftKey(currentStorageDraft) === storageBehaviorDraftKey(persistedStorageDraft)) {
      return;
    }

    if (storageSaveTimerRef.current !== null) {
      window.clearTimeout(storageSaveTimerRef.current);
    }

    storageSaveTimerRef.current = window.setTimeout(() => {
      pendingStorageSaveRef.current = currentStorageDraft;
      void flushPendingStorageSave();
    }, 500);

    return () => {
      if (storageSaveTimerRef.current !== null) {
        window.clearTimeout(storageSaveTimerRef.current);
      }
    };
  }, [currentStorageDraft, flushPendingStorageSave, persistedStorageDraft, settings]);

  const applySpeedLimit = async () => {
    const result = await updateSettings({
      input: {
        maxDownloadSpeed: speedValue,
      },
    });

    if (result.data?.updateSettings) {
      applyUpdatedSettings(result.data.updateSettings);
      pulseSpeedSaved();
    }
  };

  const applyIpReplacementBurst = async () => {
    const value = Math.max(0, Math.min(1, Math.trunc(ipReplacementBurst)));
    const result = await updateSettings({
      input: {
        ipReplacementTrialExtraConnections: value,
      },
    });

    if (result.data?.updateSettings) {
      applyUpdatedSettings(result.data.updateSettings);
      pulseIpReplacementBurstSaved();
    }
  };

  return (
    <div className="max-w-[1180px] space-y-6">
      <PageHeader
        title={t("settings.general")}
        description={t("settings.generalPageDesc")}
      />

      <SectionCard title={t("settings.language")} description={t("settings.languageDesc")}>
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
      </SectionCard>

      <SectionCard title={t("settings.speedLimit")} description={t("settings.speedLimitDesc")}>
        <div className="space-y-5">
          <SettingsInnerBox>
            <div className="text-[10.5px] font-semibold uppercase tracking-[0.13em] text-muted-foreground">
              {t("metrics.downloadSpeed")}
            </div>
            <div className="mt-2 font-space-grotesk text-[26px] font-bold leading-none tracking-tight text-foreground">
              {speedValue === 0 ? t("settings.unlimited") : formatSpeed(speedValue)}
            </div>
          </SettingsInnerBox>

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
            <Button onClick={() => void applySpeedLimit()} disabled={updateState.fetching}>
              {t("settings.applySpeedNow")}
            </Button>
            <Button variant="outline" onClick={() => setSpeedValue(0)}>
              {t("settings.resetSpeedLimit")}
            </Button>
            {speedSaved ? (
              <span className="text-sm text-status-completed">
                {t("settings.saved")}
              </span>
            ) : null}
          </div>
        </div>
      </SectionCard>

      {settings ? (
        <SectionCard
          title={t("settings.storageAndBehavior")}
          description={t("settings.storageAndBehaviorDesc")}
        >
          <div className="space-y-5">
            <div className="grid gap-4 xl:grid-cols-2">
              <SettingField
                label={t("settings.dataDir")}
                description={t("settings.dataDirDesc")}
                staticValue={settings.dataDir}
              />
              <SettingField label={t("settings.intermediateDir")} description={t("settings.intermediateDirDesc")}>
                <FolderPathInput
                  value={intermediateDir}
                  onChange={setIntermediateDir}
                  placeholder={`${settings.dataDir}/intermediate`}
                />
              </SettingField>
              <SettingField label={t("settings.completeDir")} description={t("settings.completeDirDesc")}>
                <FolderPathInput
                  value={completeDir}
                  onChange={setCompleteDir}
                  placeholder={`${settings.dataDir}/complete`}
                />
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
              <SettingField
                label={t("settings.ipReplacementTrialExtraConnections")}
                description={t("settings.ipReplacementTrialExtraConnectionsDesc")}
              >
                <div className="flex flex-wrap items-center gap-3">
                  <Input
                    type="number"
                    min={0}
                    max={1}
                    step={1}
                    value={ipReplacementBurst}
                    onChange={(event) => {
                      const value = Number(event.target.value);
                      setIpReplacementBurst(Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : 0);
                    }}
                    className="max-w-24"
                  />
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => void applyIpReplacementBurst()}
                    disabled={updateState.fetching}
                  >
                    {t("settings.save")}
                  </Button>
                  {ipReplacementBurstSaved ? (
                    <span className="text-sm text-status-completed">
                      {t("settings.saved")}
                    </span>
                  ) : null}
                </div>
              </SettingField>
            </div>

            <div className="flex items-center justify-between gap-4 rounded-inner border border-border p-5">
              <div>
                <div className="text-sm font-semibold text-foreground">
                  {t("settings.cleanupAfterExtract")}
                </div>
                <div className="mt-1 text-[12.5px] text-muted-foreground">
                  {t("settings.cleanupDesc")}
                </div>
              </div>
              <Switch checked={cleanup} onCheckedChange={setCleanup} />
            </div>

            <div className="flex flex-wrap items-center gap-3">
              {storageSaveStatus === "saving" ? (
                <span className="text-sm text-muted-foreground">{t("settings.saving")}</span>
              ) : null}
              {storageSaveStatus === "saved" ? (
                <span className="text-sm text-status-completed">
                  {t("settings.saved")}
                </span>
              ) : null}
              {storageSaveStatus === "error" && storageSaveError ? (
                <span className="text-sm text-destructive">{storageSaveError}</span>
              ) : null}
            </div>

          </div>
        </SectionCard>
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
    <div className="rounded-inner border border-border p-5">
      <Label className="mb-2 text-sm font-semibold">{label}</Label>
      {staticValue ? (
        <div className="rounded-inner border border-input bg-field/50 px-3 py-2 font-mono text-sm text-muted-foreground">
          {staticValue}
        </div>
      ) : (
        children
      )}
      <p className="mt-2 text-[12.5px] text-muted-foreground">{description}</p>
    </div>
  );
}
