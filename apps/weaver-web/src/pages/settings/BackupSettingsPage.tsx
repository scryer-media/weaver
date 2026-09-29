import { useState } from "react";
import { useQuery } from "urql";
import { SETTINGS_QUERY } from "@/graphql/queries";
import {
  BackupRestoreSection,
  SettingsPageHeader,
} from "@/pages/settings/shared";
import { useTranslate } from "@/lib/context/translate-context";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import { StoredBackups } from "@/next/pages/settings/panels/StoredBackups";

const backupShell = { search: "", actionsRef: { current: null }, setFlags: () => {}, controlsHost: null };

export function BackupSettingsPage() {
  const t = useTranslate();
  const [{ data }] = useQuery({ query: SETTINGS_QUERY });
  const [generation, setGeneration] = useState(0);

  return (
    <div className="max-w-[1180px]">
      <SettingsPageHeader
        title={t("settings.backupNav")}
        description={t("settings.backupPageDesc")}
      />
      <BackupRestoreSection currentDataDir={data?.settings?.dataDir ?? ""} onBackupCreated={() => setGeneration((value) => value + 1)} />
      <SettingsShellProvider {...backupShell}>
        <StoredBackups generation={generation} />
      </SettingsShellProvider>
    </div>
  );
}
