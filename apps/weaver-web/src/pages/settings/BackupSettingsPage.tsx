import { useQuery } from "urql";
import { SETTINGS_QUERY } from "@/graphql/queries";
import {
  BackupRestoreSection,
  SettingsPageHeader,
} from "@/pages/settings/shared";
import { useTranslate } from "@/lib/context/translate-context";

export function BackupSettingsPage() {
  const t = useTranslate();
  const [{ data }] = useQuery({ query: SETTINGS_QUERY });

  return (
    <div>
      <SettingsPageHeader
        title={t("settings.backupNav")}
        description={t("settings.backupPageDesc")}
      />
      <BackupRestoreSection currentDataDir={data?.settings?.dataDir ?? ""} />
    </div>
  );
}
