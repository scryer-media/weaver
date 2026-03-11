import { ApiKeysSection, SettingsPageHeader } from "@/pages/settings/shared";
import { useTranslate } from "@/lib/context/translate-context";

export function SecuritySettingsPage() {
  const t = useTranslate();

  return (
    <div>
      <SettingsPageHeader
        title={t("settings.security")}
        description={t("settings.securityDesc")}
      />
      <ApiKeysSection />
    </div>
  );
}
