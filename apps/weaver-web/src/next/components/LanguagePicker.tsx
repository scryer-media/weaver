import { useLanguageSettings } from "@/lib/context/translate-context";
import { AVAILABLE_LANGUAGES, type LocaleCode } from "@/lib/i18n";
import { cn } from "@/lib/utils";
import { Select } from "./controls";

const LANGUAGE_OPTIONS = AVAILABLE_LANGUAGES.map((language) => ({
  value: language.code,
  label: language.label,
}));

/**
 * The interface language, for pages outside Settings. Each language names
 * itself, so someone who cannot read the current one still finds their own.
 */
export function LanguagePicker({ className }: { className?: string }) {
  const { t, uiLanguage, setLanguagePreference } = useLanguageSettings();
  return (
    <Select<LocaleCode>
      value={uiLanguage}
      options={LANGUAGE_OPTIONS}
      onChange={setLanguagePreference}
      label={t("next.general.language")}
      icon="language"
      className={cn("h-[30px] min-w-[132px] gap-2 px-2.5 text-[12.5px]", className)}
    />
  );
}
