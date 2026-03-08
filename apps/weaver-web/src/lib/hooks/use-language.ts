import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AVAILABLE_LANGUAGES,
  DEFAULT_LANGUAGE,
  type LocaleCode,
  getLanguageLabel,
  normalizeLocale,
  t as translate,
} from "@/lib/i18n";

const UI_LANGUAGE_STORAGE_KEY = "weaver.ui.language";

function readStoredLanguageCode(): LocaleCode {
  const stored = window.sessionStorage.getItem(UI_LANGUAGE_STORAGE_KEY);
  if (!stored) {
    const browserLanguage = navigator.language.split("-")[0] ?? DEFAULT_LANGUAGE;
    return normalizeLocale(browserLanguage);
  }
  return normalizeLocale(stored);
}

function writeStoredLanguageCode(code: string) {
  const normalized = normalizeLocale(code);
  window.sessionStorage.setItem(UI_LANGUAGE_STORAGE_KEY, normalized);
}

export function useLanguage() {
  const [uiLanguage, setUiLanguage] = useState<LocaleCode>(readStoredLanguageCode);

  const t = useCallback(
    (key: string, values?: Record<string, string | number | boolean | null | undefined>) =>
      translate(key, uiLanguage, values),
    [uiLanguage],
  );

  const selectedLanguage = useMemo(
    () => AVAILABLE_LANGUAGES.find((language) => language.code === uiLanguage) ?? AVAILABLE_LANGUAGES[0],
    [uiLanguage],
  );

  const setLanguagePreference = useCallback((code: string) => {
    const normalized = normalizeLocale(code);
    setUiLanguage(normalized);
    writeStoredLanguageCode(normalized);
  }, []);

  useEffect(() => {
    writeStoredLanguageCode(uiLanguage);
    document.documentElement.lang = uiLanguage;
  }, [uiLanguage]);

  return {
    uiLanguage,
    setLanguagePreference,
    selectedLanguage,
    t,
    getLanguageLabel,
  };
}
