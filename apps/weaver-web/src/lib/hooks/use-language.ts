import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AVAILABLE_LANGUAGES,
  DEFAULT_LANGUAGE,
  type LocaleCode,
  getLanguageLabel,
  loadLocaleDictionary,
  normalizeLocale,
  translateDictionary,
} from "@/lib/i18n";
import type { LocaleDictionary } from "@/lib/i18n/types";

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
  const [dictionary, setDictionary] = useState<LocaleDictionary | null>(null);
  const [isReady, setIsReady] = useState(false);
  const initialLanguageRef = useRef(uiLanguage);
  const loadRequestIdRef = useRef(0);

  const loadLanguage = useCallback(async (code: string) => {
    const normalized = normalizeLocale(code);
    const requestId = loadRequestIdRef.current + 1;
    loadRequestIdRef.current = requestId;

    const nextDictionary = await loadLocaleDictionary(normalized);
    if (loadRequestIdRef.current !== requestId) {
      return;
    }

    setDictionary(nextDictionary);
    setUiLanguage(normalized);
    writeStoredLanguageCode(normalized);
    document.documentElement.lang = normalized;
    setIsReady(true);
  }, []);

  useEffect(() => {
    void loadLanguage(initialLanguageRef.current);
  }, [loadLanguage]);

  const t = useCallback(
    (key: string, values?: Record<string, string | number | boolean | null | undefined>) =>
      translateDictionary(dictionary, key, values),
    [dictionary],
  );

  const selectedLanguage = useMemo(
    () => AVAILABLE_LANGUAGES.find((language) => language.code === uiLanguage) ?? AVAILABLE_LANGUAGES[0],
    [uiLanguage],
  );

  const setLanguagePreference = useCallback((code: string) => {
    void loadLanguage(code);
  }, [loadLanguage]);

  return {
    isReady,
    uiLanguage,
    setLanguagePreference,
    selectedLanguage,
    t,
    getLanguageLabel,
  };
}
