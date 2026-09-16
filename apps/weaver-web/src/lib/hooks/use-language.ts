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

// A language someone picks is kept for this browser, so it survives closing the
// tab. Choices made before that lived only in the tab's session are still read.
function readStorage(storage: () => Storage): string | null {
  try {
    return storage().getItem(UI_LANGUAGE_STORAGE_KEY);
  } catch {
    return null;
  }
}

function readStoredLanguageCode(): LocaleCode {
  const stored =
    readStorage(() => window.localStorage) ?? readStorage(() => window.sessionStorage);
  if (!stored) {
    const browserLanguage = navigator.language.split("-")[0] ?? DEFAULT_LANGUAGE;
    return normalizeLocale(browserLanguage);
  }
  return normalizeLocale(stored);
}

function writeStoredLanguageCode(code: LocaleCode) {
  try {
    window.localStorage.setItem(UI_LANGUAGE_STORAGE_KEY, code);
  } catch {
    // Storage can be blocked; the choice then lasts until the page reloads.
  }
}

export function useLanguage() {
  const [uiLanguage, setUiLanguage] = useState<LocaleCode>(readStoredLanguageCode);
  const [dictionary, setDictionary] = useState<LocaleDictionary | null>(null);
  const [isReady, setIsReady] = useState(false);
  const initialLanguageRef = useRef(uiLanguage);
  const loadRequestIdRef = useRef(0);

  const loadLanguage = useCallback(async (code: string, remember: boolean) => {
    const normalized = normalizeLocale(code);
    const requestId = loadRequestIdRef.current + 1;
    loadRequestIdRef.current = requestId;

    const nextDictionary = await loadLocaleDictionary(normalized);
    if (loadRequestIdRef.current !== requestId) {
      return;
    }

    setDictionary(nextDictionary);
    setUiLanguage(normalized);
    // The browser's own language is only a default, never pinned as a choice.
    if (remember) {
      writeStoredLanguageCode(normalized);
    }
    document.documentElement.lang = normalized;
    setIsReady(true);
  }, []);

  useEffect(() => {
    void loadLanguage(initialLanguageRef.current, false);
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
    void loadLanguage(code, true);
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
