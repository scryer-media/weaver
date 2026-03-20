import { createContext, useContext } from "react";
import type { LocaleCode, LanguageOption } from "@/lib/i18n";

export type Translate = (key: string, values?: Record<string, string | number | boolean | null | undefined>) => string;

export type TranslateContextValue = {
  t: Translate;
  uiLanguage: LocaleCode;
  setLanguagePreference: (code: string) => void;
  selectedLanguage: LanguageOption;
};

export const TranslateContext = createContext<TranslateContextValue | null>(null);

export function useTranslate(): Translate {
  const ctx = useContext(TranslateContext);
  if (!ctx) throw new Error("useTranslate must be used within TranslateContext.Provider");
  return ctx.t;
}

export function useLanguageSettings() {
  const ctx = useContext(TranslateContext);
  if (!ctx) throw new Error("useLanguageSettings must be used within TranslateContext.Provider");
  return ctx;
}
