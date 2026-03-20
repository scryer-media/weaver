import type { LocaleDictionary } from "./types";
import { DEFAULT_LANGUAGE, interpolate } from "./types";
import en from "./locales/en";
import es from "./locales/es";
import zh from "./locales/zh";
import fr from "./locales/fr";
import pt from "./locales/pt";
import ja from "./locales/ja";
import de from "./locales/de";
import it from "./locales/it";
import ko from "./locales/ko";
export { DEFAULT_LANGUAGE } from "./types";

export type LocaleCode =
  | "eng"
  | "spa"
  | "zho"
  | "fra"
  | "por"
  | "jpn"
  | "deu"
  | "ita"
  | "kor";

export type LanguageOption = {
  code: LocaleCode;
  label: string;
};

type LocaleMap = Record<LocaleCode, LocaleDictionary>;

const LOCALE_ALIASES: Record<string, LocaleCode> = {
  en: "eng",
  es: "spa",
  zh: "zho",
  fr: "fra",
  pt: "por",
  de: "deu",
  ja: "jpn",
  it: "ita",
  ko: "kor",
};

const locales: LocaleMap = {
  eng: en,
  spa: es,
  zho: zh,
  fra: fr,
  por: pt,
  jpn: ja,
  deu: de,
  ita: it,
  kor: ko,
};

export const AVAILABLE_LANGUAGES: LanguageOption[] = [
  { code: "eng", label: "English" },
  { code: "deu", label: "Deutsch" },
  { code: "spa", label: "Español" },
  { code: "fra", label: "Français" },
  { code: "ita", label: "Italiano" },
  { code: "jpn", label: "日本語" },
  { code: "kor", label: "한국어" },
  { code: "por", label: "Português" },
  { code: "zho", label: "中文" },
];

export function getLanguageLabel(code: string): string {
  const normalized = normalizeLocale(code);
  return AVAILABLE_LANGUAGES.find((language) => language.code === normalized)?.label ?? normalized;
}

const FALLBACK: LocaleDictionary = en;

export function getLocaleDictionary(code: string | null | undefined): LocaleDictionary {
  if (!code) {
    return FALLBACK;
  }
  const key = normalizeLocale(code);
  return locales[key] ?? FALLBACK;
}

export function normalizeLocale(code?: string | null): LocaleCode {
  const normalized = code?.toLowerCase().trim();
  if (!normalized) {
    return DEFAULT_LANGUAGE;
  }
  const root = normalized.split("-")[0]!;
  if (root in locales) {
    return root as LocaleCode;
  }
  const alias = LOCALE_ALIASES[root];
  if (alias) {
    return alias;
  }
  return DEFAULT_LANGUAGE;
}

export function t(key: string, code: string, values?: Record<string, string | number | boolean | null | undefined>): string {
  const locale = getLocaleDictionary(code);
  const template = locale[key] ?? FALLBACK[key] ?? key;
  return interpolate(template, values);
}
