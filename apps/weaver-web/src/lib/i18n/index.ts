import type { LocaleDictionary } from "./types";
import { DEFAULT_LANGUAGE, interpolate } from "./types";

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

type LocaleLoader = () => Promise<{ default: LocaleDictionary }>;

const SUPPORTED_LOCALES: LocaleCode[] = [
  "eng",
  "spa",
  "zho",
  "fra",
  "por",
  "jpn",
  "deu",
  "ita",
  "kor",
];

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

const localeLoaders: Record<LocaleCode, LocaleLoader> = {
  eng: () => import("./locales/en"),
  spa: () => import("./locales/es"),
  zho: () => import("./locales/zh"),
  fra: () => import("./locales/fr"),
  por: () => import("./locales/pt"),
  jpn: () => import("./locales/ja"),
  deu: () => import("./locales/de"),
  ita: () => import("./locales/it"),
  kor: () => import("./locales/ko"),
};

const localeDictionaryCache = new Map<LocaleCode, Promise<LocaleDictionary>>();

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

function isLocaleCode(code: string): code is LocaleCode {
  return SUPPORTED_LOCALES.includes(code as LocaleCode);
}

function cachedLocaleDictionary(code: LocaleCode): Promise<LocaleDictionary> {
  const existing = localeDictionaryCache.get(code);
  if (existing) {
    return existing;
  }

  const next = localeLoaders[code]().then((module) => module.default);
  localeDictionaryCache.set(code, next);
  return next;
}

export function getLanguageLabel(code: string): string {
  const normalized = normalizeLocale(code);
  return AVAILABLE_LANGUAGES.find((language) => language.code === normalized)?.label ?? normalized;
}

export async function loadLocaleDictionary(code: string | null | undefined): Promise<LocaleDictionary> {
  const key = normalizeLocale(code);
  if (key === DEFAULT_LANGUAGE) {
    try {
      return await cachedLocaleDictionary(key);
    } catch {
      return {};
    }
  }

  try {
    const [fallback, selected] = await Promise.all([
      cachedLocaleDictionary(DEFAULT_LANGUAGE),
      cachedLocaleDictionary(key),
    ]);
    return { ...fallback, ...selected };
  } catch {
    try {
      return await cachedLocaleDictionary(DEFAULT_LANGUAGE);
    } catch {
      return {};
    }
  }
}

export function normalizeLocale(code?: string | null): LocaleCode {
  const normalized = code?.toLowerCase().trim();
  if (!normalized) {
    return DEFAULT_LANGUAGE;
  }

  const root = normalized.split("-")[0]!;
  if (isLocaleCode(root)) {
    return root;
  }

  const alias = LOCALE_ALIASES[root];
  if (alias) {
    return alias;
  }

  return DEFAULT_LANGUAGE;
}

export function translateDictionary(
  dictionary: LocaleDictionary | null | undefined,
  key: string,
  values?: Record<string, string | number | boolean | null | undefined>,
): string {
  const template = dictionary?.[key] ?? key;
  return interpolate(template, values);
}
