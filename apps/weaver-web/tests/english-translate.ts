import { interpolate } from "../src/lib/i18n/types.ts";
import { nextEn } from "../src/next/i18n/en.ts";

/** The English Next dictionary as a translate function, for helpers that take one. */
export const englishTranslate = (key: string, values?: Record<string, string | number | boolean | null | undefined>) =>
  interpolate(nextEn[key] ?? key, values);
