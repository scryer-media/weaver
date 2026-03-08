import { createContext, useContext } from "react";

export type Translate = (key: string, values?: Record<string, string | number | boolean | null | undefined>) => string;

export const TranslateContext = createContext<Translate | null>(null);

export function useTranslate(): Translate {
  const t = useContext(TranslateContext);
  if (!t) throw new Error("useTranslate must be used within TranslateContext.Provider");
  return t;
}
