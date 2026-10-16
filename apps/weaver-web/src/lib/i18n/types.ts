// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

export type LocaleDictionary = Record<string, string>;

export function interpolate(
  template: string,
  values?: Record<string, string | number | boolean | null | undefined>,
): string {
  if (!values) {
    return template;
  }

  return Object.entries(values).reduce((result, [key, value]) => {
    return result.replace(new RegExp(`\\{\\{${key}\\}}`, "g"), String(value ?? ""));
  }, template);
}

export const DEFAULT_LANGUAGE = "eng";
